//! WorkOS AuthKit integration.
//!
//! This module keeps the authentication UI in our application while using
//! WorkOS's headless User Management API for password authentication, email
//! verification, and password resets. The WorkOS API key and session tokens
//! remain server-side.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Query, State};
use axum::http::header::{COOKIE, HeaderValue, SET_COOKIE};
use axum::http::{HeaderMap, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::StreamExt;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use crate::http::{AuthError, AuthenticatedAccount, SessionVerifier};

const STATE_TTL: Duration = Duration::from_secs(600);
const JWKS_TTL: Duration = Duration::from_secs(300);
const SESSION_MAX_AGE: u64 = 3_600;
const REFRESH_MAX_AGE: u64 = 2_592_000;
const PENDING_AUTH_MAX_AGE: u64 = 600;
const PENDING_AUTH_COOKIE: &str = "task_space_pending_auth";
const OAUTH_STATE_COOKIE: &str = "task_space_oauth_state";
const MAX_PENDING_STATES: usize = 4_096;
const MAX_WORKOS_RESPONSE_BYTES: usize = 512 * 1024;
const MAX_IDENTITY_CLAIM_LEN: usize = 256;

#[derive(Clone, Debug)]
pub struct WorkOsAuthConfig {
    pub api_key: String,
    pub client_id: String,
    pub redirect_uri: String,
    pub post_login_redirect_uri: String,
    pub issuer: String,
    pub token_issuer: Option<String>,
    pub audience: Option<String>,
    pub cookie_name: String,
    pub allowed_origins: Vec<String>,
}

impl WorkOsAuthConfig {
    pub fn from_env() -> Result<Self, WorkOsError> {
        Ok(Self {
            api_key: required_env("WORKOS_API_KEY")?,
            client_id: required_env("WORKOS_CLIENT_ID")?,
            redirect_uri: required_env("WORKOS_REDIRECT_URI")?,
            post_login_redirect_uri: std::env::var("WORKOS_POST_LOGIN_REDIRECT_URI")
                .unwrap_or_else(|_| "/app".to_owned()),
            issuer: std::env::var("WORKOS_ISSUER")
                .unwrap_or_else(|_| "https://api.workos.com".to_owned()),
            token_issuer: std::env::var("WORKOS_TOKEN_ISSUER")
                .ok()
                .filter(|issuer| !issuer.trim().is_empty()),
            audience: std::env::var("WORKOS_AUDIENCE")
                .ok()
                .filter(|audience| !audience.trim().is_empty()),
            cookie_name: std::env::var("WORKOS_SESSION_COOKIE")
                .unwrap_or_else(|_| "task_space_session".to_owned()),
            allowed_origins: std::env::var("TASK_SPACE_ALLOWED_ORIGINS")
                .ok()
                .map(|value| value.split(',').filter_map(normalize_origin).collect())
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WorkOsError {
    #[error("missing required environment variable {0}")]
    MissingEnvironment(&'static str),
    #[error("invalid WorkOS configuration: {0}")]
    InvalidConfiguration(String),
    #[error("WorkOS request failed: {0}")]
    Request(String),
    #[error("WorkOS returned an invalid response: {0}")]
    InvalidResponse(String),
    #[error("authorization state is missing or expired")]
    InvalidState,
    #[error("authorization callback did not contain a code")]
    MissingCode,
    #[error("WorkOS rejected the authentication request ({status}): {message}")]
    Api {
        status: u16,
        code: String,
        message: String,
        detail: Option<String>,
        pending_authentication_token: Option<String>,
    },
}

#[derive(Clone)]
pub struct WorkOsAuth {
    inner: Arc<WorkOsAuthInner>,
}

struct WorkOsAuthInner {
    config: WorkOsAuthConfig,
    client: Client,
    pending_states: Mutex<HashMap<String, Instant>>,
    jwks: tokio::sync::RwLock<Option<CachedJwks>>,
}

#[derive(Clone)]
struct CachedJwks {
    fetched_at: Instant,
    keys: Vec<WorkOsJwk>,
}

#[derive(Clone, Debug, Deserialize)]
struct WorkOsJwkSet {
    keys: Vec<WorkOsJwk>,
}

#[derive(Clone, Debug, Deserialize)]
struct WorkOsJwk {
    kid: Option<String>,
    kty: String,
    n: Option<String>,
    e: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct WorkOsClaims {
    sub: String,
    sid: String,
    client_id: String,
    iss: String,
    #[serde(rename = "exp")]
    _exp: usize,
    #[serde(rename = "nbf", default)]
    _nbf: Option<usize>,
    #[serde(default)]
    org_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct WorkOsAuthenticateResponse {
    user: WorkOsUser,
    access_token: String,
    refresh_token: String,
    #[serde(default)]
    organization_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct WorkOsApiErrorBody {
    code: Option<String>,
    message: Option<String>,
    errors: Option<Vec<WorkOsApiValidationError>>,
    pending_authentication_token: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct WorkOsApiValidationError {
    code: Option<String>,
    message: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct AuthResponse {
    status: &'static str,
    message: Option<String>,
    email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pending_authentication_token: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PasswordCredentials {
    pub email: String,
    pub password: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PasswordResetRequest {
    pub email: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PasswordResetConfirmation {
    pub token: String,
    pub new_password: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct EmailVerificationRequest {
    pub code: String,
    #[serde(default)]
    pub pending_authentication_token: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct WorkOsUser {
    id: String,
}

#[derive(Clone, Debug)]
pub struct WorkOsSession {
    pub account: AuthenticatedAccount,
    pub access_token: String,
    pub refresh_token: String,
}

impl WorkOsAuth {
    pub fn new(config: WorkOsAuthConfig) -> Result<Self, WorkOsError> {
        if config.api_key.trim().is_empty()
            || config.client_id.trim().is_empty()
            || config.redirect_uri.trim().is_empty()
            || config.cookie_name.trim().is_empty()
        {
            return Err(WorkOsError::InvalidConfiguration(
                "credentials, redirect URI, and cookie name are required".to_owned(),
            ));
        }
        if config.cookie_name.len() > 64
            || !config.cookie_name.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '_' | '-')
            })
        {
            return Err(WorkOsError::InvalidConfiguration(
                "session cookie name contains unsupported characters".to_owned(),
            ));
        }
        let redirect_uri = Url::parse(&config.redirect_uri)
            .map_err(|error| WorkOsError::InvalidConfiguration(error.to_string()))?;
        if !matches!(redirect_uri.scheme(), "http" | "https")
            || redirect_uri.host_str().is_none()
            || !redirect_uri.username().is_empty()
            || redirect_uri.password().is_some()
        {
            return Err(WorkOsError::InvalidConfiguration(
                "redirect URI must be an origin-owned HTTP(S) URL".to_owned(),
            ));
        }
        if redirect_uri.scheme() == "http"
            && !is_local_development_host(redirect_uri.host_str().unwrap_or_default())
        {
            return Err(WorkOsError::InvalidConfiguration(
                "non-local redirect URI must use HTTPS".to_owned(),
            ));
        }
        if config.post_login_redirect_uri.starts_with("http://")
            || config.post_login_redirect_uri.starts_with("https://")
        {
            let post_login = Url::parse(&config.post_login_redirect_uri)
                .map_err(|error| WorkOsError::InvalidConfiguration(error.to_string()))?;
            if post_login.host_str().is_none()
                || !post_login.username().is_empty()
                || post_login.password().is_some()
            {
                return Err(WorkOsError::InvalidConfiguration(
                    "post-login redirect URL must not contain credentials".to_owned(),
                ));
            }
            if post_login.scheme() == "http"
                && !is_local_development_host(post_login.host_str().unwrap_or_default())
            {
                return Err(WorkOsError::InvalidConfiguration(
                    "non-local post-login redirect must use HTTPS".to_owned(),
                ));
            }
        } else if !config.post_login_redirect_uri.starts_with('/') {
            return Err(WorkOsError::InvalidConfiguration(
                "post-login redirect must be an absolute URL or path".to_owned(),
            ));
        }
        validate_service_url(&config.issuer, "issuer")?;
        if let Some(token_issuer) = config.token_issuer.as_deref() {
            validate_service_url(token_issuer, "token issuer")?;
        }

        Ok(Self {
            inner: Arc::new(WorkOsAuthInner {
                config,
                client: Client::builder()
                    .timeout(Duration::from_secs(15))
                    .build()
                    .map_err(|error| WorkOsError::InvalidConfiguration(error.to_string()))?,
                pending_states: Mutex::new(HashMap::new()),
                jwks: tokio::sync::RwLock::new(None),
            }),
        })
    }

    pub fn cookie_name(&self) -> &str {
        &self.inner.config.cookie_name
    }

    pub fn refresh_cookie_name(&self) -> String {
        format!("{}_refresh", self.inner.config.cookie_name)
    }

    /// Verify the configured WorkOS JWKS endpoint before accepting traffic.
    /// Authentication requests can still refresh the cache later for key
    /// rotation, but startup should fail rather than serving an instance that
    /// cannot verify any session.
    pub async fn check_readiness(&self) -> Result<(), WorkOsError> {
        let keys = self
            .fetch_jwks(true)
            .await
            .map_err(|_| WorkOsError::Request("WorkOS JWKS is unavailable".to_owned()))?;
        if keys
            .iter()
            .any(|key| key.kty == "RSA" && key.n.is_some() && key.e.is_some())
        {
            Ok(())
        } else {
            Err(WorkOsError::InvalidResponse(
                "WorkOS JWKS did not contain a usable RSA key".to_owned(),
            ))
        }
    }

    pub fn authorization_url(&self, screen_hint: &str) -> Result<String, WorkOsError> {
        let state = self.create_state()?;
        let mut url = Url::parse(&format!(
            "{}/user_management/authorize",
            self.inner.config.issuer.trim_end_matches('/')
        ))
        .map_err(|error| WorkOsError::InvalidConfiguration(error.to_string()))?;
        url.query_pairs_mut()
            .append_pair("response_type", "code")
            .append_pair("client_id", &self.inner.config.client_id)
            .append_pair("redirect_uri", &self.inner.config.redirect_uri)
            .append_pair("provider", "authkit")
            .append_pair("screen_hint", screen_hint)
            .append_pair("state", &state);
        Ok(url.into())
    }

    pub fn consume_state(&self, state: &str) -> Result<(), WorkOsError> {
        let mut states = self
            .inner
            .pending_states
            .lock()
            .map_err(|_| WorkOsError::InvalidState)?;
        let now = Instant::now();
        states.retain(|_, created| now.duration_since(*created) <= STATE_TTL);
        match states.remove(state) {
            Some(created) if now.duration_since(created) <= STATE_TTL => Ok(()),
            _ => Err(WorkOsError::InvalidState),
        }
    }

    pub async fn exchange_code(&self, code: &str) -> Result<WorkOsSession, WorkOsError> {
        if code.trim().is_empty() {
            return Err(WorkOsError::MissingCode);
        }
        self.exchange_session(serde_json::json!({
            "grant_type": "authorization_code",
            "code": code,
        }))
        .await
    }

    pub async fn refresh_session(&self, refresh_token: &str) -> Result<WorkOsSession, WorkOsError> {
        if refresh_token.trim().is_empty() {
            return Err(WorkOsError::MissingCode);
        }
        self.exchange_session(serde_json::json!({
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }))
        .await
    }

    pub async fn authenticate_password(
        &self,
        credentials: &PasswordCredentials,
    ) -> Result<WorkOsSession, WorkOsError> {
        self.authenticate(serde_json::json!({
            "grant_type": "password",
            "email": credentials.email.trim(),
            "password": credentials.password,
        }))
        .await
    }

    pub async fn authenticate_email_verification(
        &self,
        request: &EmailVerificationRequest,
        pending_authentication_token: &str,
    ) -> Result<WorkOsSession, WorkOsError> {
        self.authenticate(serde_json::json!({
            "grant_type": "urn:workos:oauth:grant-type:email-verification:code",
            "code": request.code.trim(),
            "pending_authentication_token": pending_authentication_token,
        }))
        .await
    }

    pub async fn create_user(&self, credentials: &PasswordCredentials) -> Result<(), WorkOsError> {
        if credentials.email.trim().is_empty() || credentials.password.trim().is_empty() {
            return Err(WorkOsError::InvalidConfiguration(
                "email and password are required".to_owned(),
            ));
        }
        let endpoint = format!(
            "{}/user_management/users",
            self.inner.config.issuer.trim_end_matches('/')
        );
        let response = self
            .inner
            .client
            .post(endpoint)
            .bearer_auth(&self.inner.config.api_key)
            .json(&serde_json::json!({
                "email": credentials.email.trim(),
                "password": credentials.password,
                "email_verified": false,
            }))
            .send()
            .await
            .map_err(|error| WorkOsError::Request(error.to_string()))?;
        if !response.status().is_success() {
            return Err(self.provider_error(response).await);
        }
        Ok(())
    }

    pub async fn create_password_reset(&self, email: &str) -> Result<(), WorkOsError> {
        let endpoint = format!(
            "{}/user_management/password_reset",
            self.inner.config.issuer.trim_end_matches('/')
        );
        let response = self
            .inner
            .client
            .post(endpoint)
            .bearer_auth(&self.inner.config.api_key)
            .json(&serde_json::json!({ "email": email.trim() }))
            .send()
            .await
            .map_err(|error| WorkOsError::Request(error.to_string()))?;
        if !response.status().is_success() {
            return Err(self.provider_error(response).await);
        }
        Ok(())
    }

    pub async fn reset_password(
        &self,
        request: &PasswordResetConfirmation,
    ) -> Result<(), WorkOsError> {
        let endpoint = format!(
            "{}/user_management/password_reset/confirm",
            self.inner.config.issuer.trim_end_matches('/')
        );
        let response = self
            .inner
            .client
            .post(endpoint)
            .bearer_auth(&self.inner.config.api_key)
            .json(&serde_json::json!({
                "token": request.token.trim(),
                "new_password": request.new_password,
            }))
            .send()
            .await
            .map_err(|error| WorkOsError::Request(error.to_string()))?;
        if !response.status().is_success() {
            return Err(self.provider_error(response).await);
        }
        Ok(())
    }

    async fn exchange_session(
        &self,
        mut body: serde_json::Value,
    ) -> Result<WorkOsSession, WorkOsError> {
        let endpoint = format!(
            "{}/user_management/authenticate",
            self.inner.config.issuer.trim_end_matches('/')
        );
        body["client_id"] = serde_json::Value::String(self.inner.config.client_id.clone());
        body["client_secret"] = serde_json::Value::String(self.inner.config.api_key.clone());
        let response = self
            .inner
            .client
            .post(endpoint)
            .json(&body)
            .send()
            .await
            .map_err(|error| WorkOsError::Request(error.to_string()))?;
        if !response.status().is_success() {
            return Err(self.provider_error(response).await);
        }
        let response = bounded_json_response::<WorkOsAuthenticateResponse>(response)
            .await
            .map_err(WorkOsError::InvalidResponse)?;
        self.session_from_authenticate_response(response)
    }

    async fn authenticate(
        &self,
        mut body: serde_json::Value,
    ) -> Result<WorkOsSession, WorkOsError> {
        let endpoint = format!(
            "{}/user_management/authenticate",
            self.inner.config.issuer.trim_end_matches('/')
        );
        body["client_id"] = serde_json::Value::String(self.inner.config.client_id.clone());
        body["client_secret"] = serde_json::Value::String(self.inner.config.api_key.clone());
        let response = self
            .inner
            .client
            .post(endpoint)
            .json(&body)
            .send()
            .await
            .map_err(|error| WorkOsError::Request(error.to_string()))?;
        if !response.status().is_success() {
            return Err(self.provider_error(response).await);
        }
        let response = bounded_json_response::<WorkOsAuthenticateResponse>(response)
            .await
            .map_err(WorkOsError::InvalidResponse)?;
        self.session_from_authenticate_response(response)
    }

    fn session_from_authenticate_response(
        &self,
        response: WorkOsAuthenticateResponse,
    ) -> Result<WorkOsSession, WorkOsError> {
        if response.access_token.trim().is_empty()
            || response.refresh_token.trim().is_empty()
            || response.access_token.len() > 16 * 1024
            || response.refresh_token.len() > 16 * 1024
        {
            return Err(WorkOsError::InvalidResponse(
                "WorkOS returned an invalid session token".to_owned(),
            ));
        }
        let user_id = response.user.id.trim().to_owned();
        let account_id = response
            .organization_id
            .as_deref()
            .unwrap_or(&user_id)
            .trim()
            .to_owned();
        if user_id.is_empty()
            || account_id.is_empty()
            || user_id.len() > MAX_IDENTITY_CLAIM_LEN
            || account_id.len() > MAX_IDENTITY_CLAIM_LEN
        {
            return Err(WorkOsError::InvalidResponse(
                "WorkOS returned invalid identity claims".to_owned(),
            ));
        }
        Ok(WorkOsSession {
            account: AuthenticatedAccount {
                user_id,
                account_id,
                session_id: String::new(),
            },
            access_token: response.access_token,
            refresh_token: response.refresh_token,
        })
    }

    async fn provider_error(&self, response: reqwest::Response) -> WorkOsError {
        let status = response.status().as_u16();
        let body = bounded_response_bytes(response)
            .await
            .ok()
            .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
            .unwrap_or_default();
        let details = body.get("error").unwrap_or(&body);
        let details = serde_json::from_value::<WorkOsApiErrorBody>(details.clone()).ok();
        WorkOsError::Api {
            status,
            code: details
                .as_ref()
                .and_then(|error| error.code.clone())
                .unwrap_or_else(|| "workos_error".to_owned()),
            message: details
                .as_ref()
                .and_then(|error| error.message.clone())
                .unwrap_or_else(|| "WorkOS rejected the request".to_owned()),
            detail: details.as_ref().and_then(|error| {
                error.errors.as_ref()?.first().map(|validation| {
                    match (&validation.code, &validation.message) {
                        (Some(code), Some(message)) => format!("{code}: {message}"),
                        (Some(code), None) => code.clone(),
                        (None, Some(message)) => message.clone(),
                        (None, None) => "WorkOS validation failed".to_owned(),
                    }
                })
            }),
            pending_authentication_token: details
                .and_then(|error| error.pending_authentication_token),
        }
    }

    async fn fetch_jwks(&self, force_refresh: bool) -> Result<Vec<WorkOsJwk>, AuthError> {
        if !force_refresh
            && let Some(cached) = self.inner.jwks.read().await.as_ref()
            && cached.fetched_at.elapsed() <= JWKS_TTL
        {
            return Ok(cached.keys.clone());
        }
        let endpoint = format!(
            "{}/sso/jwks/{}",
            self.inner.config.issuer.trim_end_matches('/'),
            self.inner.config.client_id
        );
        let response = self
            .inner
            .client
            .get(endpoint)
            .send()
            .await
            .map_err(|_| AuthError::VerificationFailed)?
            .error_for_status()
            .map_err(|_| AuthError::VerificationFailed)?;
        let keys = bounded_json_response::<WorkOsJwkSet>(response)
            .await
            .map_err(|_| AuthError::VerificationFailed)?
            .keys;
        *self.inner.jwks.write().await = Some(CachedJwks {
            fetched_at: Instant::now(),
            keys: keys.clone(),
        });
        Ok(keys)
    }

    fn create_state(&self) -> Result<String, WorkOsError> {
        let state = Uuid::new_v4().to_string();
        let mut states = self
            .inner
            .pending_states
            .lock()
            .map_err(|_| WorkOsError::InvalidState)?;
        let now = Instant::now();
        states.retain(|_, created| now.duration_since(*created) <= STATE_TTL);
        states.insert(state.clone(), now);
        while states.len() > MAX_PENDING_STATES {
            let oldest = states
                .iter()
                .min_by_key(|(_, created)| **created)
                .map(|(state, _)| state.clone());
            let Some(oldest) = oldest else {
                break;
            };
            states.remove(&oldest);
        }
        Ok(state)
    }
}

#[async_trait::async_trait]
impl SessionVerifier for WorkOsAuth {
    async fn verify(&self, bearer_token: &str) -> Result<AuthenticatedAccount, AuthError> {
        let header = match decode_header(bearer_token) {
            Ok(header) => header,
            Err(error) => {
                eprintln!("auth verification failed: malformed JWT header ({error})");
                return Err(AuthError::VerificationFailed);
            }
        };
        if header.alg != Algorithm::RS256 {
            eprintln!(
                "auth verification failed: unsupported JWT algorithm {:?}",
                header.alg
            );
            return Err(AuthError::VerificationFailed);
        }
        let token_kid = match header.kid.as_deref().filter(|kid| !kid.trim().is_empty()) {
            Some(kid) if kid.len() <= 256 => kid,
            _ => {
                eprintln!("auth verification failed: JWT header has no usable key id");
                return Err(AuthError::VerificationFailed);
            }
        };
        let mut keys = match self.fetch_jwks(false).await {
            Ok(keys) => keys,
            Err(error) => {
                eprintln!("auth verification failed: could not fetch WorkOS JWKS ({error:?})");
                return Err(AuthError::VerificationFailed);
            }
        };
        let mut jwk = keys
            .iter()
            .find(|key| key.kid.as_deref() == Some(token_kid))
            .filter(|key| key.kty == "RSA")
            .cloned();
        // WorkOS can rotate signing keys while our short-lived JWKS cache is
        // still warm. Retry once with a fresh key set before rejecting an
        // otherwise well-formed session.
        if jwk.is_none()
            && let Ok(fresh_keys) = self.fetch_jwks(true).await
        {
            keys = fresh_keys;
            jwk = keys
                .iter()
                .find(|key| key.kid.as_deref() == Some(token_kid))
                .filter(|key| key.kty == "RSA")
                .cloned();
        }
        let jwk = match jwk {
            Some(jwk) => jwk,
            None => {
                eprintln!(
                    "auth verification failed: no RSA JWKS key matched token kid (keys={})",
                    keys.len()
                );
                return Err(AuthError::VerificationFailed);
            }
        };
        let key = DecodingKey::from_rsa_components(
            match jwk.n.as_deref() {
                Some(value) => value,
                None => {
                    eprintln!("auth verification failed: JWKS key has no modulus");
                    return Err(AuthError::VerificationFailed);
                }
            },
            match jwk.e.as_deref() {
                Some(value) => value,
                None => {
                    eprintln!("auth verification failed: JWKS key has no exponent");
                    return Err(AuthError::VerificationFailed);
                }
            },
        )
        .map_err(|error| {
            eprintln!("auth verification failed: invalid JWKS RSA key ({error})");
            AuthError::VerificationFailed
        })?;
        let base_issuer = self.inner.config.issuer.trim_end_matches('/');
        let trailing_slash_issuer = format!("{base_issuer}/");
        let user_management_issuer = format!(
            "{base_issuer}/user_management/{}",
            self.inner.config.client_id
        );
        let mut allowed_issuers = vec![
            base_issuer.to_owned(),
            trailing_slash_issuer,
            user_management_issuer,
        ];
        if let Some(token_issuer) = self.inner.config.token_issuer.as_deref() {
            let token_issuer = token_issuer.trim_end_matches('/');
            allowed_issuers.push(token_issuer.to_owned());
            allowed_issuers.push(format!("{token_issuer}/"));
        }

        let mut validation = Validation::new(Algorithm::RS256);
        validation.validate_nbf = true;
        validation.set_issuer(&allowed_issuers);
        if let Some(audience) = self.inner.config.audience.as_deref() {
            validation.set_audience(&[audience]);
        } else {
            validation.validate_aud = false;
        }
        let token = match decode::<WorkOsClaims>(bearer_token, &key, &validation) {
            Ok(token) => token,
            Err(error) => {
                eprintln!("auth verification failed: JWT validation/claims failed ({error})");
                return Err(AuthError::VerificationFailed);
            }
        };
        let client_id_matches = token.claims.client_id == self.inner.config.client_id;
        let issuer_matches = allowed_issuers
            .iter()
            .any(|issuer| issuer == &token.claims.iss);
        if !client_id_matches || !issuer_matches {
            eprintln!(
                "auth verification failed: token claims do not match server configuration (client_id_match={client_id_matches}, issuer_match={issuer_matches})"
            );
            return Err(AuthError::VerificationFailed);
        }
        let user_id = token.claims.sub.trim().to_owned();
        let session_id = token.claims.sid.trim().to_owned();
        let account_id = token
            .claims
            .org_id
            .as_deref()
            .unwrap_or(&user_id)
            .trim()
            .to_owned();
        if user_id.is_empty()
            || session_id.is_empty()
            || account_id.is_empty()
            || user_id.len() > MAX_IDENTITY_CLAIM_LEN
            || session_id.len() > MAX_IDENTITY_CLAIM_LEN
            || account_id.len() > MAX_IDENTITY_CLAIM_LEN
        {
            eprintln!("auth verification failed: required identity claim is empty");
            return Err(AuthError::VerificationFailed);
        }
        Ok(AuthenticatedAccount {
            user_id,
            account_id,
            session_id,
        })
    }
}

pub fn router(auth: Arc<WorkOsAuth>) -> Router {
    Router::new()
        .route("/auth/sign-in", get(sign_in).post(password_sign_in))
        .route("/auth/sign-up", get(sign_up).post(password_sign_up))
        .route("/auth/callback", get(callback))
        .route("/auth/verify-email", post(verify_email))
        .route("/auth/password-reset", post(request_password_reset))
        .route("/auth/password-reset/confirm", post(confirm_password_reset))
        .route("/auth/refresh", axum::routing::post(refresh))
        .route("/auth/logout", post(logout))
        // Every authentication mutation is protected against cross-site
        // form/login CSRF, including endpoints that do not yet carry a
        // session cookie.
        .layer(middleware::from_fn_with_state(
            auth.clone(),
            require_auth_origin,
        ))
        .layer(DefaultBodyLimit::max(64 * 1024))
        .with_state(auth)
}

async fn require_auth_origin(
    State(auth): State<Arc<WorkOsAuth>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if request.method() == axum::http::Method::POST
        && ensure_same_site_request(request.headers(), &auth.inner.config.allowed_origins).is_err()
    {
        return AuthRouteError::CsrfRejected.into_response();
    }
    next.run(request).await
}

async fn sign_in(State(auth): State<Arc<WorkOsAuth>>) -> Result<Response, AuthRouteError> {
    auth.authorization_redirect("sign-in")
}

async fn sign_up(State(auth): State<Arc<WorkOsAuth>>) -> Result<Response, AuthRouteError> {
    auth.authorization_redirect("sign-up")
}

async fn password_sign_in(
    State(auth): State<Arc<WorkOsAuth>>,
    Json(credentials): Json<PasswordCredentials>,
) -> Response {
    if let Err(message) = validate_credentials(&credentials) {
        return json_error(StatusCode::BAD_REQUEST, message);
    }
    match auth.authenticate_password(&credentials).await {
        Ok(session) => auth.authenticated_response(session),
        Err(WorkOsError::Api {
            code,
            pending_authentication_token: Some(token),
            ..
        }) if code == "email_verification_required" && token.len() <= 8 * 1024 => {
            let mut response = Json(AuthResponse {
                status: "verification_required",
                message: Some("check your email for the verification code".to_owned()),
                email: Some(credentials.email.trim().to_owned()),
                pending_authentication_token: Some(token.clone()),
            })
            .into_response();
            if let Err(error) = auth.set_pending_auth_cookie(&mut response, &token) {
                return auth.user_facing_error(error, "could not start email verification");
            }
            response
        }
        Err(error) => auth.user_facing_error(error, "email or password is incorrect"),
    }
}

async fn password_sign_up(
    State(auth): State<Arc<WorkOsAuth>>,
    Json(credentials): Json<PasswordCredentials>,
) -> Response {
    if let Err(message) = validate_credentials(&credentials) {
        return json_error(StatusCode::BAD_REQUEST, message);
    }
    if let Err(error) = auth.create_user(&credentials).await {
        return auth.user_facing_error(error, "we couldn't create that account");
    }
    password_sign_in(State(auth), Json(credentials)).await
}

async fn verify_email(
    State(auth): State<Arc<WorkOsAuth>>,
    headers: HeaderMap,
    Json(request): Json<EmailVerificationRequest>,
) -> Response {
    if request.code.trim().len() != 6
        || !request
            .code
            .trim()
            .chars()
            .all(|char| char.is_ascii_digit())
    {
        return json_error(
            StatusCode::BAD_REQUEST,
            "enter the six-digit code from your email",
        );
    }
    let pending_token = headers
        .get(COOKIE)
        .and_then(|value| value.to_str().ok())
        .and_then(|cookies| cookie_value(cookies, PENDING_AUTH_COOKIE))
        .or_else(|| request.pending_authentication_token.clone())
        .ok_or(WorkOsError::InvalidState);
    let pending_token = match pending_token {
        Ok(token) => token,
        Err(error) => {
            return auth.user_facing_error(error, "your verification session has expired");
        }
    };
    if pending_token.len() > 8 * 1024 {
        return auth.user_facing_error(
            WorkOsError::InvalidState,
            "your verification session has expired",
        );
    }
    match auth
        .authenticate_email_verification(&request, &pending_token)
        .await
    {
        Ok(session) => {
            let mut response = auth.authenticated_response(session);
            auth.clear_pending_auth_cookie(&mut response);
            response
        }
        Err(error) => auth.user_facing_error(error, "that verification code is not valid"),
    }
}

async fn request_password_reset(
    State(auth): State<Arc<WorkOsAuth>>,
    Json(request): Json<PasswordResetRequest>,
) -> Response {
    if !is_valid_email(&request.email) {
        return json_error(StatusCode::BAD_REQUEST, "enter your email address");
    }
    match auth.create_password_reset(&request.email).await {
        Ok(()) => Json(AuthResponse {
            status: "reset_requested",
            message: Some("if that email has an account, a reset link is on its way".to_owned()),
            email: None,
            pending_authentication_token: None,
        })
        .into_response(),
        Err(WorkOsError::Api { status: 404, .. }) => Json(AuthResponse {
            status: "reset_requested",
            message: Some("if that email has an account, a reset link is on its way".to_owned()),
            email: None,
            pending_authentication_token: None,
        })
        .into_response(),
        Err(error) => auth.user_facing_error(error, "we couldn't send a reset link right now"),
    }
}

async fn confirm_password_reset(
    State(auth): State<Arc<WorkOsAuth>>,
    Json(request): Json<PasswordResetConfirmation>,
) -> Response {
    if request.token.trim().is_empty()
        || request.token.len() > 8 * 1024
        || request.new_password.trim().len() < 10
        || request.new_password.len() > 1024
    {
        return json_error(
            StatusCode::BAD_REQUEST,
            "use a valid reset link and a password of at least 10 characters",
        );
    }
    match auth.reset_password(&request).await {
        Ok(()) => Json(AuthResponse {
            status: "password_reset",
            message: Some("your password has been reset".to_owned()),
            email: None,
            pending_authentication_token: None,
        })
        .into_response(),
        Err(error) => auth.user_facing_error(error, "we couldn't reset that password"),
    }
}

fn validate_credentials(credentials: &PasswordCredentials) -> Result<(), &'static str> {
    if !is_valid_email(&credentials.email) {
        return Err("enter a valid email address");
    }
    if credentials.password.trim().len() < 10 || credentials.password.len() > 1024 {
        return Err("your password must be at least 10 characters");
    }
    Ok(())
}

fn is_valid_email(email: &str) -> bool {
    let email = email.trim();
    email.len() >= 3
        && email.len() <= 320
        && email.contains('@')
        && email
            .rsplit_once('@')
            .is_some_and(|(_, domain)| domain.contains('.') && !domain.starts_with('.'))
}

fn json_error(status: StatusCode, message: &str) -> Response {
    (
        status,
        Json(AuthResponse {
            status: "error",
            message: Some(message.to_owned()),
            email: None,
            pending_authentication_token: None,
        }),
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
struct AuthCallbackQuery {
    code: Option<String>,
    state: Option<String>,
}

async fn callback(
    State(auth): State<Arc<WorkOsAuth>>,
    headers: HeaderMap,
    Query(query): Query<AuthCallbackQuery>,
) -> Result<Response, AuthRouteError> {
    let state = query.state.as_deref().ok_or(WorkOsError::InvalidState)?;
    let state_cookie = headers
        .get(COOKIE)
        .and_then(|value| value.to_str().ok())
        .and_then(|cookies| cookie_value(cookies, OAUTH_STATE_COOKIE))
        .ok_or(WorkOsError::InvalidState)?;
    if state_cookie != state {
        return Err(WorkOsError::InvalidState.into());
    }
    // The HttpOnly, host-only state cookie is the cross-instance binding. The
    // in-memory record remains a best-effort replay guard on the same process,
    // but a callback routed to another instance is still valid.
    let _ = auth.consume_state(state);
    let session = auth
        .exchange_code(query.code.as_deref().ok_or(WorkOsError::MissingCode)?)
        .await?;
    let mut response = Redirect::to(&auth.inner.config.post_login_redirect_uri).into_response();
    let secure = auth.secure_cookie_suffix();
    let state_cookie = format!(
        "{OAUTH_STATE_COOKIE}=; Path=/; HttpOnly{}; SameSite=Lax; Max-Age=0",
        secure
    );
    response.headers_mut().append(
        SET_COOKIE,
        HeaderValue::from_str(&state_cookie)
            .map_err(|error| WorkOsError::InvalidResponse(error.to_string()))?,
    );
    let cookie = format!(
        "{}={}; Path=/; HttpOnly{}; SameSite=Lax; Max-Age={}",
        auth.cookie_name(),
        session.access_token,
        secure,
        SESSION_MAX_AGE
    );
    set_cookie(&mut response, &cookie)?;
    let refresh_cookie = format!(
        "{}={}; Path=/; HttpOnly{}; SameSite=Lax; Max-Age={}",
        auth.refresh_cookie_name(),
        session.refresh_token,
        secure,
        REFRESH_MAX_AGE
    );
    response.headers_mut().append(
        SET_COOKIE,
        HeaderValue::from_str(&refresh_cookie)
            .map_err(|error| WorkOsError::InvalidResponse(error.to_string()))?,
    );
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        HeaderValue::from_static("no-store"),
    );
    Ok(response)
}

async fn refresh(
    State(auth): State<Arc<WorkOsAuth>>,
    headers: HeaderMap,
) -> Result<Response, AuthRouteError> {
    ensure_same_site_request(&headers, &auth.inner.config.allowed_origins)?;
    let refresh_token = headers
        .get(COOKIE)
        .and_then(|value| value.to_str().ok())
        .and_then(|cookies| cookie_value(cookies, &auth.refresh_cookie_name()))
        .ok_or(WorkOsError::MissingCode)?;
    let session = auth.refresh_session(&refresh_token).await?;
    let mut response = (axum::http::StatusCode::OK, "refreshed").into_response();
    let secure = auth.secure_cookie_suffix();
    let access_cookie = format!(
        "{}={}; Path=/; HttpOnly{}; SameSite=Lax; Max-Age={}",
        auth.cookie_name(),
        session.access_token,
        secure,
        SESSION_MAX_AGE
    );
    set_cookie(&mut response, &access_cookie)?;
    let rotated_refresh_cookie = format!(
        "{}={}; Path=/; HttpOnly{}; SameSite=Lax; Max-Age={}",
        auth.refresh_cookie_name(),
        session.refresh_token,
        secure,
        REFRESH_MAX_AGE
    );
    response.headers_mut().append(
        SET_COOKIE,
        HeaderValue::from_str(&rotated_refresh_cookie)
            .map_err(|error| WorkOsError::InvalidResponse(error.to_string()))?,
    );
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        HeaderValue::from_static("no-store"),
    );
    Ok(response)
}

async fn logout(State(auth): State<Arc<WorkOsAuth>>, headers: HeaderMap) -> Response {
    if let Err(error) = ensure_same_site_request(&headers, &auth.inner.config.allowed_origins) {
        return error.into_response();
    }
    let secure = auth.secure_cookie_suffix();
    let cookie = format!(
        "{}=; Path=/; HttpOnly{}; SameSite=Lax; Max-Age=0",
        auth.cookie_name(),
        secure
    );
    let mut response = Json(AuthResponse {
        status: "signed_out",
        message: None,
        email: None,
        pending_authentication_token: None,
    })
    .into_response();
    response.headers_mut().insert(
        SET_COOKIE,
        HeaderValue::from_str(&cookie).expect("configured cookie name should be valid"),
    );
    let refresh_cookie = format!(
        "{}=; Path=/; HttpOnly{}; SameSite=Lax; Max-Age=0",
        auth.refresh_cookie_name(),
        secure
    );
    response.headers_mut().append(
        SET_COOKIE,
        HeaderValue::from_str(&refresh_cookie).expect("configured cookie name should be valid"),
    );
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        HeaderValue::from_static("no-store"),
    );
    response
}

fn ensure_same_site_request(
    headers: &HeaderMap,
    allowed_origins: &[String],
) -> Result<(), AuthRouteError> {
    if headers
        .get("sec-fetch-site")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("cross-site"))
    {
        return Err(AuthRouteError::CsrfRejected);
    }
    let fetch_site = headers
        .get("sec-fetch-site")
        .and_then(|value| value.to_str().ok());
    if fetch_site.is_some_and(|value| value.eq_ignore_ascii_case("same-origin")) {
        return Ok(());
    }
    let source = headers
        .get("origin")
        .or_else(|| headers.get("referer"))
        .and_then(|value| value.to_str().ok())
        .ok_or(AuthRouteError::CsrfRejected)?;
    let url = Url::parse(source).map_err(|_| AuthRouteError::CsrfRejected)?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(AuthRouteError::CsrfRejected);
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(AuthRouteError::CsrfRejected);
    }
    let source_host = url.host_str().ok_or(AuthRouteError::CsrfRejected)?;
    let request_host = headers
        .get("host")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    let source_host_authority = if source_host.contains(':') && !source_host.starts_with('[') {
        format!("[{source_host}]")
    } else {
        source_host.to_owned()
    };
    let source_authority = match url.port() {
        Some(port) => format!("{source_host_authority}:{port}"),
        None => source_host_authority,
    };
    let source_origin = format!("{}://{source_authority}", url.scheme());
    if allowed_origins
        .iter()
        .any(|origin| origin == &source_origin)
    {
        return Ok(());
    }
    let request_host_name = Url::parse(&format!("http://{request_host}"))
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned));
    let local_dev = matches!(source_host, "localhost" | "127.0.0.1" | "::1")
        && request_host_name
            .as_deref()
            .is_some_and(|host| matches!(host, "localhost" | "127.0.0.1" | "::1"));
    // Production origins must be explicitly configured. Loopback origins are
    // the only implicit exception for a local UI/API split-port setup.
    if !local_dev {
        return Err(AuthRouteError::CsrfRejected);
    }
    Ok(())
}

impl WorkOsAuth {
    fn authorization_redirect(&self, screen_hint: &str) -> Result<Response, AuthRouteError> {
        let location = self.authorization_url(screen_hint)?;
        let state = Url::parse(&location)
            .ok()
            .and_then(|url| {
                url.query_pairs()
                    .find(|(key, _)| key == "state")
                    .map(|(_, value)| value.into_owned())
            })
            .ok_or(WorkOsError::InvalidState)?;
        let mut response = Redirect::temporary(&location).into_response();
        let cookie = format!(
            "{OAUTH_STATE_COOKIE}={state}; Path=/; HttpOnly{}; SameSite=Lax; Max-Age={}",
            self.secure_cookie_suffix(),
            STATE_TTL.as_secs()
        );
        set_cookie(&mut response, &cookie)?;
        Ok(response)
    }

    fn secure_cookie_suffix(&self) -> &'static str {
        if Url::parse(&self.inner.config.redirect_uri)
            .ok()
            .is_some_and(|url| url.scheme() == "https")
        {
            "; Secure"
        } else {
            ""
        }
    }

    fn authenticated_response(&self, session: WorkOsSession) -> Response {
        let mut response = Json(AuthResponse {
            status: "authenticated",
            message: None,
            email: None,
            pending_authentication_token: None,
        })
        .into_response();
        let secure = self.secure_cookie_suffix();
        let access_cookie = format!(
            "{}={}; Path=/; HttpOnly{}; SameSite=Lax; Max-Age={}",
            self.cookie_name(),
            session.access_token,
            secure,
            SESSION_MAX_AGE
        );
        let refresh_cookie = format!(
            "{}={}; Path=/; HttpOnly{}; SameSite=Lax; Max-Age={}",
            self.refresh_cookie_name(),
            session.refresh_token,
            secure,
            REFRESH_MAX_AGE
        );
        if set_cookie(&mut response, &access_cookie).is_err()
            || set_cookie(&mut response, &refresh_cookie).is_err()
        {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "could not create a session",
            );
        }
        response.headers_mut().insert(
            axum::http::header::CACHE_CONTROL,
            HeaderValue::from_static("no-store"),
        );
        response
    }

    fn set_pending_auth_cookie(
        &self,
        response: &mut Response,
        pending_token: &str,
    ) -> Result<(), WorkOsError> {
        let cookie = format!(
            "{}={}; Path=/; HttpOnly{}; SameSite=Lax; Max-Age={}",
            PENDING_AUTH_COOKIE,
            pending_token,
            self.secure_cookie_suffix(),
            PENDING_AUTH_MAX_AGE
        );
        set_cookie(response, &cookie)
    }

    fn clear_pending_auth_cookie(&self, response: &mut Response) {
        let cookie = format!(
            "{}=; Path=/; HttpOnly{}; SameSite=Lax; Max-Age=0",
            PENDING_AUTH_COOKIE,
            self.secure_cookie_suffix()
        );
        let _ = set_cookie(response, &cookie);
    }

    fn user_facing_error(&self, error: WorkOsError, fallback: &str) -> Response {
        match error {
            WorkOsError::Api { code, detail, .. }
                if matches!(
                    code.as_str(),
                    "user_already_exists" | "email_already_exists" | "email_not_available"
                ) || detail
                    .as_deref()
                    .is_some_and(|detail| detail.starts_with("email_not_available:")) =>
            {
                json_error(
                    StatusCode::CONFLICT,
                    "this email is already registered; try signing in or resetting your password",
                )
            }
            WorkOsError::Api {
                code,
                message,
                detail,
                ..
            } if code == "user_creation_error" => {
                let message = detail
                    .map(|detail| format!("WorkOS could not create this account: {detail}"))
                    .unwrap_or_else(|| format!("WorkOS could not create this account: {message}"));
                json_error(StatusCode::BAD_REQUEST, &message)
            }
            WorkOsError::Api { code, .. }
                if matches!(
                    code.as_str(),
                    "password_policy_violation" | "password_strength_error"
                ) =>
            {
                json_error(
                    StatusCode::BAD_REQUEST,
                    "choose a stronger password with at least 10 characters, a number, and a symbol",
                )
            }
            WorkOsError::Api { status: 401, .. } => json_error(
                StatusCode::BAD_GATEWAY,
                "the server's WorkOS credentials were rejected; check WORKOS_API_KEY",
            ),
            WorkOsError::Api { status: 403, .. } => json_error(
                StatusCode::BAD_GATEWAY,
                "the server's WorkOS credentials do not have permission for this flow",
            ),
            WorkOsError::Api {
                status: 400, code, ..
            } if matches!(
                code.as_str(),
                "invalid_credentials" | "authentication_failed" | "invalid_password"
            ) =>
            {
                json_error(StatusCode::UNAUTHORIZED, fallback)
            }
            WorkOsError::Api { status, code, .. } if status == 400 || status == 422 => {
                let message =
                    format!("WorkOS rejected the request ({code}); check the form details");
                json_error(StatusCode::BAD_REQUEST, &message)
            }
            WorkOsError::Api { status, code, .. } => {
                let message = format!("WorkOS rejected the request ({status}, {code})");
                json_error(StatusCode::BAD_GATEWAY, &message)
            }
            WorkOsError::InvalidState => json_error(StatusCode::BAD_REQUEST, fallback),
            WorkOsError::Request(_) => json_error(
                StatusCode::BAD_GATEWAY,
                "the server could not reach WorkOS; please try again",
            ),
            WorkOsError::InvalidResponse(_) => json_error(
                StatusCode::BAD_GATEWAY,
                "WorkOS returned an unexpected response; please try again",
            ),
            _ => json_error(StatusCode::INTERNAL_SERVER_ERROR, fallback),
        }
    }
}

fn set_cookie(response: &mut Response, cookie: &str) -> Result<(), WorkOsError> {
    response.headers_mut().append(
        SET_COOKIE,
        HeaderValue::from_str(cookie)
            .map_err(|error| WorkOsError::InvalidResponse(error.to_string()))?,
    );
    Ok(())
}

fn cookie_value(cookies: &str, cookie_name: &str) -> Option<String> {
    cookies.split(';').find_map(|cookie| {
        let (name, value) = cookie.trim().split_once('=')?;
        (name == cookie_name && !value.trim().is_empty()).then(|| value.to_owned())
    })
}

#[derive(Debug)]
enum AuthRouteError {
    WorkOs(WorkOsError),
    CsrfRejected,
}

impl From<WorkOsError> for AuthRouteError {
    fn from(error: WorkOsError) -> Self {
        Self::WorkOs(error)
    }
}

impl IntoResponse for AuthRouteError {
    fn into_response(self) -> Response {
        let is_csrf_rejected = matches!(&self, Self::CsrfRejected);
        let status = match self {
            Self::CsrfRejected => axum::http::StatusCode::FORBIDDEN,
            Self::WorkOs(WorkOsError::Request(_))
            | Self::WorkOs(WorkOsError::InvalidResponse(_)) => axum::http::StatusCode::BAD_GATEWAY,
            Self::WorkOs(WorkOsError::InvalidState) | Self::WorkOs(WorkOsError::MissingCode) => {
                axum::http::StatusCode::BAD_REQUEST
            }
            Self::WorkOs(WorkOsError::Api { status, .. }) => {
                axum::http::StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY)
            }
            Self::WorkOs(WorkOsError::MissingEnvironment(_))
            | Self::WorkOs(WorkOsError::InvalidConfiguration(_)) => {
                axum::http::StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        // Do not serialize provider details, authorization codes, refresh
        // tokens, or pending-authentication tokens into a browser response.
        // Those fields are useful only to bounded server-side diagnostics.
        let message = if is_csrf_rejected {
            "request origin was rejected"
        } else {
            "authentication failed"
        };
        (status, message).into_response()
    }
}

fn normalize_origin(value: &str) -> Option<String> {
    let url = Url::parse(value.trim()).ok()?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return None;
    }
    let host = url.host_str()?;
    let host = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_owned()
    };
    let authority = match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host,
    };
    Some(format!("{}://{authority}", url.scheme()))
}

fn validate_service_url(value: &str, label: &str) -> Result<(), WorkOsError> {
    let url = Url::parse(value).map_err(|error| {
        WorkOsError::InvalidConfiguration(format!("{label} is invalid: {error}"))
    })?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || (url.scheme() == "http"
            && !is_local_development_host(url.host_str().unwrap_or_default()))
    {
        return Err(WorkOsError::InvalidConfiguration(format!(
            "{label} must be an HTTP(S) URL without credentials, query, or fragment"
        )));
    }
    Ok(())
}

fn is_local_development_host(host: &str) -> bool {
    matches!(
        host.trim_matches(['[', ']']),
        "localhost" | "127.0.0.1" | "::1"
    )
}

async fn bounded_response_bytes(response: reqwest::Response) -> Result<Vec<u8>, String> {
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| error.to_string())?;
        if bytes.len().saturating_add(chunk.len()) > MAX_WORKOS_RESPONSE_BYTES {
            return Err("WorkOS response is too large".to_owned());
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

async fn bounded_json_response<T: DeserializeOwned>(
    response: reqwest::Response,
) -> Result<T, String> {
    let bytes = bounded_response_bytes(response).await?;
    serde_json::from_slice(&bytes).map_err(|error| error.to_string())
}

fn required_env(name: &'static str) -> Result<String, WorkOsError> {
    std::env::var(name).map_err(|_| WorkOsError::MissingEnvironment(name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::header::{HOST, ORIGIN};

    fn auth_config() -> WorkOsAuthConfig {
        WorkOsAuthConfig {
            api_key: "sk_test_example".to_owned(),
            client_id: "client_example".to_owned(),
            redirect_uri: "http://localhost:3000/auth/callback".to_owned(),
            post_login_redirect_uri: "/app".to_owned(),
            issuer: "https://api.workos.com".to_owned(),
            token_issuer: None,
            audience: None,
            cookie_name: "task_space_session".to_owned(),
            allowed_origins: vec!["http://localhost:8080".to_owned()],
        }
    }

    #[test]
    fn auth_service_urls_cannot_use_non_http_schemes_or_credentials() {
        let mut config = auth_config();
        config.issuer = "file:///tmp/workos".to_owned();
        assert!(matches!(
            WorkOsAuth::new(config),
            Err(WorkOsError::InvalidConfiguration(message))
                if message.contains("issuer")
        ));

        let mut config = auth_config();
        config.redirect_uri = "http://auth.example.com/callback".to_owned();
        assert!(matches!(
            WorkOsAuth::new(config),
            Err(WorkOsError::InvalidConfiguration(message))
                if message.contains("redirect URI")
        ));

        let mut config = auth_config();
        config.token_issuer = Some("https://user:pass@example.com".to_owned());
        assert!(matches!(
            WorkOsAuth::new(config),
            Err(WorkOsError::InvalidConfiguration(message))
                if message.contains("token issuer")
        ));
    }

    #[test]
    fn authorization_state_cache_is_bounded() {
        let auth = WorkOsAuth::new(auth_config()).expect("test auth config should be valid");
        for _ in 0..(MAX_PENDING_STATES + 32) {
            auth.create_state().expect("state should be generated");
        }
        assert!(
            auth.inner
                .pending_states
                .lock()
                .expect("state lock should not be poisoned")
                .len()
                <= MAX_PENDING_STATES
        );
    }

    #[test]
    fn csrf_accepts_configured_origin_and_rejects_cross_site() {
        let mut headers = HeaderMap::new();
        headers.insert(ORIGIN, HeaderValue::from_static("https://app.example.com"));
        headers.insert(HOST, HeaderValue::from_static("api.example.com"));
        let allowed = vec!["https://app.example.com".to_owned()];
        assert!(ensure_same_site_request(&headers, &allowed).is_ok());

        headers.insert("sec-fetch-site", HeaderValue::from_static("cross-site"));
        assert!(matches!(
            ensure_same_site_request(&headers, &allowed),
            Err(AuthRouteError::CsrfRejected)
        ));
    }

    #[test]
    fn csrf_rejects_missing_or_untrusted_origin() {
        let headers = HeaderMap::new();
        assert!(matches!(
            ensure_same_site_request(&headers, &[]),
            Err(AuthRouteError::CsrfRejected)
        ));

        let mut headers = HeaderMap::new();
        headers.insert(ORIGIN, HeaderValue::from_static("https://evil.example"));
        headers.insert(HOST, HeaderValue::from_static("api.example.com"));
        assert!(matches!(
            ensure_same_site_request(&headers, &[]),
            Err(AuthRouteError::CsrfRejected)
        ));
    }

    #[test]
    fn csrf_does_not_trust_localhost_origin_for_a_production_host() {
        let mut headers = HeaderMap::new();
        headers.insert(ORIGIN, HeaderValue::from_static("http://localhost:8080"));
        headers.insert(HOST, HeaderValue::from_static("api.example.com"));
        assert!(matches!(
            ensure_same_site_request(&headers, &[]),
            Err(AuthRouteError::CsrfRejected)
        ));
    }

    #[test]
    fn csrf_rejects_origin_credentials_even_when_host_is_allowed() {
        let mut headers = HeaderMap::new();
        headers.insert(
            ORIGIN,
            HeaderValue::from_static("https://user:pass@app.example.com"),
        );
        headers.insert(HOST, HeaderValue::from_static("api.example.com"));
        assert!(matches!(
            ensure_same_site_request(&headers, &["https://app.example.com".to_owned()]),
            Err(AuthRouteError::CsrfRejected)
        ));
    }

    #[test]
    fn origin_normalization_rejects_paths_and_credentials() {
        assert_eq!(
            normalize_origin(" https://app.example.com:8443/ "),
            Some("https://app.example.com:8443".to_owned())
        );
        assert_eq!(
            normalize_origin("http://[::1]:8080/"),
            Some("http://[::1]:8080".to_owned())
        );
        assert!(normalize_origin("https://app.example.com/path").is_none());
        assert!(normalize_origin("https://user:pass@app.example.com/").is_none());
    }
}
