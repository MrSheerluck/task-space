//! WorkOS AuthKit integration.
//!
//! This module keeps the authentication UI in our application while using
//! WorkOS's headless User Management API for password authentication, email
//! verification, and password resets. The WorkOS API key and session tokens
//! remain server-side.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::{Query, State};
use axum::http::header::{COOKIE, HeaderValue, SET_COOKIE};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use reqwest::Client;
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

#[derive(Clone, Debug)]
pub struct WorkOsAuthConfig {
    pub api_key: String,
    pub client_id: String,
    pub redirect_uri: String,
    pub post_login_redirect_uri: String,
    pub issuer: String,
    pub cookie_name: String,
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
            cookie_name: std::env::var("WORKOS_SESSION_COOKIE")
                .unwrap_or_else(|_| "task_space_session".to_owned()),
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
        Url::parse(&config.redirect_uri)
            .map_err(|error| WorkOsError::InvalidConfiguration(error.to_string()))?;
        if config.post_login_redirect_uri.starts_with("http://")
            || config.post_login_redirect_uri.starts_with("https://")
        {
            Url::parse(&config.post_login_redirect_uri)
                .map_err(|error| WorkOsError::InvalidConfiguration(error.to_string()))?;
        } else if !config.post_login_redirect_uri.starts_with('/') {
            return Err(WorkOsError::InvalidConfiguration(
                "post-login redirect must be an absolute URL or path".to_owned(),
            ));
        }
        Url::parse(&config.issuer)
            .map_err(|error| WorkOsError::InvalidConfiguration(error.to_string()))?;

        Ok(Self {
            inner: Arc::new(WorkOsAuthInner {
                config,
                client: Client::new(),
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
        let response = response
            .json::<WorkOsAuthenticateResponse>()
            .await
            .map_err(|error| WorkOsError::InvalidResponse(error.to_string()))?;
        let account_id = response
            .organization_id
            .clone()
            .unwrap_or_else(|| response.user.id.clone());
        Ok(WorkOsSession {
            account: AuthenticatedAccount {
                user_id: response.user.id,
                account_id,
                session_id: String::new(),
            },
            access_token: response.access_token,
            refresh_token: response.refresh_token,
        })
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
        let response = response
            .json::<WorkOsAuthenticateResponse>()
            .await
            .map_err(|error| WorkOsError::InvalidResponse(error.to_string()))?;
        self.session_from_authenticate_response(response)
    }

    fn session_from_authenticate_response(
        &self,
        response: WorkOsAuthenticateResponse,
    ) -> Result<WorkOsSession, WorkOsError> {
        let account_id = response
            .organization_id
            .clone()
            .unwrap_or_else(|| response.user.id.clone());
        Ok(WorkOsSession {
            account: AuthenticatedAccount {
                user_id: response.user.id,
                account_id,
                session_id: String::new(),
            },
            access_token: response.access_token,
            refresh_token: response.refresh_token,
        })
    }

    async fn provider_error(&self, response: reqwest::Response) -> WorkOsError {
        let status = response.status().as_u16();
        let body = response
            .json::<serde_json::Value>()
            .await
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

    async fn fetch_jwks(&self) -> Result<Vec<WorkOsJwk>, AuthError> {
        if let Some(cached) = self.inner.jwks.read().await.as_ref()
            && cached.fetched_at.elapsed() <= JWKS_TTL
        {
            return Ok(cached.keys.clone());
        }
        let endpoint = format!(
            "{}/sso/jwks/{}",
            self.inner.config.issuer.trim_end_matches('/'),
            self.inner.config.client_id
        );
        let keys = self
            .inner
            .client
            .get(endpoint)
            .send()
            .await
            .map_err(|_| AuthError::VerificationFailed)?
            .error_for_status()
            .map_err(|_| AuthError::VerificationFailed)?
            .json::<WorkOsJwkSet>()
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
        self.inner
            .pending_states
            .lock()
            .map_err(|_| WorkOsError::InvalidState)?
            .insert(state.clone(), Instant::now());
        Ok(state)
    }
}

#[async_trait::async_trait]
impl SessionVerifier for WorkOsAuth {
    async fn verify(&self, bearer_token: &str) -> Result<AuthenticatedAccount, AuthError> {
        let header = decode_header(bearer_token).map_err(|_| AuthError::VerificationFailed)?;
        if header.alg != Algorithm::RS256 {
            return Err(AuthError::VerificationFailed);
        }
        let keys = self.fetch_jwks().await?;
        let jwk = keys
            .iter()
            .find(|key| key.kid.as_deref() == header.kid.as_deref())
            .filter(|key| key.kty == "RSA")
            .ok_or(AuthError::VerificationFailed)?;
        let key = DecodingKey::from_rsa_components(
            jwk.n.as_deref().ok_or(AuthError::VerificationFailed)?,
            jwk.e.as_deref().ok_or(AuthError::VerificationFailed)?,
        )
        .map_err(|_| AuthError::VerificationFailed)?;
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[self.inner.config.issuer.as_str()]);
        validation.validate_aud = false;
        let token = decode::<WorkOsClaims>(bearer_token, &key, &validation)
            .map_err(|_| AuthError::VerificationFailed)?;
        if token.claims.client_id != self.inner.config.client_id
            || token.claims.iss != self.inner.config.issuer
        {
            return Err(AuthError::VerificationFailed);
        }
        let user_id = token.claims.sub;
        Ok(AuthenticatedAccount {
            user_id: user_id.clone(),
            account_id: token.claims.org_id.unwrap_or(user_id),
            session_id: token.claims.sid,
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
        .route("/auth/logout", get(logout))
        .with_state(auth)
}

async fn sign_in(State(auth): State<Arc<WorkOsAuth>>) -> Result<Redirect, AuthRouteError> {
    Ok(Redirect::temporary(&auth.authorization_url("sign-in")?))
}

async fn sign_up(State(auth): State<Arc<WorkOsAuth>>) -> Result<Redirect, AuthRouteError> {
    Ok(Redirect::temporary(&auth.authorization_url("sign-up")?))
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
        }) if code == "email_verification_required" => {
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
    if request.token.trim().is_empty() || request.new_password.trim().len() < 10 {
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
    if credentials.password.trim().len() < 10 {
        return Err("your password must be at least 10 characters");
    }
    Ok(())
}

fn is_valid_email(email: &str) -> bool {
    let email = email.trim();
    email.len() >= 3
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
    Query(query): Query<AuthCallbackQuery>,
) -> Result<Response, AuthRouteError> {
    let state = query.state.as_deref().ok_or(WorkOsError::InvalidState)?;
    auth.consume_state(state)?;
    let session = auth
        .exchange_code(query.code.as_deref().ok_or(WorkOsError::MissingCode)?)
        .await?;
    let mut response = Redirect::to(&auth.inner.config.post_login_redirect_uri).into_response();
    let secure = auth.secure_cookie_suffix();
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
    Ok(response)
}

async fn logout(State(auth): State<Arc<WorkOsAuth>>) -> Response {
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
    response
}

impl WorkOsAuth {
    fn secure_cookie_suffix(&self) -> &'static str {
        Url::parse(&self.inner.config.redirect_uri)
            .ok()
            .is_some_and(|url| url.scheme() == "https")
            .then_some("; Secure")
            .unwrap_or_default()
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
}

impl From<WorkOsError> for AuthRouteError {
    fn from(error: WorkOsError) -> Self {
        Self::WorkOs(error)
    }
}

impl IntoResponse for AuthRouteError {
    fn into_response(self) -> Response {
        let status = match self {
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
        (status, format!("authentication failed: {self:?}")).into_response()
    }
}

fn required_env(name: &'static str) -> Result<String, WorkOsError> {
    std::env::var(name).map_err(|_| WorkOsError::MissingEnvironment(name))
}
