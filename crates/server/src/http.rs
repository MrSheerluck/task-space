//! Authenticated HTTP/SSE transport for the sync engine.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::extract::{Extension, Path, State};
use axum::http::{Request, StatusCode, header::CACHE_CONTROL};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::Stream;
use serde::{Deserialize, Serialize};
use task_core::sync::{
    SYNC_PROTOCOL_VERSION, SyncPullRequest, SyncPullResponse, SyncPushRequest, SyncPushResponse,
};
use tokio_stream::StreamExt;

use crate::dodo::{BillingInterval, DodoClient, DodoError};
use crate::postgres::{PostgresBillingStore, PostgresStoreError, PostgresSyncStore};

/// The identity supplied by authentication middleware. Sync handlers never
/// accept an account id from a request body or query parameter.
#[derive(Clone, Debug)]
pub struct AuthenticatedAccount {
    pub user_id: String,
    pub account_id: String,
    pub session_id: String,
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("authorization header is missing")]
    MissingAuthorization,
    #[error("authorization header is not a bearer token")]
    InvalidAuthorization,
    #[error("session verification failed")]
    VerificationFailed,
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::MissingAuthorization | Self::InvalidAuthorization => {
                axum::http::StatusCode::UNAUTHORIZED
            }
            Self::VerificationFailed => axum::http::StatusCode::UNAUTHORIZED,
        };
        (status, self.to_string()).into_response()
    }
}

/// Provider-neutral session verification contract. A WorkOS/JWT implementation
/// can be supplied later without changing the sync handlers.
#[async_trait]
pub trait SessionVerifier: Send + Sync {
    async fn verify(&self, bearer_token: &str) -> Result<AuthenticatedAccount, AuthError>;
}

#[derive(Clone)]
pub struct SyncHttpState {
    pub store: PostgresSyncStore,
    pub billing: PostgresBillingStore,
    pub payments: DodoClient,
    pub verifier: Arc<dyn SessionVerifier>,
    pub session_cookie_name: String,
}

/// Routes for the authenticated sync surface. The verifier and store are
/// carried by one router state so Axum can enforce authentication before any
/// sync handler runs.
pub fn protected_sync_router(state: SyncHttpState) -> Router {
    Router::new()
        .route("/sync/pull", post(sync_pull))
        .route("/sync/push", post(sync_push))
        .route("/sync/spaces", get(list_spaces))
        .route("/sync/spaces/{space_id}", post(register_space))
        .route("/sync/events", get(sync_events))
        .route("/account/entitlement", get(account_entitlement))
        .route("/billing/checkout", post(create_checkout))
        .route("/billing/portal", post(create_billing_portal))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_session,
        ))
        .with_state(state)
}

async fn require_session(
    State(state): State<SyncHttpState>,
    mut request: Request<Body>,
    next: Next,
) -> Result<Response, AuthError> {
    let token = if let Some(header) = request.headers().get(axum::http::header::AUTHORIZATION) {
        let authorization = header
            .to_str()
            .map_err(|_| AuthError::InvalidAuthorization)?;
        authorization
            .strip_prefix("Bearer ")
            .filter(|token| !token.trim().is_empty())
            .map(str::to_owned)
            .ok_or(AuthError::InvalidAuthorization)?
    } else {
        match request
            .headers()
            .get(axum::http::header::COOKIE)
            .and_then(|value| value.to_str().ok())
            .and_then(|cookies| cookie_value(cookies, &state.session_cookie_name))
        {
            Some(token) => token,
            None => {
                eprintln!(
                    "auth rejected: no {} cookie for {}",
                    state.session_cookie_name,
                    request.uri().path()
                );
                return Err(AuthError::MissingAuthorization);
            }
        }
    };
    let account = match state.verifier.verify(&token).await {
        Ok(account) => account,
        Err(error) => {
            eprintln!(
                "auth rejected: session verification failed for {}",
                request.uri().path()
            );
            return Err(error);
        }
    };
    request.extensions_mut().insert(account);
    Ok(next.run(request).await)
}

fn cookie_value(cookies: &str, cookie_name: &str) -> Option<String> {
    cookies.split(';').find_map(|cookie| {
        let (name, value) = cookie.trim().split_once('=')?;
        (name == cookie_name && !value.trim().is_empty()).then(|| value.to_owned())
    })
}

async fn sync_pull(
    State(state): State<SyncHttpState>,
    Extension(account): Extension<AuthenticatedAccount>,
    Json(request): Json<SyncPullRequest>,
) -> Result<Json<SyncPullResponse>, ApiError> {
    state
        .store
        .pull(&account.account_id, &request)
        .await
        .map(Json)
        .map_err(ApiError::from)
}

async fn sync_push(
    State(state): State<SyncHttpState>,
    Extension(account): Extension<AuthenticatedAccount>,
    Json(request): Json<SyncPushRequest>,
) -> Result<Json<SyncPushResponse>, ApiError> {
    let event = state
        .store
        .push(&account.account_id, &request)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(SyncPushResponse {
        protocol_version: SYNC_PROTOCOL_VERSION,
        space_id: request.space_id,
        accepted: event.is_some(),
        event_id: event.map(|event| event.event_id),
    }))
}

async fn sync_events(
    State(state): State<SyncHttpState>,
    Extension(account): Extension<AuthenticatedAccount>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = tokio_stream::wrappers::BroadcastStream::new(state.store.subscribe()).filter_map(
        move |message| match message {
            Ok(delivery) if delivery.account_id == account.account_id => Event::default()
                .event("space-update")
                .json_data(delivery.event)
                .ok()
                .map(Ok),
            _ => None,
        },
    );
    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(30))
            .text("heartbeat"),
    )
}

async fn list_spaces(
    State(state): State<SyncHttpState>,
    Extension(account): Extension<AuthenticatedAccount>,
) -> Result<Json<Vec<task_core::Space>>, ApiError> {
    state
        .store
        .list_spaces(&account.account_id)
        .await
        .map(Json)
        .map_err(ApiError::from)
}

#[derive(Debug, Deserialize)]
struct RegisterSpaceRequest {
    name: String,
}

async fn register_space(
    State(state): State<SyncHttpState>,
    Extension(account): Extension<AuthenticatedAccount>,
    Path(space_id): Path<u64>,
    Json(request): Json<RegisterSpaceRequest>,
) -> Result<StatusCode, ApiError> {
    if request.name.trim().is_empty() {
        return Err(ApiError::Store(PostgresStoreError::InvalidInput(
            "space name cannot be empty".to_owned(),
        )));
    }
    state
        .store
        .register_space(&account.account_id, space_id, request.name.trim())
        .await
        .map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn account_entitlement(
    State(state): State<SyncHttpState>,
    Extension(account): Extension<AuthenticatedAccount>,
) -> Result<Response, ApiError> {
    let entitlement = state
        .billing
        .entitlement(&account.account_id)
        .await
        .map_err(ApiError::from)?;
    let mut response = Json(entitlement).into_response();
    response.headers_mut().insert(
        CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-store"),
    );
    Ok(response)
}

#[derive(Debug, Deserialize)]
struct CheckoutRequest {
    interval: BillingInterval,
}

#[derive(Debug, Serialize)]
struct CheckoutResponse {
    checkout_url: String,
}

#[derive(Debug, Serialize)]
struct BillingPortalResponse {
    portal_url: String,
}

async fn create_checkout(
    State(state): State<SyncHttpState>,
    Extension(account): Extension<AuthenticatedAccount>,
    Json(request): Json<CheckoutRequest>,
) -> Result<Json<CheckoutResponse>, ApiError> {
    let checkout_url = state
        .payments
        .create_checkout(&account.account_id, request.interval)
        .await
        .map_err(|error| {
            eprintln!("Dodo checkout failed for an authenticated account: {error}");
            ApiError::from(error)
        })?;
    Ok(Json(CheckoutResponse { checkout_url }))
}

async fn create_billing_portal(
    State(state): State<SyncHttpState>,
    Extension(account): Extension<AuthenticatedAccount>,
) -> Result<Json<BillingPortalResponse>, ApiError> {
    let entitlement = state
        .billing
        .entitlement(&account.account_id)
        .await
        .map_err(ApiError::from)?;
    let customer_id = entitlement
        .provider_customer_id
        .as_deref()
        .filter(|_| entitlement.provider.as_deref() == Some("dodo"))
        .ok_or(ApiError::BillingPortalUnavailable)?;
    let portal_url = state
        .payments
        .create_customer_portal(customer_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(BillingPortalResponse { portal_url }))
}

#[derive(Debug)]
enum ApiError {
    Store(PostgresStoreError),
    Dodo(DodoError),
    BillingPortalUnavailable,
}

impl From<PostgresStoreError> for ApiError {
    fn from(error: PostgresStoreError) -> Self {
        Self::Store(error)
    }
}

impl From<DodoError> for ApiError {
    fn from(error: DodoError) -> Self {
        Self::Dodo(error)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::Store(PostgresStoreError::SpaceAccessDenied) => (
                axum::http::StatusCode::FORBIDDEN,
                "space access denied".to_owned(),
            ),
            Self::Store(PostgresStoreError::Database(_)) => (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "database unavailable".to_owned(),
            ),
            Self::Store(error) => (axum::http::StatusCode::BAD_REQUEST, error.to_string()),
            Self::Dodo(DodoError::Api { status, .. }) => (
                axum::http::StatusCode::BAD_GATEWAY,
                format!("payment provider rejected checkout (HTTP {status})"),
            ),
            Self::Dodo(DodoError::Request(_)) => (
                axum::http::StatusCode::BAD_GATEWAY,
                "payment provider unavailable; please try again".to_owned(),
            ),
            Self::Dodo(error) => (axum::http::StatusCode::BAD_REQUEST, error.to_string()),
            Self::BillingPortalUnavailable => (
                axum::http::StatusCode::BAD_REQUEST,
                "billing portal is unavailable for this account".to_owned(),
            ),
        };
        (status, message).into_response()
    }
}
