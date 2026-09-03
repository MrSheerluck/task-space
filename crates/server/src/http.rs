//! Authenticated HTTP/SSE transport for the sync engine.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::extract::{Extension, State};
use axum::http::Request;
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::Stream;
use task_core::billing::Entitlement;
use task_core::sync::{
    SYNC_PROTOCOL_VERSION, SyncPullRequest, SyncPullResponse, SyncPushRequest, SyncPushResponse,
};
use tokio_stream::StreamExt;

use crate::billing::{BillingStore, BillingStoreError};
use crate::{SyncStore, SyncStoreError};

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
    pub store: SyncStore,
    pub billing: BillingStore,
    pub verifier: Arc<dyn SessionVerifier>,
}

/// Routes for the authenticated sync surface. The verifier and store are
/// carried by one router state so Axum can enforce authentication before any
/// sync handler runs.
pub fn protected_sync_router(state: SyncHttpState) -> Router {
    Router::new()
        .route("/sync/pull", post(sync_pull))
        .route("/sync/push", post(sync_push))
        .route("/sync/events", get(sync_events))
        .route("/account/entitlement", get(account_entitlement))
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
    let authorization = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .ok_or(AuthError::MissingAuthorization)?;
    let bearer_token = authorization
        .strip_prefix("Bearer ")
        .filter(|token| !token.trim().is_empty())
        .ok_or(AuthError::InvalidAuthorization)?;
    let account = state.verifier.verify(bearer_token).await?;
    request.extensions_mut().insert(account);
    Ok(next.run(request).await)
}

async fn sync_pull(
    State(state): State<SyncHttpState>,
    Extension(account): Extension<AuthenticatedAccount>,
    Json(request): Json<SyncPullRequest>,
) -> Result<Json<SyncPullResponse>, ApiError> {
    state
        .store
        .pull(&account.account_id, &request)
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

async fn account_entitlement(
    State(state): State<SyncHttpState>,
    Extension(account): Extension<AuthenticatedAccount>,
) -> Result<Json<Entitlement>, ApiError> {
    state
        .billing
        .entitlement(&account.account_id)
        .map(Json)
        .map_err(ApiError::from)
}

#[derive(Debug)]
enum ApiError {
    Sync(SyncStoreError),
    Billing(BillingStoreError),
}

impl From<SyncStoreError> for ApiError {
    fn from(error: SyncStoreError) -> Self {
        Self::Sync(error)
    }
}

impl From<BillingStoreError> for ApiError {
    fn from(error: BillingStoreError) -> Self {
        Self::Billing(error)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::Sync(SyncStoreError::SpaceAccessDenied) => (
                axum::http::StatusCode::FORBIDDEN,
                "space access denied".to_owned(),
            ),
            Self::Sync(error) => (axum::http::StatusCode::BAD_REQUEST, error.to_string()),
            Self::Billing(BillingStoreError::LockPoisoned) => (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "billing store unavailable".to_owned(),
            ),
            Self::Billing(error) => (axum::http::StatusCode::BAD_REQUEST, error.to_string()),
        };
        (status, message).into_response()
    }
}
