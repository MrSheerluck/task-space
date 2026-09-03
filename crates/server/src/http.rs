//! Authenticated HTTP/SSE transport for the sync engine.

use std::convert::Infallible;
use std::time::Duration;

use axum::extract::{Extension, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::Stream;
use task_core::sync::{
    SYNC_PROTOCOL_VERSION, SyncPullRequest, SyncPullResponse, SyncPushRequest, SyncPushResponse,
};
use tokio_stream::StreamExt;

use crate::{SyncStore, SyncStoreError};

/// The identity supplied by authentication middleware. Sync handlers never
/// accept an account id from a request body or query parameter.
#[derive(Clone, Debug)]
pub struct AuthenticatedAccount(pub String);

#[derive(Clone)]
pub struct SyncHttpState {
    pub store: SyncStore,
}

/// Routes for the authenticated sync surface. The caller must install the
/// authentication layer that provides `Extension<AuthenticatedAccount>` before
/// mounting this router.
pub fn protected_sync_router(state: SyncHttpState) -> Router {
    Router::new()
        .route("/sync/pull", post(sync_pull))
        .route("/sync/push", post(sync_push))
        .route("/sync/events", get(sync_events))
        .with_state(state)
}

async fn sync_pull(
    State(state): State<SyncHttpState>,
    Extension(account): Extension<AuthenticatedAccount>,
    Json(request): Json<SyncPullRequest>,
) -> Result<Json<SyncPullResponse>, ApiError> {
    state
        .store
        .pull(&account.0, &request)
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
        .push(&account.0, &request)
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
            Ok(delivery) if delivery.account_id == account.0 => Event::default()
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

#[derive(Debug)]
enum ApiError {
    Sync(SyncStoreError),
}

impl From<SyncStoreError> for ApiError {
    fn from(error: SyncStoreError) -> Self {
        Self::Sync(error)
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
        };
        (status, message).into_response()
    }
}
