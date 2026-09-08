//! Authenticated HTTP/SSE transport for the sync engine.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Extension, Path, Query, State};
use axum::http::{HeaderMap, Request, StatusCode, header::CACHE_CONTROL};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::Stream;
use serde::{Deserialize, Serialize};
use task_core::billing::SyncAccessMode;
use task_core::sync::{
    SYNC_PROTOCOL_VERSION, SyncMetadataRequest, SyncMetadataResponse, SyncPullRequest,
    SyncPullResponse, SyncPushRequest, SyncPushResponse, SyncReconcileRequest,
    SyncReconcileResponse,
};
use tokio_stream::StreamExt;
use url::Url;
use uuid::Uuid;

use crate::dodo::{BillingInterval, DodoClient, DodoError};
use crate::metrics::Metrics;
use crate::postgres::{PostgresBillingStore, PostgresStoreError, PostgresSyncStore};

// EncodedUpdate values are URL-safe base64 in JSON, so the HTTP envelope is
// larger than the decoded 2 MiB update limit. Keep a separate bounded envelope
// limit that still admits a maximum update plus state vector and JSON overhead.
const MAX_SYNC_REQUEST_BODY_BYTES: usize = 4 * 1024 * 1024;

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
    #[error("cross-site cookie mutation rejected")]
    CsrfRejected,
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let code = match &self {
            Self::MissingAuthorization | Self::InvalidAuthorization => "SESSION_REQUIRED",
            Self::VerificationFailed => "SESSION_EXPIRED",
            Self::CsrfRejected => "CSRF_REJECTED",
        };
        let status = match &self {
            Self::MissingAuthorization | Self::InvalidAuthorization => {
                axum::http::StatusCode::UNAUTHORIZED
            }
            Self::VerificationFailed => axum::http::StatusCode::UNAUTHORIZED,
            Self::CsrfRejected => axum::http::StatusCode::FORBIDDEN,
        };
        let mut response = (status, self.to_string()).into_response();
        response.headers_mut().insert(
            axum::http::HeaderName::from_static("x-task-space-error-code"),
            axum::http::HeaderValue::from_static(code),
        );
        response
    }
}

/// Provider-neutral session verification contract. A WorkOS/JWT implementation
/// can be supplied later without changing the sync handlers.
#[async_trait]
pub trait SessionVerifier: Send + Sync {
    async fn verify(&self, bearer_token: &str) -> Result<AuthenticatedAccount, AuthError>;
}

#[derive(Clone, Debug)]
pub struct RequestId(pub String);

/// Attach a bounded correlation id to every response. Reverse proxies may
/// supply one, but malformed or oversized values are replaced so logs cannot
/// be used to inject arbitrary headers or unbounded label cardinality.
pub async fn add_request_id(mut request: Request<Body>, next: Next) -> Response {
    let request_id = request
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 128
                && value
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || "-_".contains(character))
        })
        .map(str::to_owned)
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    request
        .extensions_mut()
        .insert(RequestId(request_id.clone()));
    let mut response = next.run(request).await;
    if let Ok(value) = axum::http::HeaderValue::from_str(&request_id) {
        response.headers_mut().insert("x-request-id", value);
    }
    response
}

#[derive(Clone)]
pub struct SyncHttpState {
    pub store: PostgresSyncStore,
    pub billing: PostgresBillingStore,
    pub payments: DodoClient,
    pub verifier: Arc<dyn SessionVerifier>,
    pub session_cookie_name: String,
    /// Exact browser origins allowed to send cookie-authenticated mutations.
    /// Keeping this list server-owned prevents a permissive CORS setting from
    /// accidentally becoming a CSRF bypass.
    pub allowed_origins: Arc<Vec<String>>,
    pub rate_limiter: RequestRateLimiter,
    pub metrics: Metrics,
}

#[derive(Clone, Default)]
pub struct RequestRateLimiter {
    windows: Arc<Mutex<HashMap<String, Vec<Instant>>>>,
}

const MAX_RATE_LIMIT_KEYS: usize = 8_192;

impl RequestRateLimiter {
    fn allow(&self, key: String, limit: usize, window: Duration) -> bool {
        let Ok(mut windows) = self.windows.lock() else {
            // Fail closed if the process-local limiter cannot be inspected.
            return false;
        };
        let now = Instant::now();
        if !windows.contains_key(&key) && windows.len() >= MAX_RATE_LIMIT_KEYS {
            windows.retain(|_, entries| {
                entries
                    .last()
                    .is_some_and(|created| now.duration_since(*created) < window)
            });
            if windows.len() >= MAX_RATE_LIMIT_KEYS {
                return false;
            }
        }
        let entries = windows.entry(key).or_default();
        entries.retain(|created| now.duration_since(*created) < window);
        if entries.len() >= limit {
            return false;
        }
        entries.push(now);
        true
    }
}

/// Routes for the authenticated sync surface. The verifier and store are
/// carried by one router state so Axum can enforce authentication before any
/// sync handler runs.
pub fn protected_sync_router(state: SyncHttpState) -> Router {
    Router::new()
        .route("/sync/pull", post(sync_pull))
        .route("/sync/push", post(sync_push))
        .route("/sync/v2/reconcile", post(sync_reconcile))
        .route("/sync/spaces", get(list_spaces))
        .route("/sync/spaces/{space_id}", post(register_space))
        .route(
            "/sync/spaces/{space_id}/metadata",
            post(update_space_metadata),
        )
        .route("/sync/events", get(sync_events))
        .route("/account/entitlement", get(account_entitlement))
        .route("/billing/checkout", post(create_checkout))
        .route("/billing/portal", post(create_billing_portal))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_session,
        ))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            observe_request,
        ))
        .layer(DefaultBodyLimit::max(MAX_SYNC_REQUEST_BODY_BYTES))
        .with_state(state)
}

#[derive(Clone)]
struct MetricsState {
    metrics: Metrics,
    token: Option<String>,
}

pub fn metrics_router(metrics: Metrics, token: Option<String>) -> Router {
    Router::new()
        .route("/internal/metrics", get(metrics_endpoint))
        .with_state(MetricsState { metrics, token })
}

async fn metrics_endpoint(
    State(state): State<MetricsState>,
    headers: HeaderMap,
) -> Result<Response, StatusCode> {
    let configured = state
        .token
        .as_deref()
        .filter(|token| !token.trim().is_empty())
        .ok_or(StatusCode::NOT_FOUND)?;
    let supplied = headers
        .get("x-metrics-token")
        .and_then(|value| value.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    if supplied != configured {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let mut response = state.metrics.render_prometheus().into_response();
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("text/plain; version=0.0.4"),
    );
    response.headers_mut().insert(
        CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-store"),
    );
    Ok(response)
}

async fn observe_request(
    State(state): State<SyncHttpState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let route = metric_route(request.uri().path());
    state
        .metrics
        .inc_labeled("task_space_http_requests_total", &[("route", route)]);
    let response = next.run(request).await;
    let status = response.status().as_u16().to_string();
    state.metrics.inc_labeled(
        "task_space_http_responses_total",
        &[("route", route), ("status", &status)],
    );
    response
}

fn metric_route(path: &str) -> &'static str {
    match path {
        "/sync/pull" => "sync_pull",
        "/sync/push" => "sync_push",
        "/sync/v2/reconcile" => "sync_reconcile",
        "/sync/spaces" => "sync_spaces",
        "/sync/events" => "sync_events",
        "/account/entitlement" => "account_entitlement",
        "/billing/checkout" => "billing_checkout",
        "/billing/portal" => "billing_portal",
        _ if path.starts_with("/sync/spaces/") => "sync_space",
        _ => "other",
    }
}

fn sync_error_metric_kind(error: &PostgresStoreError) -> &'static str {
    match error {
        PostgresStoreError::PayloadTooLarge => "payload_too_large",
        PostgresStoreError::MutationIdReused => "mutation_conflict",
        PostgresStoreError::InvalidUpdate(_) | PostgresStoreError::InvalidStateVector(_) => {
            "invalid_payload"
        }
        PostgresStoreError::SpaceAccessDenied => "space_denied",
        PostgresStoreError::Database(_) => "database",
        _ => "other",
    }
}

async fn require_session(
    State(state): State<SyncHttpState>,
    mut request: Request<Body>,
    next: Next,
) -> Result<Response, AuthError> {
    let has_bearer = request
        .headers()
        .contains_key(axum::http::header::AUTHORIZATION);
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
    if !has_bearer
        && !matches!(
            *request.method(),
            axum::http::Method::GET | axum::http::Method::HEAD | axum::http::Method::OPTIONS
        )
    {
        let fetch_site = request
            .headers()
            .get("sec-fetch-site")
            .and_then(|value| value.to_str().ok());
        if !same_site_or_allowed_origin(
            request.headers(),
            fetch_site,
            state.allowed_origins.as_slice(),
        ) {
            return Err(AuthError::CsrfRejected);
        }
    }
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

fn same_site_or_allowed_origin(
    headers: &HeaderMap,
    fetch_site: Option<&str>,
    allowed_origins: &[String],
) -> bool {
    if fetch_site.is_some_and(|value| value.eq_ignore_ascii_case("cross-site")) {
        return false;
    }
    if fetch_site.is_some_and(|value| value.eq_ignore_ascii_case("same-origin")) {
        return true;
    }
    let source = headers
        .get("origin")
        .or_else(|| headers.get("referer"))
        .and_then(|value| value.to_str().ok());
    let Some(source) = source else {
        return false;
    };
    let Ok(url) = Url::parse(source) else {
        return false;
    };
    if !matches!(url.scheme(), "http" | "https") {
        return false;
    }
    if !url.username().is_empty() || url.password().is_some() {
        return false;
    }
    let Some(source_host) = url.host_str() else {
        return false;
    };
    let request_host = headers
        .get(axum::http::header::HOST)
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
        return true;
    }
    let request_host_name = Url::parse(&format!("http://{request_host}"))
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned));
    let local_dev = matches!(source_host, "localhost" | "127.0.0.1" | "::1")
        && request_host_name
            .as_deref()
            .is_some_and(|host| matches!(host, "localhost" | "127.0.0.1" | "::1"));
    // Production origins must be explicitly configured. The only implicit
    // exception is the loopback development set, where the UI and API use
    // different localhost ports.
    local_dev
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
    if !state.rate_limiter.allow(
        format!("sync-pull:{}", account.account_id),
        600,
        Duration::from_secs(60),
    ) {
        return Err(ApiError::RateLimited);
    }
    require_sync_entitlement(&state.billing, &account.account_id).await?;
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
    if !state.rate_limiter.allow(
        format!("sync-push:{}", account.account_id),
        600,
        Duration::from_secs(60),
    ) {
        return Err(ApiError::RateLimited);
    }
    require_sync_entitlement(&state.billing, &account.account_id).await?;
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

async fn sync_reconcile(
    State(state): State<SyncHttpState>,
    Extension(account): Extension<AuthenticatedAccount>,
    Json(request): Json<SyncReconcileRequest>,
) -> Result<Json<SyncReconcileResponse>, ApiError> {
    state
        .metrics
        .inc("task_space_sync_reconcile_attempts_total");
    if !state.rate_limiter.allow(
        format!("sync-reconcile:{}", account.account_id),
        600,
        Duration::from_secs(60),
    ) {
        return Err(ApiError::RateLimited);
    }
    let entitlement = require_sync_entitlement(&state.billing, &account.account_id).await?;
    let mut response = match state.store.reconcile(&account.account_id, &request).await {
        Ok(response) => {
            state.metrics.inc("task_space_sync_reconcile_success_total");
            response
        }
        Err(error) => {
            state.metrics.inc_labeled(
                "task_space_sync_reconcile_failures_total",
                &[("kind", sync_error_metric_kind(&error))],
            );
            return Err(ApiError::from(error));
        }
    };
    response.entitlement_version = entitlement.version;
    Ok(Json(response))
}

async fn sync_events(
    State(state): State<SyncHttpState>,
    Extension(account): Extension<AuthenticatedAccount>,
    headers: HeaderMap,
    Query(query): Query<SyncCursorQuery>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    if !state.rate_limiter.allow(
        format!("sync-events:{}", account.account_id),
        60,
        Duration::from_secs(60),
    ) {
        return Err(ApiError::RateLimited);
    }
    require_sync_entitlement(&state.billing, &account.account_id).await?;
    let after_event_id = headers
        .get(axum::http::HeaderName::from_static("last-event-id"))
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .or(query.after_event_id)
        .unwrap_or_default();
    // Subscribe before reading the durable replay so a commit between those
    // two operations is present in either the replay or the live buffer.
    let live_receiver = state.store.subscribe();
    let (replay, reset_required) = match state
        .store
        .events_since(&account.account_id, after_event_id)
        .await
    {
        Ok(replay) => (replay, false),
        Err(PostgresStoreError::EventCursorRequiresReset) => {
            state.metrics.inc("task_space_sync_cursor_resets_total");
            (Vec::new(), true)
        }
        Err(error) => return Err(ApiError::from(error)),
    };
    let replay_events: Vec<Result<Event, Infallible>> = if reset_required {
        vec![Ok(Event::default()
            .event("sync-reset")
            .data("cursor expired; full reconciliation required"))]
    } else {
        replay
            .into_iter()
            .filter_map(|event| {
                Event::default()
                    .id(event.event_id.to_string())
                    .event("space-update")
                    .json_data(event)
                    .ok()
                    .map(Ok)
            })
            .collect()
    };
    let replay_stream = tokio_stream::iter(replay_events);
    let live_stream =
        tokio_stream::wrappers::BroadcastStream::new(live_receiver).filter_map(move |message| {
            match message {
                Ok(delivery)
                    if delivery.account_id == account.account_id
                        && delivery.event.event_id > after_event_id =>
                {
                    Event::default()
                        .id(delivery.event.event_id.to_string())
                        .event("space-update")
                        .json_data(delivery.event)
                        .ok()
                        .map(Ok)
                }
                _ => None,
            }
        });
    let stream = replay_stream.chain(live_stream);
    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(30))
            .text("heartbeat"),
    ))
}

#[derive(Debug, Deserialize)]
struct SyncCursorQuery {
    after_event_id: Option<u64>,
}

async fn list_spaces(
    State(state): State<SyncHttpState>,
    Extension(account): Extension<AuthenticatedAccount>,
) -> Result<Json<Vec<task_core::Space>>, ApiError> {
    if !state.rate_limiter.allow(
        format!("sync-spaces:{}", account.account_id),
        120,
        Duration::from_secs(60),
    ) {
        return Err(ApiError::RateLimited);
    }
    require_sync_entitlement(&state.billing, &account.account_id).await?;
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
    if !state.rate_limiter.allow(
        format!("sync-register:{}", account.account_id),
        120,
        Duration::from_secs(60),
    ) {
        return Err(ApiError::RateLimited);
    }
    let entitlement = require_sync_entitlement(&state.billing, &account.account_id).await?;
    if request.name.trim().is_empty() || request.name.trim().len() > 48 {
        return Err(ApiError::Store(PostgresStoreError::InvalidInput(
            "space name must be between 1 and 48 characters".to_owned(),
        )));
    }
    state
        .store
        .register_space(
            &account.account_id,
            space_id,
            request.name.trim(),
            entitlement.max_spaces,
        )
        .await
        .map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn update_space_metadata(
    State(state): State<SyncHttpState>,
    Extension(account): Extension<AuthenticatedAccount>,
    Path(_space_id): Path<u64>,
    Json(mut request): Json<SyncMetadataRequest>,
) -> Result<Json<SyncMetadataResponse>, ApiError> {
    if !state.rate_limiter.allow(
        format!("sync-metadata:{}", account.account_id),
        120,
        Duration::from_secs(60),
    ) {
        return Err(ApiError::RateLimited);
    }
    require_sync_entitlement(&state.billing, &account.account_id).await?;
    // The path is authoritative; a body cannot redirect a metadata mutation
    // to another account or resource.
    request.space_id = _space_id;
    state
        .store
        .apply_metadata(&account.account_id, &request)
        .await
        .map(Json)
        .map_err(ApiError::from)
}

async fn require_sync_entitlement(
    billing: &PostgresBillingStore,
    account_id: &str,
) -> Result<task_core::billing::Entitlement, ApiError> {
    let entitlement = billing
        .entitlement(account_id)
        .await
        .map_err(ApiError::from)?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
    if entitlement.can_sync_at(now) {
        Ok(entitlement)
    } else if matches!(entitlement.access_mode, SyncAccessMode::GraceReadWrite) {
        Err(ApiError::Store(PostgresStoreError::SyncPaymentPaused))
    } else {
        Err(ApiError::Store(PostgresStoreError::SyncNotEntitled))
    }
}

async fn account_entitlement(
    State(state): State<SyncHttpState>,
    Extension(account): Extension<AuthenticatedAccount>,
) -> Result<Response, ApiError> {
    if !state.rate_limiter.allow(
        format!("account-entitlement:{}", account.account_id),
        120,
        Duration::from_secs(60),
    ) {
        return Err(ApiError::RateLimited);
    }
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
    if !state.rate_limiter.allow(
        format!("checkout:{}", account.account_id),
        10,
        Duration::from_secs(60),
    ) {
        return Err(ApiError::RateLimited);
    }
    let interval_name = match request.interval {
        BillingInterval::Month => "month",
        BillingInterval::Year => "year",
    };
    let (idempotency_key, existing_url) = state
        .billing
        .reserve_checkout(&account.account_id, interval_name)
        .await
        .map_err(ApiError::from)?;
    if let Some(checkout_url) = existing_url {
        return Ok(Json(CheckoutResponse { checkout_url }));
    }
    let checkout_url = state
        .payments
        .create_checkout(&account.account_id, request.interval, &idempotency_key)
        .await
        .map_err(|error| {
            eprintln!("Dodo checkout failed for an authenticated account: {error}");
            ApiError::from(error)
        })?;
    state
        .billing
        .complete_checkout(&idempotency_key, &checkout_url)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(CheckoutResponse { checkout_url }))
}

async fn create_billing_portal(
    State(state): State<SyncHttpState>,
    Extension(account): Extension<AuthenticatedAccount>,
) -> Result<Json<BillingPortalResponse>, ApiError> {
    if !state.rate_limiter.allow(
        format!("billing-portal:{}", account.account_id),
        10,
        Duration::from_secs(60),
    ) {
        return Err(ApiError::RateLimited);
    }
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
    RateLimited,
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
        let code = match &self {
            Self::Store(PostgresStoreError::SpaceAccessDenied) => "SPACE_ACCESS_DENIED",
            Self::Store(PostgresStoreError::SyncNotEntitled) => "SYNC_NOT_ENTITLED",
            Self::Store(PostgresStoreError::SyncPaymentPaused) => "SYNC_PAYMENT_PAUSED",
            Self::Store(PostgresStoreError::SpaceLimitReached) => "SPACE_LIMIT_REACHED",
            Self::Store(PostgresStoreError::MetadataVersionConflict) => "METADATA_VERSION_CONFLICT",
            Self::Store(PostgresStoreError::MetadataOperationIdReused) => {
                "METADATA_OPERATION_ID_REUSED"
            }
            Self::Store(PostgresStoreError::EventCursorRequiresReset) => {
                "SYNC_CURSOR_RESET_REQUIRED"
            }
            Self::Store(PostgresStoreError::MutationIdReused) => "MUTATION_ID_REUSED",
            Self::Store(PostgresStoreError::InvalidUpdate(_)) => "INVALID_UPDATE",
            Self::Store(PostgresStoreError::InvalidStateVector(_)) => "INVALID_STATE_VECTOR",
            Self::Store(PostgresStoreError::InvalidInput(_)) => "INVALID_INPUT",
            Self::Store(PostgresStoreError::PayloadTooLarge) => "SYNC_PAYLOAD_TOO_LARGE",
            Self::Store(PostgresStoreError::UnsupportedSyncProtocol(_))
            | Self::Store(PostgresStoreError::UnsupportedSyncReconcileProtocol(_))
            | Self::Store(PostgresStoreError::UnsupportedBillingProtocol(_)) => {
                "UNSUPPORTED_PROTOCOL"
            }
            Self::Store(PostgresStoreError::Database(_)) => "DATABASE_UNAVAILABLE",
            Self::Dodo(_) => "PAYMENT_PROVIDER_ERROR",
            Self::BillingPortalUnavailable => "BILLING_PORTAL_UNAVAILABLE",
            Self::RateLimited => "RATE_LIMITED",
            _ => "API_ERROR",
        };
        let rate_limited = matches!(&self, Self::RateLimited);
        let (status, message) = match self {
            Self::Store(PostgresStoreError::SpaceAccessDenied) => (
                axum::http::StatusCode::FORBIDDEN,
                "space access denied".to_owned(),
            ),
            Self::Store(PostgresStoreError::SyncNotEntitled) => (
                axum::http::StatusCode::FORBIDDEN,
                "sync is not enabled for this account".to_owned(),
            ),
            Self::Store(PostgresStoreError::SyncPaymentPaused) => (
                axum::http::StatusCode::FORBIDDEN,
                "sync is paused pending payment recovery".to_owned(),
            ),
            Self::Store(PostgresStoreError::SpaceLimitReached) => (
                axum::http::StatusCode::CONFLICT,
                "sync space limit reached".to_owned(),
            ),
            Self::Store(PostgresStoreError::MetadataVersionConflict) => (
                axum::http::StatusCode::CONFLICT,
                "space metadata changed elsewhere".to_owned(),
            ),
            Self::Store(PostgresStoreError::MetadataOperationIdReused) => (
                axum::http::StatusCode::CONFLICT,
                "metadata operation id was reused with a different request".to_owned(),
            ),
            Self::Store(PostgresStoreError::EventCursorRequiresReset) => (
                axum::http::StatusCode::CONFLICT,
                "sync cursor expired; reset required".to_owned(),
            ),
            Self::Store(PostgresStoreError::Database(_)) => (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "database unavailable".to_owned(),
            ),
            Self::Store(PostgresStoreError::PayloadTooLarge) => (
                axum::http::StatusCode::PAYLOAD_TOO_LARGE,
                "sync payload is too large".to_owned(),
            ),
            Self::Store(PostgresStoreError::InvalidUpdate(_))
            | Self::Store(PostgresStoreError::InvalidStateVector(_)) => (
                axum::http::StatusCode::UNPROCESSABLE_ENTITY,
                "sync payload is invalid".to_owned(),
            ),
            Self::Store(PostgresStoreError::InvalidInput(_)) => (
                axum::http::StatusCode::BAD_REQUEST,
                "request input is invalid".to_owned(),
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
            Self::RateLimited => (
                axum::http::StatusCode::TOO_MANY_REQUESTS,
                "too many requests; please retry shortly".to_owned(),
            ),
        };
        let mut response = (status, message).into_response();
        response.headers_mut().insert(
            axum::http::HeaderName::from_static("x-task-space-error-code"),
            axum::http::HeaderValue::from_static(code),
        );
        if rate_limited {
            response.headers_mut().insert(
                axum::http::header::RETRY_AFTER,
                axum::http::HeaderValue::from_static("10"),
            );
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use axum::http::header::{HOST, ORIGIN};

    #[test]
    fn csrf_accepts_an_explicit_production_origin() {
        let mut headers = HeaderMap::new();
        headers.insert(ORIGIN, HeaderValue::from_static("https://app.example.com"));
        headers.insert(HOST, HeaderValue::from_static("api.example.com"));
        let allowed = vec!["https://app.example.com".to_owned()];

        assert!(same_site_or_allowed_origin(&headers, None, &allowed));
    }

    #[test]
    fn csrf_rejects_cross_site_fetch_even_if_origin_is_configured() {
        let mut headers = HeaderMap::new();
        headers.insert(ORIGIN, HeaderValue::from_static("https://app.example.com"));
        headers.insert(HOST, HeaderValue::from_static("api.example.com"));
        headers.insert(
            axum::http::HeaderName::from_static("sec-fetch-site"),
            HeaderValue::from_static("cross-site"),
        );
        let allowed = vec!["https://app.example.com".to_owned()];

        assert!(!same_site_or_allowed_origin(
            &headers,
            Some("cross-site"),
            &allowed
        ));
    }

    #[test]
    fn csrf_does_not_treat_same_site_sibling_origins_as_trusted() {
        let mut headers = HeaderMap::new();
        headers.insert(ORIGIN, HeaderValue::from_static("https://evil.example.com"));
        headers.insert(HOST, HeaderValue::from_static("api.example.com"));
        headers.insert(
            axum::http::HeaderName::from_static("sec-fetch-site"),
            HeaderValue::from_static("same-site"),
        );
        assert!(!same_site_or_allowed_origin(
            &headers,
            Some("same-site"),
            &["https://app.example.com".to_owned()]
        ));
    }

    #[test]
    fn csrf_does_not_trust_localhost_origin_for_a_production_host() {
        let mut headers = HeaderMap::new();
        headers.insert(ORIGIN, HeaderValue::from_static("http://localhost:8080"));
        headers.insert(HOST, HeaderValue::from_static("api.example.com"));
        assert!(!same_site_or_allowed_origin(&headers, None, &[]));
    }

    #[test]
    fn csrf_rejects_origin_credentials_even_when_host_is_allowed() {
        let mut headers = HeaderMap::new();
        headers.insert(
            ORIGIN,
            HeaderValue::from_static("https://user:pass@app.example.com"),
        );
        headers.insert(HOST, HeaderValue::from_static("api.example.com"));
        assert!(!same_site_or_allowed_origin(
            &headers,
            None,
            &["https://app.example.com".to_owned()]
        ));
    }

    #[test]
    fn rate_limiter_separates_account_keys() {
        let limiter = RequestRateLimiter::default();
        assert!(limiter.allow("account-a".to_owned(), 1, Duration::from_secs(60)));
        assert!(!limiter.allow("account-a".to_owned(), 1, Duration::from_secs(60)));
        assert!(limiter.allow("account-b".to_owned(), 1, Duration::from_secs(60)));
    }

    #[tokio::test]
    async fn metrics_endpoint_requires_operator_token() {
        let metrics = Metrics::default();
        metrics.inc_labeled(
            "task_space_http_responses_total",
            &[("route", "sync_reconcile"), ("status", "200")],
        );
        let state = MetricsState {
            metrics,
            token: Some("operator-secret".to_owned()),
        };

        assert_eq!(
            metrics_endpoint(State(state.clone()), HeaderMap::new())
                .await
                .unwrap_err(),
            StatusCode::UNAUTHORIZED
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-metrics-token",
            HeaderValue::from_static("operator-secret"),
        );
        let response = metrics_endpoint(State(state), headers)
            .await
            .expect("valid operator token should scrape metrics");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some("no-store")
        );
    }
}
