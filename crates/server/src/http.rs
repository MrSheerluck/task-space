//! Authenticated HTTP/SSE transport for the sync engine.

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Extension, Path, Query, State};
use axum::http::header::AUTHORIZATION;
use axum::http::{HeaderMap, Request, StatusCode, header::CACHE_CONTROL};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use task_core::billing::SyncAccessMode;
use task_core::sync::{
    SYNC_DOCUMENT_SCHEMA_VERSION, SYNC_PROTOCOL_VERSION, SyncMetadataRequest, SyncMetadataResponse,
    SyncPullRequest, SyncPullResponse, SyncPushRequest, SyncPushResponse, SyncReconcileRequest,
    SyncReconcileResponse,
};
use task_core::{BoardData, EntityId};
use tokio_stream::StreamExt;
use url::Url;
use uuid::Uuid;

use crate::dodo::{BillingInterval, DodoClient, DodoError};
use crate::metrics::Metrics;
use crate::postgres::{
    CrudSpaceRecord, PostgresBillingStore, PostgresStoreError, PostgresSyncStore,
};

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
        response.headers_mut().insert(
            CACHE_CONTROL,
            axum::http::HeaderValue::from_static("no-store"),
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

    /// Apply both an account-scoped and a client-IP-scoped budget. The IP
    /// component is deliberately normalized to a parsed address (or the
    /// single bounded `unknown` bucket) so attacker-controlled headers cannot
    /// create unbounded limiter keys.
    pub(crate) fn allow_account_and_ip(
        &self,
        headers: &HeaderMap,
        route: &str,
        account_id: &str,
        account_limit: usize,
        ip_limit: usize,
        window: Duration,
    ) -> bool {
        self.allow(
            format!("{route}:account:{account_id}"),
            account_limit,
            window,
        ) && self.allow(
            format!("{route}:ip:{}", client_ip(headers)),
            ip_limit,
            window,
        )
    }

    pub(crate) fn allow_ip(
        &self,
        headers: &HeaderMap,
        route: &str,
        limit: usize,
        window: Duration,
    ) -> bool {
        self.allow(format!("{route}:ip:{}", client_ip(headers)), limit, window)
    }
}

fn client_ip(headers: &HeaderMap) -> String {
    for header_name in ["x-real-ip", "x-forwarded-for"] {
        let Some(value) = headers
            .get(header_name)
            .and_then(|value| value.to_str().ok())
        else {
            continue;
        };
        for candidate in value.split(',').map(str::trim) {
            if let Ok(address) = candidate.parse::<IpAddr>() {
                return address.to_string();
            }
        }
    }
    "unknown".to_owned()
}

/// Routes for the authenticated sync surface. The verifier and store are
/// carried by one router state so Axum can enforce authentication before any
/// sync handler runs.
pub fn protected_sync_router(state: SyncHttpState) -> Router {
    Router::new()
        .route("/sync/pull", post(sync_pull))
        .route("/sync/push", post(sync_push))
        .route("/sync/v2/reconcile", post(sync_reconcile))
        .route(
            "/sync/v2/spaces/{space_id}/reconcile",
            post(sync_reconcile_scoped),
        )
        .route("/sync/spaces", get(list_spaces))
        .route("/sync/spaces/{space_id}", post(register_space))
        .route(
            "/sync/spaces/{space_id}/metadata",
            post(update_space_metadata),
        )
        .route("/sync/events", get(sync_events))
        .route("/auth/session", get(auth_session))
        .route("/auth/diagnostics", get(auth_diagnostics))
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

/// Authenticated HTTP CRUD surface used by the simplified application. The
/// legacy sync router remains available to old acceptance tests during the
/// pre-production cutover, but the running server mounts this router instead.
pub fn protected_api_router(state: SyncHttpState) -> Router {
    Router::new()
        .route("/api/spaces", get(crud_list_spaces).post(crud_create_space))
        .route(
            "/api/spaces/{space_id}",
            patch(crud_update_space).delete(crud_delete_space),
        )
        .route(
            "/api/spaces/{space_id}/board",
            get(crud_get_board).put(crud_put_board),
        )
        .route("/auth/session", get(auth_session))
        .route("/auth/diagnostics", get(auth_diagnostics))
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

#[derive(Debug, Deserialize)]
struct CrudCreateSpaceRequest {
    name: String,
}

#[derive(Debug, Deserialize, Default)]
struct CrudUpdateSpaceRequest {
    name: Option<String>,
    archived: Option<bool>,
    deleted: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct CrudPutBoardRequest {
    board: BoardData,
}

#[derive(Debug, Serialize)]
struct CrudBoardResponse {
    space_id: EntityId,
    version: u64,
    board: BoardData,
}

async fn crud_list_spaces(
    State(state): State<SyncHttpState>,
    Extension(account): Extension<AuthenticatedAccount>,
    headers: HeaderMap,
) -> Result<Json<Vec<CrudSpaceRecord>>, ApiError> {
    if !state.rate_limiter.allow_account_and_ip(
        &headers,
        "crud-spaces-list",
        &account.account_id,
        120,
        600,
        Duration::from_secs(60),
    ) {
        return Err(ApiError::RateLimited);
    }
    require_sync_entitlement(&state.billing, &account.account_id).await?;
    state
        .store
        .crud_list_spaces(&account.account_id)
        .await
        .map(Json)
        .map_err(ApiError::from)
}

async fn crud_create_space(
    State(state): State<SyncHttpState>,
    Extension(account): Extension<AuthenticatedAccount>,
    headers: HeaderMap,
    Json(request): Json<CrudCreateSpaceRequest>,
) -> Result<(StatusCode, Json<CrudSpaceRecord>), ApiError> {
    if !state.rate_limiter.allow_account_and_ip(
        &headers,
        "crud-space-create",
        &account.account_id,
        120,
        600,
        Duration::from_secs(60),
    ) {
        return Err(ApiError::RateLimited);
    }
    let entitlement = require_sync_entitlement(&state.billing, &account.account_id).await?;
    let space = state
        .store
        .crud_create_space(&account.account_id, &request.name, entitlement.max_spaces)
        .await
        .map_err(ApiError::from)?;
    Ok((StatusCode::CREATED, Json(space)))
}

async fn crud_update_space(
    State(state): State<SyncHttpState>,
    Extension(account): Extension<AuthenticatedAccount>,
    Path(space_id): Path<u64>,
    headers: HeaderMap,
    Json(request): Json<CrudUpdateSpaceRequest>,
) -> Result<Json<CrudSpaceRecord>, ApiError> {
    if !state.rate_limiter.allow_account_and_ip(
        &headers,
        "crud-space-update",
        &account.account_id,
        240,
        1_200,
        Duration::from_secs(60),
    ) {
        return Err(ApiError::RateLimited);
    }
    require_sync_entitlement(&state.billing, &account.account_id).await?;
    state
        .store
        .crud_update_space(
            &account.account_id,
            space_id,
            request.name.as_deref(),
            request.archived,
            request.deleted,
        )
        .await
        .map(Json)
        .map_err(ApiError::from)
}

async fn crud_delete_space(
    State(state): State<SyncHttpState>,
    Extension(account): Extension<AuthenticatedAccount>,
    Path(space_id): Path<u64>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    if !state.rate_limiter.allow_account_and_ip(
        &headers,
        "crud-space-delete",
        &account.account_id,
        120,
        600,
        Duration::from_secs(60),
    ) {
        return Err(ApiError::RateLimited);
    }
    require_sync_entitlement(&state.billing, &account.account_id).await?;
    state
        .store
        .crud_update_space(&account.account_id, space_id, None, Some(true), Some(true))
        .await
        .map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn crud_get_board(
    State(state): State<SyncHttpState>,
    Extension(account): Extension<AuthenticatedAccount>,
    Path(space_id): Path<u64>,
    headers: HeaderMap,
) -> Result<Json<CrudBoardResponse>, ApiError> {
    if !state.rate_limiter.allow_account_and_ip(
        &headers,
        "crud-board-get",
        &account.account_id,
        600,
        3_000,
        Duration::from_secs(60),
    ) {
        return Err(ApiError::RateLimited);
    }
    require_sync_entitlement(&state.billing, &account.account_id).await?;
    let (version, board) = state
        .store
        .crud_get_board(&account.account_id, space_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(CrudBoardResponse {
        space_id,
        version,
        board,
    }))
}

async fn crud_put_board(
    State(state): State<SyncHttpState>,
    Extension(account): Extension<AuthenticatedAccount>,
    Path(space_id): Path<u64>,
    headers: HeaderMap,
    Json(request): Json<CrudPutBoardRequest>,
) -> Result<Json<CrudBoardResponse>, ApiError> {
    if !state.rate_limiter.allow_account_and_ip(
        &headers,
        "crud-board-put",
        &account.account_id,
        600,
        3_000,
        Duration::from_secs(60),
    ) {
        return Err(ApiError::RateLimited);
    }
    require_sync_entitlement(&state.billing, &account.account_id).await?;
    let version = state
        .store
        .crud_put_board(&account.account_id, space_id, &request.board)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(CrudBoardResponse {
        space_id,
        version,
        board: request.board,
    }))
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

#[derive(Clone)]
struct HealthState {
    pool: PgPool,
    metrics: Metrics,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

/// Public process and database probes for a reverse proxy or orchestrator.
/// These endpoints never expose provider, account, or database details.
pub fn health_router(pool: PgPool, metrics: Metrics) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .with_state(HealthState { pool, metrics })
}

async fn healthz() -> Response {
    health_response(StatusCode::OK, "ok")
}

async fn readyz(State(state): State<HealthState>) -> Response {
    state.metrics.inc("task_space_readiness_checks_total");
    match sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&state.pool)
        .await
    {
        Ok(1) => {
            state.metrics.inc("task_space_readiness_success_total");
            health_response(StatusCode::OK, "ready")
        }
        Ok(_) | Err(_) => {
            state.metrics.inc("task_space_readiness_failures_total");
            health_response(StatusCode::SERVICE_UNAVAILABLE, "not_ready")
        }
    }
}

fn health_response(status: StatusCode, value: &'static str) -> Response {
    let mut response = (status, Json(HealthResponse { status: value })).into_response();
    response.headers_mut().insert(
        CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-store"),
    );
    response
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
        .or_else(|| {
            headers
                .get(AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix("Bearer "))
        })
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
    let started = Instant::now();
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
    state.metrics.observe_ms_labeled(
        "task_space_http_request_duration_ms",
        &[("route", route)],
        started.elapsed(),
    );
    if let Some(error_kind) = response_error_metric_kind(&response) {
        state
            .metrics
            .inc_labeled("task_space_api_failures_total", &[("kind", error_kind)]);
    }
    response
}

fn metric_route(path: &str) -> &'static str {
    match path {
        "/sync/pull" => "sync_pull",
        "/sync/push" => "sync_push",
        "/sync/v2/reconcile" => "sync_reconcile",
        _ if path.starts_with("/sync/v2/spaces/") => "sync_reconcile",
        "/sync/spaces" => "sync_spaces",
        "/sync/events" => "sync_events",
        "/auth/session" => "auth_session",
        "/auth/diagnostics" => "auth_diagnostics",
        "/account/entitlement" => "account_entitlement",
        "/billing/checkout" => "billing_checkout",
        "/billing/portal" => "billing_portal",
        "/api/spaces" => "crud_spaces",
        _ if path.starts_with("/api/spaces/") && path.ends_with("/board") => "crud_board",
        _ if path.starts_with("/api/spaces/") => "crud_space",
        _ if path.starts_with("/sync/spaces/") => "sync_space",
        _ => "other",
    }
}

fn response_error_metric_kind(response: &Response) -> Option<&'static str> {
    let code = response
        .headers()
        .get("x-task-space-error-code")
        .and_then(|value| value.to_str().ok())?;
    Some(match code {
        "SESSION_REQUIRED" => "auth_missing",
        "SESSION_EXPIRED" => "auth_expired",
        "CSRF_REJECTED" => "csrf_rejected",
        "SYNC_NOT_ENTITLED" => "entitlement_denied",
        "SYNC_PAYMENT_PAUSED" => "entitlement_payment_paused",
        "MUTATION_ID_REUSED" => "mutation_conflict",
        "METADATA_VERSION_CONFLICT" => "metadata_conflict",
        "METADATA_CONFLICT_SUPERSEDED" => "metadata_conflict_superseded",
        "SYNC_CURSOR_RESET_REQUIRED" => "cursor_reset",
        "DATABASE_UNAVAILABLE" => "database",
        "RATE_LIMITED" => "rate_limited",
        "PAYMENT_PROVIDER_ERROR" => "payment_provider",
        _ => "other_api_error",
    })
}

fn sync_error_metric_kind(error: &PostgresStoreError) -> &'static str {
    match error {
        PostgresStoreError::PayloadTooLarge => "payload_too_large",
        PostgresStoreError::MutationIdReused => "mutation_conflict",
        PostgresStoreError::InvalidUpdate(_) | PostgresStoreError::InvalidStateVector(_) => {
            "invalid_payload"
        }
        PostgresStoreError::UnsupportedSyncDocumentSchema(_) => "client_upgrade_required",
        PostgresStoreError::SpaceAccessDenied => "space_denied",
        PostgresStoreError::MetadataConflictSuperseded => "metadata_conflict_superseded",
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
                let request_id = request
                    .extensions()
                    .get::<RequestId>()
                    .map(|request_id| request_id.0.as_str())
                    .unwrap_or("unknown");
                eprintln!(
                    "auth rejected: no {} cookie for {} request_id={}",
                    state.session_cookie_name,
                    request.uri().path(),
                    request_id,
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
            let request_id = request
                .extensions()
                .get::<RequestId>()
                .map(|request_id| request_id.0.as_str())
                .unwrap_or("unknown");
            eprintln!(
                "auth rejected: session verification failed for {} request_id={}",
                request.uri().path(),
                request_id,
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
    headers: HeaderMap,
    Json(request): Json<SyncPullRequest>,
) -> Result<Json<SyncPullResponse>, ApiError> {
    if !state.rate_limiter.allow_account_and_ip(
        &headers,
        "sync-pull",
        &account.account_id,
        600,
        3_000,
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

#[derive(Debug, Serialize)]
struct SessionResponse {
    authenticated: bool,
    user_id: String,
    account_id: String,
}

async fn auth_session(Extension(account): Extension<AuthenticatedAccount>) -> Response {
    let mut response = Json(SessionResponse {
        authenticated: true,
        user_id: account.user_id,
        account_id: account.account_id,
    })
    .into_response();
    response.headers_mut().insert(
        CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-store"),
    );
    response
}

#[derive(Debug, Serialize)]
struct AuthDiagnosticsResponse {
    authenticated: bool,
    account_id: String,
    session_present: bool,
    entitlement_version: u64,
    sync_enabled: bool,
    access_mode: SyncAccessMode,
    server_time: u64,
}

async fn auth_diagnostics(
    State(state): State<SyncHttpState>,
    Extension(account): Extension<AuthenticatedAccount>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    if !state.rate_limiter.allow_account_and_ip(
        &headers,
        "auth-diagnostics",
        &account.account_id,
        120,
        600,
        Duration::from_secs(60),
    ) {
        return Err(ApiError::RateLimited);
    }
    let entitlement = state
        .billing
        .entitlement(&account.account_id)
        .await
        .map_err(ApiError::from)?;
    let server_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
    let mut response = Json(AuthDiagnosticsResponse {
        authenticated: true,
        account_id: account.account_id,
        session_present: !account.session_id.trim().is_empty(),
        entitlement_version: entitlement.version,
        sync_enabled: entitlement.sync_enabled,
        access_mode: entitlement.access_mode,
        server_time,
    })
    .into_response();
    response.headers_mut().insert(
        CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-store"),
    );
    Ok(response)
}

async fn sync_push(
    State(state): State<SyncHttpState>,
    Extension(account): Extension<AuthenticatedAccount>,
    headers: HeaderMap,
    Json(request): Json<SyncPushRequest>,
) -> Result<Json<SyncPushResponse>, ApiError> {
    if !state.rate_limiter.allow_account_and_ip(
        &headers,
        "sync-push",
        &account.account_id,
        600,
        3_000,
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
        document_schema_version: SYNC_DOCUMENT_SCHEMA_VERSION,
        space_id: request.space_id,
        stable_space_id: request.stable_space_id.clone(),
        accepted: event.is_some(),
        event_id: event.map(|event| event.event_id),
    }))
}

async fn sync_reconcile(
    State(state): State<SyncHttpState>,
    Extension(account): Extension<AuthenticatedAccount>,
    headers: HeaderMap,
    Json(request): Json<SyncReconcileRequest>,
) -> Result<Json<SyncReconcileResponse>, ApiError> {
    let started = Instant::now();
    state
        .metrics
        .inc("task_space_sync_reconcile_attempts_total");
    if !state.rate_limiter.allow_account_and_ip(
        &headers,
        "sync-reconcile",
        &account.account_id,
        600,
        3_000,
        Duration::from_secs(60),
    ) {
        return Err(ApiError::RateLimited);
    }
    if let Ok(state_vector) = request.state_vector.to_bytes() {
        state.metrics.add(
            "task_space_sync_state_vector_bytes_total",
            state_vector.len() as u64,
        );
    }
    if let Ok(update) = request.update.to_bytes() {
        state
            .metrics
            .add("task_space_sync_update_bytes_total", update.len() as u64);
    }
    let entitlement = require_sync_entitlement(&state.billing, &account.account_id).await?;
    let mut response = match state.store.reconcile(&account.account_id, &request).await {
        Ok(response) => {
            state.metrics.inc("task_space_sync_reconcile_success_total");
            if response.event_id.is_none() {
                state
                    .metrics
                    .inc("task_space_sync_reconcile_without_new_event_total");
            }
            if let Ok(update) = response.update.to_bytes() {
                state.metrics.add(
                    "task_space_sync_response_update_bytes_total",
                    update.len() as u64,
                );
            }
            if let Ok(state_vector) = response.state_vector.to_bytes() {
                state.metrics.add(
                    "task_space_sync_response_state_vector_bytes_total",
                    state_vector.len() as u64,
                );
            }
            state.metrics.observe_ms_labeled(
                "task_space_sync_reconcile_duration_ms",
                &[("outcome", "success")],
                started.elapsed(),
            );
            response
        }
        Err(error) => {
            let kind = sync_error_metric_kind(&error);
            state.metrics.inc_labeled(
                "task_space_sync_reconcile_failures_total",
                &[("kind", kind)],
            );
            state.metrics.observe_ms_labeled(
                "task_space_sync_reconcile_duration_ms",
                &[("outcome", kind)],
                started.elapsed(),
            );
            return Err(ApiError::from(error));
        }
    };
    response.entitlement_version = entitlement.version;
    Ok(Json(response))
}

async fn sync_reconcile_scoped(
    State(state): State<SyncHttpState>,
    Extension(account): Extension<AuthenticatedAccount>,
    Path(space_id): Path<u64>,
    headers: HeaderMap,
    Json(request): Json<SyncReconcileRequest>,
) -> Result<Json<SyncReconcileResponse>, ApiError> {
    if request.space_id != space_id {
        return Err(ApiError::Store(PostgresStoreError::InvalidInput(
            "reconcile path and payload space ids must match".to_owned(),
        )));
    }
    sync_reconcile(State(state), Extension(account), headers, Json(request)).await
}

async fn sync_events(
    State(state): State<SyncHttpState>,
    Extension(account): Extension<AuthenticatedAccount>,
    headers: HeaderMap,
    Query(query): Query<SyncCursorQuery>,
) -> Result<Response, ApiError> {
    if !state.rate_limiter.allow_account_and_ip(
        &headers,
        "sync-events",
        &account.account_id,
        60,
        600,
        Duration::from_secs(60),
    ) {
        return Err(ApiError::RateLimited);
    }
    require_sync_entitlement(&state.billing, &account.account_id).await?;
    state.metrics.inc("task_space_sync_sse_connections_total");
    let after_event_id = headers
        .get(axum::http::HeaderName::from_static("last-event-id"))
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .or(query.after_event_id)
        .unwrap_or_default();
    if after_event_id > 0 {
        state.metrics.inc("task_space_sync_sse_reconnects_total");
    }
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
    if !reset_required {
        for _ in &replay {
            state.metrics.inc("task_space_sync_sse_replay_events_total");
        }
    }
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
    // Flush a comment as the first frame. This proves the response is a live
    // SSE stream immediately instead of making mobile browsers wait for the
    // first database event or the keep-alive timer before they consider the
    // connection usable.
    let connection_event = tokio_stream::once(Ok::<Event, Infallible>(
        Event::default()
            .comment("task-space-sync-connected")
            .retry(Duration::from_secs(5)),
    ));
    let replay_stream = connection_event.chain(tokio_stream::iter(replay_events));
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
    let mut response = Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(10))
                .text("heartbeat"),
        )
        .into_response();
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("text/event-stream; charset=utf-8"),
    );
    response.headers_mut().insert(
        CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-cache, no-store, must-revalidate"),
    );
    response.headers_mut().insert(
        axum::http::HeaderName::from_static("x-accel-buffering"),
        axum::http::HeaderValue::from_static("no"),
    );
    Ok(response)
}

#[derive(Debug, Deserialize)]
struct SyncCursorQuery {
    after_event_id: Option<u64>,
}

async fn list_spaces(
    State(state): State<SyncHttpState>,
    Extension(account): Extension<AuthenticatedAccount>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    if !state.rate_limiter.allow_account_and_ip(
        &headers,
        "sync-spaces",
        &account.account_id,
        120,
        600,
        Duration::from_secs(60),
    ) {
        return Err(ApiError::RateLimited);
    }
    require_sync_entitlement(&state.billing, &account.account_id).await?;
    let spaces = state
        .store
        .list_spaces(&account.account_id)
        .await
        .map_err(ApiError::from)?;
    let mut response = Json(spaces).into_response();
    response.headers_mut().insert(
        CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-store"),
    );
    Ok(response)
}

#[derive(Debug, Deserialize)]
struct RegisterSpaceRequest {
    name: String,
    #[serde(default)]
    stable_id: Option<String>,
}

async fn register_space(
    State(state): State<SyncHttpState>,
    Extension(account): Extension<AuthenticatedAccount>,
    Path(space_id): Path<u64>,
    headers: HeaderMap,
    Json(request): Json<RegisterSpaceRequest>,
) -> Result<StatusCode, ApiError> {
    if !state.rate_limiter.allow_account_and_ip(
        &headers,
        "sync-register",
        &account.account_id,
        120,
        600,
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
            request.stable_id.as_deref(),
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
    headers: HeaderMap,
    Json(mut request): Json<SyncMetadataRequest>,
) -> Result<Json<SyncMetadataResponse>, ApiError> {
    if !state.rate_limiter.allow_account_and_ip(
        &headers,
        "sync-metadata",
        &account.account_id,
        120,
        600,
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
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    if !state.rate_limiter.allow_account_and_ip(
        &headers,
        "account-entitlement",
        &account.account_id,
        120,
        600,
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
    headers: HeaderMap,
    Json(request): Json<CheckoutRequest>,
) -> Result<Json<CheckoutResponse>, ApiError> {
    if !state.rate_limiter.allow_account_and_ip(
        &headers,
        "checkout",
        &account.account_id,
        10,
        100,
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
    headers: HeaderMap,
) -> Result<Json<BillingPortalResponse>, ApiError> {
    if !state.rate_limiter.allow_account_and_ip(
        &headers,
        "billing-portal",
        &account.account_id,
        10,
        100,
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
            Self::Store(PostgresStoreError::MetadataConflictSuperseded) => {
                "METADATA_CONFLICT_SUPERSEDED"
            }
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
            Self::Store(PostgresStoreError::UnsupportedSyncDocumentSchema(_)) => {
                "CLIENT_UPGRADE_REQUIRED"
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
            Self::Store(PostgresStoreError::MetadataConflictSuperseded) => (
                axum::http::StatusCode::CONFLICT,
                "space metadata lost a deterministic concurrent conflict".to_owned(),
            ),
            Self::Store(PostgresStoreError::MetadataOperationIdReused) => (
                axum::http::StatusCode::CONFLICT,
                "metadata operation id was reused with a different request".to_owned(),
            ),
            Self::Store(PostgresStoreError::EventCursorRequiresReset) => (
                axum::http::StatusCode::CONFLICT,
                "sync cursor expired; reset required".to_owned(),
            ),
            Self::Store(PostgresStoreError::UnsupportedSyncDocumentSchema(_)) => (
                axum::http::StatusCode::UPGRADE_REQUIRED,
                "client document schema is not supported; upgrade required".to_owned(),
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
        response.headers_mut().insert(
            CACHE_CONTROL,
            axum::http::HeaderValue::from_static("no-store"),
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
    fn auth_errors_are_never_cacheable() {
        let response = AuthError::MissingAuthorization.into_response();
        assert_eq!(
            response.headers().get(CACHE_CONTROL),
            Some(&HeaderValue::from_static("no-store"))
        );
    }

    #[test]
    fn rate_limiter_separates_account_keys() {
        let limiter = RequestRateLimiter::default();
        assert!(limiter.allow("account-a".to_owned(), 1, Duration::from_secs(60)));
        assert!(!limiter.allow("account-a".to_owned(), 1, Duration::from_secs(60)));
        assert!(limiter.allow("account-b".to_owned(), 1, Duration::from_secs(60)));
    }

    #[test]
    fn rate_limiter_applies_both_account_and_normalized_ip_budgets() {
        let limiter = RequestRateLimiter::default();
        let mut first_ip = HeaderMap::new();
        first_ip.insert("x-real-ip", HeaderValue::from_static("192.0.2.1"));

        assert!(limiter.allow_account_and_ip(
            &first_ip,
            "sync-reconcile",
            "account-a",
            2,
            2,
            Duration::from_secs(60),
        ));
        assert!(limiter.allow_account_and_ip(
            &first_ip,
            "sync-reconcile",
            "account-b",
            2,
            2,
            Duration::from_secs(60),
        ));
        assert!(!limiter.allow_account_and_ip(
            &first_ip,
            "sync-reconcile",
            "account-c",
            2,
            2,
            Duration::from_secs(60),
        ));

        let mut second_ip = HeaderMap::new();
        second_ip.insert(
            "x-forwarded-for",
            HeaderValue::from_static("198.51.100.9, 10.0.0.2"),
        );
        assert!(limiter.allow_account_and_ip(
            &second_ip,
            "sync-reconcile",
            "account-c",
            2,
            2,
            Duration::from_secs(60),
        ));
        assert_eq!(client_ip(&second_ip), "198.51.100.9");
    }

    #[test]
    fn malformed_client_ip_headers_share_the_bounded_unknown_bucket() {
        let mut headers = HeaderMap::new();
        headers.insert("x-real-ip", HeaderValue::from_static("not-an-ip"));
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static("also-not-an-ip"),
        );
        assert_eq!(client_ip(&headers), "unknown");
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

        let mut bearer_headers = HeaderMap::new();
        bearer_headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer operator-secret"),
        );
        assert_eq!(
            metrics_endpoint(
                State(MetricsState {
                    metrics: Metrics::default(),
                    token: Some("operator-secret".to_owned()),
                }),
                bearer_headers,
            )
            .await
            .expect("Bearer token should scrape metrics")
            .status(),
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn health_probe_is_public_and_not_cacheable() {
        let response = healthz().await;
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
