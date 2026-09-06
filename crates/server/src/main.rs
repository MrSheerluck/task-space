use std::sync::Arc;

use axum::http::header::{ACCEPT, AUTHORIZATION, CACHE_CONTROL, CONTENT_TYPE};
use axum::http::{HeaderValue, Method};
use sqlx::PgPool;
use task_server::dodo::{DodoClient, DodoClientConfig, DodoWebhook, DodoWebhookConfig};
use task_server::http::{SyncHttpState, protected_sync_router};
use task_server::postgres::{PostgresBillingStore, PostgresSyncStore};
use task_server::workos::{self, WorkOsAuth, WorkOsAuthConfig};
use tower_http::cors::{AllowOrigin, CorsLayer};

#[tokio::main]
async fn main() {
    let workos = Arc::new(
        WorkOsAuth::new(
            WorkOsAuthConfig::from_env().expect("WorkOS environment is not configured"),
        )
        .expect("WorkOS configuration is invalid"),
    );
    let dodo = Arc::new(
        DodoWebhook::new(
            DodoWebhookConfig::from_env().expect("Dodo environment is not configured"),
        )
        .expect("Dodo webhook configuration is invalid"),
    );
    let dodo_client = DodoClient::new(
        DodoClientConfig::from_env().expect("Dodo API configuration is not configured"),
    )
    .expect("Dodo API configuration is invalid");
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL is not configured");
    let pool = PgPool::connect(&database_url)
        .await
        .expect("database should be reachable");
    let store = PostgresSyncStore::new(pool.clone());
    store
        .migrate()
        .await
        .expect("database migrations should run");
    let billing = PostgresBillingStore::new(pool);
    let local_cors = CorsLayer::new()
        .allow_origin(AllowOrigin::predicate(|origin: &HeaderValue, _| {
            origin.to_str().ok().is_some_and(|origin| {
                matches!(
                    origin,
                    "http://localhost" | "http://localhost." | "http://127.0.0.1" | "http://[::1]"
                ) || origin.starts_with("http://localhost:")
                    || origin.starts_with("http://localhost.:")
                    || origin.starts_with("http://127.0.0.1:")
                    || origin.starts_with("http://[::1]:")
            })
        }))
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers([ACCEPT, AUTHORIZATION, CACHE_CONTROL, CONTENT_TYPE])
        .allow_credentials(true);
    let app = workos::router(workos.clone())
        .merge(task_server::dodo::router(dodo, billing.clone()))
        .merge(protected_sync_router(SyncHttpState {
            store,
            billing,
            payments: dodo_client,
            verifier: workos.clone(),
            session_cookie_name: workos.cookie_name().to_owned(),
        }))
        .layer(local_cors);
    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".to_owned());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .expect("server should bind");
    println!("task-space server listening on {listener:?}");
    axum::serve(listener, app).await.expect("server should run");
}
