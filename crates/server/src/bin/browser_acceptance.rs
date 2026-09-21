//! Disposable authenticated browser acceptance server.
//!
//! This binary is intentionally separate from the production server. It uses
//! a deterministic in-process verifier and a seeded entitled account so the
//! real frontend can exercise cookie-authenticated reconciliation in isolated
//! browser profiles without requiring a WorkOS or Dodo account. It must only
//! be run against a disposable database.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use axum::http::header::{ACCEPT, AUTHORIZATION, CACHE_CONTROL, CONTENT_TYPE, RETRY_AFTER};
use axum::http::{HeaderName, HeaderValue, Method};
use axum::middleware;
use task_core::billing::{
    BILLING_PROTOCOL_VERSION, BillingEvent, BillingEventType, SubscriptionPlan, SubscriptionStatus,
};
use task_server::dodo::{DodoClient, DodoClientConfig};
use task_server::http::{
    AuthError, AuthenticatedAccount, RequestRateLimiter, SessionVerifier, SyncHttpState,
    add_request_id, health_router, metrics_router, protected_sync_router,
};
use task_server::metrics::Metrics;
use task_server::postgres::{PostgresBillingStore, PostgresSyncStore};
use tokio::sync::RwLock;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::services::{ServeDir, ServeFile};
use uuid::Uuid;

const TEST_SESSION_COOKIE: &str = "browser-acceptance-session";

#[derive(Clone, Default)]
struct TestVerifier {
    sessions: Arc<RwLock<HashMap<String, AuthenticatedAccount>>>,
}

#[async_trait]
impl SessionVerifier for TestVerifier {
    async fn verify(&self, bearer_token: &str) -> Result<AuthenticatedAccount, AuthError> {
        self.sessions
            .read()
            .await
            .get(bearer_token)
            .cloned()
            .ok_or(AuthError::VerificationFailed)
    }
}

fn dodo_client() -> DodoClient {
    DodoClient::new(DodoClientConfig {
        api_key: "browser_acceptance_dodo_key".to_owned(),
        environment: "test_mode".to_owned(),
        return_url: "http://127.0.0.1:3301/app".to_owned(),
        pro_monthly_product_id: "browser_acceptance_monthly".to_owned(),
        pro_yearly_product_id: "browser_acceptance_yearly".to_owned(),
    })
    .expect("browser acceptance Dodo configuration should validate")
}

async fn seed_entitlement(
    billing: &PostgresBillingStore,
    account_id: &str,
) -> Result<(), task_server::postgres::PostgresStoreError> {
    let occurred_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    billing
        .apply_event(&BillingEvent {
            protocol_version: BILLING_PROTOCOL_VERSION,
            provider: "browser-acceptance".to_owned(),
            provider_event_id: format!("browser-acceptance-{}", Uuid::new_v4()),
            event_type: BillingEventType::SubscriptionStarted,
            account_id: account_id.to_owned(),
            plan: SubscriptionPlan::Pro,
            status: SubscriptionStatus::Active,
            provider_customer_id: None,
            provider_subscription_id: None,
            provider_payment_id: None,
            refund_amount: None,
            payment_amount: None,
            current_period_end: Some(occurred_at.saturating_add(24 * 60 * 60)),
            cancel_at_period_end: false,
            occurred_at,
        })
        .await
        .map(|_| ())
}

#[tokio::main]
async fn main() {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL is required");
    let store = PostgresSyncStore::connect(&database_url)
        .await
        .expect("browser acceptance database should be reachable");
    store
        .migrate()
        .await
        .expect("browser acceptance migrations should apply");
    let billing = PostgresBillingStore::new(store.pool().clone());
    let account_id = std::env::var("TASK_SPACE_BROWSER_TEST_ACCOUNT")
        .unwrap_or_else(|_| format!("browser-acceptance-{}", Uuid::new_v4()));
    seed_entitlement(&billing, &account_id)
        .await
        .expect("browser acceptance entitlement should be seeded");

    let verifier = TestVerifier::default();
    verifier.sessions.write().await.insert(
        TEST_SESSION_COOKIE.to_owned(),
        AuthenticatedAccount {
            user_id: format!("{account_id}-user"),
            account_id: account_id.clone(),
            session_id: format!("{account_id}-session"),
        },
    );

    let origin = std::env::var("TASK_SPACE_BROWSER_TEST_ORIGIN")
        .unwrap_or_else(|_| "http://127.0.0.1:3301".to_owned());
    let metrics = Metrics::default();
    let app = health_router(store.pool().clone(), metrics.clone())
        .merge(metrics_router(metrics.clone(), None))
        .merge(protected_sync_router(SyncHttpState {
            store,
            billing,
            payments: dodo_client(),
            verifier: Arc::new(verifier),
            session_cookie_name: "task_space_session".to_owned(),
            allowed_origins: Arc::new(vec![origin.clone()]),
            rate_limiter: RequestRateLimiter::default(),
            metrics,
        }))
        .layer(
            CorsLayer::new()
                .allow_origin(AllowOrigin::exact(
                    HeaderValue::from_str(&origin).expect("browser origin should be valid"),
                ))
                .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
                .allow_headers([
                    ACCEPT,
                    AUTHORIZATION,
                    CACHE_CONTROL,
                    CONTENT_TYPE,
                    HeaderName::from_static("last-event-id"),
                    HeaderName::from_static("x-request-id"),
                ])
                .expose_headers([
                    HeaderName::from_static("x-request-id"),
                    HeaderName::from_static("x-task-space-error-code"),
                    RETRY_AFTER,
                ])
                .allow_credentials(true),
        )
        .layer(middleware::from_fn(add_request_id));

    let static_dir = std::env::var_os("TASK_SPACE_BROWSER_STATIC_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("apps/web/dist"));
    let index = static_dir.join("index.html");
    let app = app
        .route_service("/", ServeFile::new(index.clone()))
        .route_service("/app", ServeFile::new(index.clone()))
        .route_service("/app/", ServeFile::new(index))
        .fallback_service(ServeDir::new(static_dir));
    let port = std::env::var("PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(3301);
    let address = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .expect("browser acceptance server should bind");
    println!(
        "browser acceptance server listening on http://127.0.0.1:{port}/app account={account_id} session_cookie={TEST_SESSION_COOKIE}"
    );
    axum::serve(listener, app)
        .await
        .expect("browser acceptance server should run");
}
