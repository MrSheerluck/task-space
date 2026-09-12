use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::header::{CONTENT_TYPE, COOKIE, HOST, ORIGIN};
use axum::http::{Method, Request, StatusCode};
use serde_json::{Value, json};
use task_core::BoardData;
use task_core::billing::{
    BILLING_PROTOCOL_VERSION, BillingEvent, BillingEventType, SubscriptionPlan, SubscriptionStatus,
};
use task_core::sync::{
    EncodedUpdate, SYNC_DOCUMENT_SCHEMA_VERSION, SYNC_RECONCILE_PROTOCOL_VERSION,
};
use task_core::{Note, crdt::SpaceDoc};
use task_server::dodo::{DodoClient, DodoClientConfig};
use task_server::http::{
    AuthError, AuthenticatedAccount, RequestRateLimiter, SessionVerifier, SyncHttpState,
    health_router, protected_sync_router,
};
use task_server::postgres::{PostgresBillingStore, PostgresStoreError, PostgresSyncStore};
use tokio::sync::RwLock;
use tower::ServiceExt;
use uuid::Uuid;

/// Run with:
///
/// TASK_SPACE_TEST_DATABASE_URL=postgres://... \
///   cargo test -p task-server --test http_sync -- --ignored --nocapture
///
/// This test intentionally exercises the real Axum router against PostgreSQL,
/// but uses a deterministic in-process verifier so it does not require a
/// WorkOS session or expose provider credentials.
#[derive(Clone, Default)]
struct TestVerifier {
    accounts: Arc<RwLock<HashMap<String, AuthenticatedAccount>>>,
}

#[async_trait]
impl SessionVerifier for TestVerifier {
    async fn verify(&self, bearer_token: &str) -> Result<AuthenticatedAccount, AuthError> {
        self.accounts
            .read()
            .await
            .get(bearer_token)
            .cloned()
            .ok_or(AuthError::VerificationFailed)
    }
}

fn dodo_client() -> DodoClient {
    DodoClient::new(DodoClientConfig {
        api_key: "dodo_test_http_acceptance".to_owned(),
        environment: "test_mode".to_owned(),
        return_url: "https://app.test/app".to_owned(),
        pro_monthly_product_id: "prod_http_monthly".to_owned(),
        pro_yearly_product_id: "prod_http_yearly".to_owned(),
    })
    .expect("test Dodo configuration should validate without making a request")
}

async fn send(
    app: Router,
    method: Method,
    uri: &str,
    headers: &[(&str, &str)],
    body: Value,
) -> axum::response::Response {
    let mut builder = Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    app.oneshot(
        builder
            .body(Body::from(body.to_string()))
            .expect("request should build"),
    )
    .await
    .expect("router should respond")
}

async fn body_json(response: axum::response::Response) -> Value {
    let body = to_bytes(response.into_body(), 256 * 1024)
        .await
        .expect("response body should be readable");
    serde_json::from_slice(&body).expect("response should be JSON")
}

fn bearer(token: &str) -> [(&'static str, &'static str); 1] {
    // The test owns its static token values; callers pass one of the two
    // literals below. Keeping this helper small makes request intent obvious.
    match token {
        "token-a" => [("authorization", "Bearer token-a")],
        "token-device-b" => [("authorization", "Bearer token-device-b")],
        _ => [("authorization", "Bearer token-b")],
    }
}

async fn activate_account(
    billing: &PostgresBillingStore,
    account_id: &str,
    event_id: &str,
) -> Result<(), PostgresStoreError> {
    billing
        .apply_event(&BillingEvent {
            protocol_version: BILLING_PROTOCOL_VERSION,
            provider: "http-test".to_owned(),
            provider_event_id: event_id.to_owned(),
            event_type: BillingEventType::SubscriptionStarted,
            account_id: account_id.to_owned(),
            plan: SubscriptionPlan::Pro,
            status: SubscriptionStatus::Active,
            provider_customer_id: None,
            provider_subscription_id: None,
            provider_payment_id: None,
            refund_amount: None,
            payment_amount: None,
            current_period_end: None,
            cancel_at_period_end: false,
            occurred_at: 1_800_000_000,
        })
        .await
        .map(|_| ())
}

#[tokio::test]
#[ignore = "requires TASK_SPACE_TEST_DATABASE_URL"]
async fn authenticated_http_boundary_enforces_csrf_entitlement_and_account_isolation() {
    let database_url = std::env::var("TASK_SPACE_TEST_DATABASE_URL")
        .expect("set TASK_SPACE_TEST_DATABASE_URL for the HTTP acceptance test");
    let store = PostgresSyncStore::connect(&database_url)
        .await
        .expect("PostgreSQL should be reachable");
    store.migrate().await.expect("migrations should apply");
    let pool = store.pool().clone();
    let billing = PostgresBillingStore::new(pool.clone());

    let account_a = format!("http-sync-a-{}", Uuid::new_v4());
    let account_b = format!("http-sync-b-{}", Uuid::new_v4());
    let space_id = 9_100_000_000_000_u64;
    let stable_space_id = Uuid::new_v4().to_string();
    let cleanup = || async {
        sqlx::query("DELETE FROM spaces WHERE account_id = $1 OR account_id = $2")
            .bind(&account_a)
            .bind(&account_b)
            .execute(&pool)
            .await
            .expect("spaces should be removable");
        sqlx::query("DELETE FROM billing_events WHERE account_id = $1 OR account_id = $2")
            .bind(&account_a)
            .bind(&account_b)
            .execute(&pool)
            .await
            .expect("billing events should be removable");
        sqlx::query("DELETE FROM billing_entitlements WHERE account_id = $1 OR account_id = $2")
            .bind(&account_a)
            .bind(&account_b)
            .execute(&pool)
            .await
            .expect("entitlements should be removable");
    };

    store
        .register_space(
            &account_a,
            space_id,
            Some(&stable_space_id),
            "HTTP acceptance space",
            100,
        )
        .await
        .expect("account A should own the test space");
    activate_account(
        &billing,
        &account_a,
        &format!("http-test-event-a-{}", Uuid::new_v4()),
    )
    .await
    .expect("account A should be entitled");
    activate_account(
        &billing,
        &account_b,
        &format!("http-test-event-b-{}", Uuid::new_v4()),
    )
    .await
    .expect("account B should be entitled");

    let verifier = TestVerifier::default();
    verifier.accounts.write().await.extend([
        (
            "token-a".to_owned(),
            AuthenticatedAccount {
                user_id: "user-a".to_owned(),
                account_id: account_a.clone(),
                session_id: "session-a".to_owned(),
            },
        ),
        (
            "token-b".to_owned(),
            AuthenticatedAccount {
                user_id: "user-b".to_owned(),
                account_id: account_b.clone(),
                session_id: "session-b".to_owned(),
            },
        ),
        (
            "token-device-b".to_owned(),
            AuthenticatedAccount {
                user_id: "user-a-device-b".to_owned(),
                account_id: account_a.clone(),
                session_id: "session-a-device-b".to_owned(),
            },
        ),
    ]);
    let app = health_router(pool.clone(), Default::default()).merge(protected_sync_router(
        SyncHttpState {
            store: store.clone(),
            billing: billing.clone(),
            payments: dodo_client(),
            verifier: Arc::new(verifier),
            session_cookie_name: "task_space_session".to_owned(),
            allowed_origins: Arc::new(vec!["https://app.test".to_owned()]),
            rate_limiter: RequestRateLimiter::default(),
            metrics: Default::default(),
        },
    ));

    let health = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .expect("health request should build"),
        )
        .await
        .expect("health route should respond");
    assert_eq!(health.status(), StatusCode::OK);
    assert_eq!(body_json(health).await["status"], "ok");

    let missing_session = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/sync/spaces")
                .body(Body::empty())
                .expect("unauthenticated request should build"),
        )
        .await
        .expect("auth middleware should respond");
    assert_eq!(missing_session.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        missing_session
            .headers()
            .get("cache-control")
            .and_then(|value| value.to_str().ok()),
        Some("no-store")
    );

    let csrf_rejected = send(
        app.clone(),
        Method::POST,
        &format!("/sync/spaces/{}", space_id + 1),
        &[
            (COOKIE.as_str(), "task_space_session=token-a"),
            (HOST.as_str(), "app.test"),
        ],
        json!({"name": "should be rejected", "stable_id": Uuid::new_v4().to_string()}),
    )
    .await;
    assert_eq!(csrf_rejected.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        csrf_rejected
            .headers()
            .get("x-task-space-error-code")
            .and_then(|value| value.to_str().ok()),
        Some("CSRF_REJECTED")
    );

    let spaces_a = send(
        app.clone(),
        Method::GET,
        "/sync/spaces",
        &bearer("token-a"),
        json!(null),
    )
    .await;
    assert_eq!(spaces_a.status(), StatusCode::OK);
    let spaces_a = body_json(spaces_a).await;
    assert_eq!(spaces_a.as_array().map(Vec::len), Some(1));
    assert_eq!(spaces_a[0]["stable_id"], stable_space_id);
    // Space timestamps share the browser's epoch-millisecond contract used
    // by local projection ordering and tombstone comparisons.
    assert!(spaces_a[0]["created_at"].as_u64().unwrap_or_default() > 1_000_000_000_000);
    assert!(spaces_a[0]["updated_at"].as_u64().unwrap_or_default() > 1_000_000_000_000);

    let empty = SpaceDoc::new();
    let device_a = SpaceDoc::new();
    device_a.import_board(&BoardData {
        notes: vec![Note {
            id: 1,
            text: "HTTP device A".into(),
            ..Default::default()
        }],
        ..Default::default()
    });
    let device_b = SpaceDoc::new();
    device_b.import_board(&BoardData {
        notes: vec![Note {
            id: 2,
            text: "HTTP device B".into(),
            ..Default::default()
        }],
        ..Default::default()
    });
    let reconcile_body = |mutation_id: &str, device_id: &str, update: Vec<u8>| {
        json!({
            "protocol_version": SYNC_RECONCILE_PROTOCOL_VERSION,
            "document_schema_version": SYNC_DOCUMENT_SCHEMA_VERSION,
            "space_id": space_id,
            "stable_space_id": stable_space_id,
            "mutation_id": mutation_id,
            "device_id": device_id,
            "local_generation": 1,
            "last_server_sequence": 0,
            "state_vector": EncodedUpdate::from_bytes(&empty.state_vector()),
            "update": EncodedUpdate::from_bytes(&update),
        })
    };
    let request_a = reconcile_body(
        &format!("http-device-a-{}", Uuid::new_v4()),
        "http-device-a",
        device_a
            .encode_update(&empty.state_vector())
            .expect("device A update should encode"),
    );
    let request_b = reconcile_body(
        &format!("http-device-b-{}", Uuid::new_v4()),
        "http-device-b",
        device_b
            .encode_update(&empty.state_vector())
            .expect("device B update should encode"),
    );
    let device_a_headers = [
        ("authorization", "Bearer token-a"),
        (CONTENT_TYPE.as_str(), "application/json"),
    ];
    let device_b_headers = [
        ("authorization", "Bearer token-device-b"),
        (CONTENT_TYPE.as_str(), "application/json"),
    ];
    let (response_a, response_b) = tokio::join!(
        send(
            app.clone(),
            Method::POST,
            "/sync/v2/reconcile",
            &device_a_headers,
            request_a,
        ),
        send(
            app.clone(),
            Method::POST,
            "/sync/v2/reconcile",
            &device_b_headers,
            request_b,
        ),
    );
    assert_eq!(response_a.status(), StatusCode::OK);
    assert_eq!(response_b.status(), StatusCode::OK);
    let pull_headers = [
        ("authorization", "Bearer token-a"),
        (CONTENT_TYPE.as_str(), "application/json"),
    ];
    let merged_pull = send(
        app.clone(),
        Method::POST,
        "/sync/pull",
        &pull_headers,
        json!({
            "protocol_version": task_core::sync::SYNC_PROTOCOL_VERSION,
            "document_schema_version": SYNC_DOCUMENT_SCHEMA_VERSION,
            "space_id": space_id,
            "stable_space_id": stable_space_id,
            "last_server_sequence": 0,
            "state_vector": EncodedUpdate::from_bytes(&empty.state_vector()),
            "local_generation": 0,
        }),
    )
    .await;
    assert_eq!(merged_pull.status(), StatusCode::OK);
    let merged_pull = body_json(merged_pull).await;
    let merged_update = EncodedUpdate::from_base64(
        merged_pull["update"]
            .as_str()
            .expect("pull should return an encoded update"),
    )
    .expect("pull update should use the sync encoding");
    let merged =
        SpaceDoc::from_update(&merged_update.to_bytes().expect("pull update should decode"))
            .expect("merged HTTP pull should decode as a CRDT document");
    let merged_texts = merged
        .board()
        .notes
        .into_iter()
        .map(|note| note.text)
        .collect::<std::collections::HashSet<_>>();
    assert!(merged_texts.contains("HTTP device A"));
    assert!(merged_texts.contains("HTTP device B"));

    let cross_account_reconcile = send(
        app.clone(),
        Method::POST,
        "/sync/v2/reconcile",
        &[
            ("authorization", "Bearer token-b"),
            (CONTENT_TYPE.as_str(), "application/json"),
        ],
        json!({
            "protocol_version": SYNC_RECONCILE_PROTOCOL_VERSION,
            "document_schema_version": SYNC_DOCUMENT_SCHEMA_VERSION,
            "space_id": space_id,
            "stable_space_id": stable_space_id,
            "mutation_id": Uuid::new_v4().to_string(),
            "device_id": "http-test-device-b",
            "local_generation": 1,
            "last_server_sequence": 0,
            "state_vector": EncodedUpdate::from_bytes(&[]),
            "update": EncodedUpdate::from_bytes(&[]),
        }),
    )
    .await;
    assert_eq!(cross_account_reconcile.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        cross_account_reconcile
            .headers()
            .get("x-task-space-error-code")
            .and_then(|value| value.to_str().ok()),
        Some("SPACE_ACCESS_DENIED")
    );

    let cookie_register = send(
        app,
        Method::POST,
        &format!("/sync/spaces/{}", space_id + 1),
        &[
            (COOKIE.as_str(), "task_space_session=token-a"),
            (HOST.as_str(), "app.test"),
            (ORIGIN.as_str(), "https://app.test"),
            (CONTENT_TYPE.as_str(), "application/json"),
        ],
        json!({"name": "same-site", "stable_id": Uuid::new_v4().to_string()}),
    )
    .await;
    assert_eq!(cookie_register.status(), StatusCode::NO_CONTENT);

    cleanup().await;
}
