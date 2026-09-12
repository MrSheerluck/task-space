use std::sync::Arc;
use std::time::Duration;

use axum::http::header::{ACCEPT, AUTHORIZATION, CACHE_CONTROL, CONTENT_TYPE, RETRY_AFTER};
use axum::http::{HeaderName, HeaderValue, Method};
use axum::middleware;
use sqlx::PgPool;
use task_server::dodo::{DodoClient, DodoClientConfig, DodoWebhook, DodoWebhookConfig};
use task_server::http::{
    RequestRateLimiter, SyncHttpState, add_request_id, health_router, metrics_router,
    protected_sync_router,
};
use task_server::metrics::Metrics;
use task_server::postgres::{PostgresBillingStore, PostgresSyncStore};
use task_server::workos::{self, WorkOsAuth, WorkOsAuthConfig};
use tower_http::cors::{AllowOrigin, CorsLayer};
use url::Url;

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
    let migrated_documents = store
        .backfill_document_migrations()
        .await
        .expect("CRDT document migrations should run");
    if migrated_documents > 0 {
        eprintln!("materialized {migrated_documents} migrated CRDT snapshots");
    }
    store.start_event_listener();
    workos
        .check_readiness()
        .await
        .expect("WorkOS JWKS readiness check should pass");
    let retention_seconds = std::env::var("SYNC_EVENT_RETENTION_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(90 * 24 * 60 * 60);
    let tombstone_retention_seconds = std::env::var("SYNC_TOMBSTONE_RETENTION_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(retention_seconds);
    let metrics = Metrics::default();
    let retention_store = store.clone();
    let retention_metrics = metrics.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60 * 60));
        loop {
            interval.tick().await;
            let tombstone_retention_millis = tombstone_retention_seconds.saturating_mul(1_000);
            retention_metrics.inc("task_space_sync_compaction_runs_total");
            match retention_store
                .compact_documents(retention_seconds, tombstone_retention_millis, 100)
                .await
            {
                Ok(compacted) if compacted > 0 => {
                    retention_metrics.add("task_space_sync_compacted_snapshots_total", compacted);
                    eprintln!("sync document compaction replaced {compacted} snapshots")
                }
                Ok(_) => {}
                Err(error) => {
                    retention_metrics.inc("task_space_sync_compaction_failures_total");
                    eprintln!("sync document compaction failed: {error}");
                }
            }
            match retention_store.prune_history(retention_seconds).await {
                Ok(deleted) => {
                    retention_metrics.add("task_space_sync_pruned_updates_total", deleted);
                }
                Err(error) => {
                    retention_metrics.inc("task_space_sync_retention_failures_total");
                    eprintln!("sync history retention failed: {error}");
                }
            }
        }
    });
    let billing = PostgresBillingStore::new(pool);
    let billing_history_store = billing.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60 * 60));
        loop {
            interval.tick().await;
            if let Err(error) = billing_history_store
                .recover_stale_webhook_attempts(15 * 60)
                .await
            {
                eprintln!("stale billing webhook recovery failed: {error}");
            }
            if let Err(error) = billing_history_store
                .prune_billing_history(retention_seconds)
                .await
            {
                eprintln!("billing history retention failed: {error}");
            }
        }
    });
    let reconciliation_billing = billing.clone();
    let reconciliation_client = dodo_client.clone();
    let reconciliation_webhook = dodo.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(15 * 60));
        loop {
            interval.tick().await;
            let candidates = match reconciliation_billing
                .reconciliation_candidates(50, 60 * 60)
                .await
            {
                Ok(candidates) => candidates,
                Err(error) => {
                    eprintln!("billing reconciliation query failed: {error}");
                    continue;
                }
            };
            for (account_id, subscription_id) in candidates {
                let result: Result<(), String> = async {
                    let snapshot = reconciliation_client
                        .fetch_subscription(&subscription_id)
                        .await
                        .map_err(|error| error.to_string())?;
                    let event = reconciliation_webhook
                        .normalize_subscription_snapshot(&account_id, &snapshot)
                        .map_err(|error| error.to_string())?;
                    reconciliation_billing
                        .apply_event(&event)
                        .await
                        .map(|_| ())
                        .map_err(|error| error.to_string())
                }
                .await;
                match result {
                    Ok(_) => {
                        if let Err(error) = reconciliation_billing
                            .mark_reconciliation(&account_id, None)
                            .await
                        {
                            eprintln!("billing reconciliation checkpoint failed: {error}");
                        }
                    }
                    Err(error) => {
                        eprintln!("billing reconciliation failed for {account_id}: {error}");
                        let _ = reconciliation_billing
                            .mark_reconciliation(&account_id, Some(&error.to_string()))
                            .await;
                    }
                }
            }
        }
    });
    let configured_origins = allowed_origins();
    if std::env::var("TASK_SPACE_ALLOWED_ORIGINS").is_ok() && configured_origins.is_empty() {
        panic!("TASK_SPACE_ALLOWED_ORIGINS did not contain a valid HTTP(S) origin");
    }
    if std::env::var("DODO_PAYMENTS_ENVIRONMENT").as_deref() == Ok("live_mode")
        && configured_origins
            .iter()
            .any(|origin| !origin.starts_with("https://"))
    {
        panic!("live mode requires HTTPS TASK_SPACE_ALLOWED_ORIGINS");
    }
    let local_cors = CorsLayer::new()
        .allow_origin(AllowOrigin::predicate({
            let allowed_origins = configured_origins.clone();
            move |origin: &HeaderValue, _| {
                origin
                    .to_str()
                    .ok()
                    .is_some_and(|origin| allowed_origins.iter().any(|allowed| allowed == origin))
            }
        }))
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
        .allow_credentials(true);
    let app = workos::router(workos.clone())
        .merge(task_server::dodo::router(
            dodo,
            billing.clone(),
            metrics.clone(),
        ))
        .merge(health_router(store.pool().clone(), metrics.clone()))
        .merge(metrics_router(
            metrics.clone(),
            std::env::var("TASK_SPACE_METRICS_TOKEN").ok(),
        ))
        .merge(protected_sync_router(SyncHttpState {
            store,
            billing,
            payments: dodo_client,
            verifier: workos.clone(),
            session_cookie_name: workos.cookie_name().to_owned(),
            allowed_origins: Arc::new(configured_origins),
            rate_limiter: RequestRateLimiter::default(),
            metrics,
        }))
        .layer(local_cors)
        .layer(middleware::from_fn(add_request_id));
    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".to_owned());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .expect("server should bind");
    println!("task-space server listening on {listener:?}");
    axum::serve(listener, app).await.expect("server should run");
}

fn allowed_origins() -> Vec<String> {
    if let Ok(value) = std::env::var("TASK_SPACE_ALLOWED_ORIGINS") {
        // Presence of the explicit variable is authoritative, even if an
        // operator mistyped an origin. Falling back in that case could widen
        // a deliberately restricted production policy.
        return value.split(',').filter_map(normalize_origin).collect();
    }
    let mut origins = vec![
        "http://localhost".to_owned(),
        "http://localhost:8080".to_owned(),
        "http://localhost:3000".to_owned(),
        "http://127.0.0.1".to_owned(),
        "http://127.0.0.1:8080".to_owned(),
        "http://127.0.0.1:3000".to_owned(),
        "http://[::1]".to_owned(),
        "http://[::1]:8080".to_owned(),
        "http://[::1]:3000".to_owned(),
    ];
    // A stable HTTPS test origin is already required for the auth callback
    // and payment return URL. When the explicit allowlist is omitted, derive
    // the browser origin from those server-owned URLs so cookie-authenticated
    // POSTs from an ngrok/staging host are not silently rejected as CSRF. An
    // explicit TASK_SPACE_ALLOWED_ORIGINS value remains authoritative in
    // production and can be used to narrow this set to one exact origin.
    for variable in [
        "WORKOS_POST_LOGIN_REDIRECT_URI",
        "DODO_PAYMENTS_RETURN_URL",
        "WORKOS_REDIRECT_URI",
    ] {
        if let Ok(value) = std::env::var(variable)
            && let Some(origin) = origin_from_url(&value)
            && !origins.iter().any(|existing| existing == &origin)
        {
            origins.push(origin);
        }
    }
    origins
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
    origin_from_parsed_url(&url)
}

fn origin_from_url(value: &str) -> Option<String> {
    let url = Url::parse(value.trim()).ok()?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return None;
    }
    origin_from_parsed_url(&url)
}

fn origin_from_parsed_url(url: &Url) -> Option<String> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redirect_urls_reduce_to_the_exact_browser_origin() {
        assert_eq!(
            origin_from_url("https://adnate-anesthetically-jenice.ngrok-free.dev/auth/callback"),
            Some("https://adnate-anesthetically-jenice.ngrok-free.dev".to_owned())
        );
        assert_eq!(
            origin_from_url("https://app.example.test/app?checkout=1"),
            Some("https://app.example.test".to_owned())
        );
    }

    #[test]
    fn origin_derivation_rejects_credentials() {
        assert!(origin_from_url("https://user:password@app.example.test/app").is_none());
        assert!(normalize_origin("https://app.example.test/app").is_none());
    }
}
