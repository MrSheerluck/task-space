//! Dodo Payments webhook verification and normalization.
//!
//! Dodo uses Standard Webhooks signatures. The raw payload is verified before
//! it is parsed, then only the fields needed by the shared billing contract
//! are forwarded to `BillingStore`.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::Deserialize;
use serde_json::Value;
use sha2::Sha256;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use task_core::billing::{
    BILLING_PROTOCOL_VERSION, BillingEvent, BillingEventType, SubscriptionPlan, SubscriptionStatus,
};

use crate::billing::{BillingApplyResult, BillingStore, BillingStoreError};
use crate::postgres::{PostgresBillingStore, PostgresStoreError};

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Debug)]
pub struct DodoWebhookConfig {
    pub webhook_secret: String,
    pub pro_monthly_product_id: String,
    pub pro_yearly_product_id: String,
    pub max_timestamp_skew_seconds: u64,
}

impl DodoWebhookConfig {
    pub fn from_env() -> Result<Self, DodoError> {
        Ok(Self {
            webhook_secret: required_env("DODO_PAYMENTS_WEBHOOK_KEY")?,
            pro_monthly_product_id: required_env("DODO_PRO_MONTHLY_PRODUCT_ID")?,
            pro_yearly_product_id: required_env("DODO_PRO_YEARLY_PRODUCT_ID")?,
            max_timestamp_skew_seconds: 300,
        })
    }
}

#[derive(Clone)]
pub struct DodoWebhook {
    config: DodoWebhookConfig,
}

#[derive(Debug, thiserror::Error)]
pub enum DodoError {
    #[error("missing required environment variable {0}")]
    MissingEnvironment(&'static str),
    #[error("Dodo webhook header is missing {0}")]
    MissingHeader(&'static str),
    #[error("Dodo webhook signature is invalid")]
    InvalidSignature,
    #[error("Dodo webhook timestamp is invalid")]
    InvalidTimestamp,
    #[error("Dodo webhook payload is invalid: {0}")]
    InvalidPayload(String),
    #[error("Dodo billing event is missing account_id metadata")]
    MissingAccountMetadata,
    #[error("billing store error: {0}")]
    Billing(#[from] BillingStoreError),
    #[error("database billing store error: {0}")]
    Postgres(#[from] PostgresStoreError),
    #[error("Dodo API request failed with status {status}: {body}")]
    Api { status: u16, body: String },
    #[error("Dodo API could not be reached: {0}")]
    Request(String),
    #[error("Dodo checkout response did not contain a checkout URL")]
    MissingCheckoutUrl,
    #[error("Dodo customer portal response did not contain a portal URL")]
    MissingPortalUrl,
}

#[derive(Clone, Debug)]
pub struct DodoClientConfig {
    pub api_key: String,
    pub environment: String,
    pub return_url: String,
    pub pro_monthly_product_id: String,
    pub pro_yearly_product_id: String,
}

impl DodoClientConfig {
    pub fn from_env() -> Result<Self, DodoError> {
        Ok(Self {
            api_key: required_env("DODO_PAYMENTS_API_KEY")?,
            environment: std::env::var("DODO_PAYMENTS_ENVIRONMENT")
                .unwrap_or_else(|_| "test_mode".to_owned()),
            return_url: required_env("DODO_PAYMENTS_RETURN_URL")?,
            pro_monthly_product_id: required_env("DODO_PRO_MONTHLY_PRODUCT_ID")?,
            pro_yearly_product_id: required_env("DODO_PRO_YEARLY_PRODUCT_ID")?,
        })
    }
}

#[derive(Clone)]
pub struct DodoClient {
    client: reqwest::Client,
    config: DodoClientConfig,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum BillingInterval {
    Month,
    Year,
}

impl DodoClient {
    pub fn new(config: DodoClientConfig) -> Result<Self, DodoError> {
        if config.api_key.trim().is_empty()
            || config.return_url.trim().is_empty()
            || config.pro_monthly_product_id.trim().is_empty()
            || config.pro_yearly_product_id.trim().is_empty()
        {
            return Err(DodoError::InvalidPayload(
                "Dodo API key, return URL, and both product ids are required".to_owned(),
            ));
        }
        if !matches!(config.environment.as_str(), "test_mode" | "live_mode") {
            return Err(DodoError::InvalidPayload(
                "DODO_PAYMENTS_ENVIRONMENT must be test_mode or live_mode".to_owned(),
            ));
        }
        let return_url = url::Url::parse(&config.return_url)
            .map_err(|error| DodoError::InvalidPayload(format!("invalid return URL: {error}")))?;
        if !matches!(return_url.scheme(), "http" | "https") {
            return Err(DodoError::InvalidPayload(
                "DODO_PAYMENTS_RETURN_URL must use http or https".to_owned(),
            ));
        }
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .map_err(|error| DodoError::Request(error.to_string()))?;
        Ok(Self { client, config })
    }

    pub async fn create_checkout(
        &self,
        account_id: &str,
        interval: BillingInterval,
    ) -> Result<String, DodoError> {
        let product_id = match interval {
            BillingInterval::Month => &self.config.pro_monthly_product_id,
            BillingInterval::Year => &self.config.pro_yearly_product_id,
        };
        let base_url = match self.config.environment.as_str() {
            "test_mode" => "https://test.dodopayments.com",
            "live_mode" => "https://live.dodopayments.com",
            _ => unreachable!("Dodo environment is validated during client construction"),
        };
        let response = self
            .client
            .post(format!("{base_url}/checkouts"))
            .bearer_auth(&self.config.api_key)
            .json(&serde_json::json!({
                "product_cart": [{ "product_id": product_id, "quantity": 1 }],
                "metadata": { "account_id": account_id },
                "return_url": self.config.return_url,
            }))
            .send()
            .await
            .map_err(|error| DodoError::Request(error.to_string()))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|error| DodoError::InvalidPayload(error.to_string()))?;
        if !status.is_success() {
            return Err(DodoError::Api {
                status: status.as_u16(),
                body,
            });
        }
        let checkout: CheckoutSessionResponse = serde_json::from_str(&body)
            .map_err(|error| DodoError::InvalidPayload(error.to_string()))?;
        checkout.checkout_url.ok_or(DodoError::MissingCheckoutUrl)
    }

    pub async fn create_customer_portal(&self, customer_id: &str) -> Result<String, DodoError> {
        if !customer_id.starts_with("cus_")
            || !customer_id
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || character == '_')
        {
            return Err(DodoError::InvalidPayload(
                "invalid Dodo customer id".to_owned(),
            ));
        }
        let base_url = match self.config.environment.as_str() {
            "test_mode" => "https://test.dodopayments.com",
            "live_mode" => "https://live.dodopayments.com",
            _ => unreachable!("Dodo environment is validated during client construction"),
        };
        let response = self
            .client
            .post(format!(
                "{base_url}/customers/{customer_id}/customer-portal/session"
            ))
            .bearer_auth(&self.config.api_key)
            .query(&[("return_url", self.config.return_url.as_str())])
            .send()
            .await
            .map_err(|error| DodoError::Request(error.to_string()))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|error| DodoError::InvalidPayload(error.to_string()))?;
        if !status.is_success() {
            return Err(DodoError::Api {
                status: status.as_u16(),
                body,
            });
        }
        let portal: CustomerPortalResponse = serde_json::from_str(&body)
            .map_err(|error| DodoError::InvalidPayload(error.to_string()))?;
        portal.link.ok_or(DodoError::MissingPortalUrl)
    }
}

#[derive(Debug, Deserialize)]
struct CheckoutSessionResponse {
    checkout_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CustomerPortalResponse {
    link: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum DodoApplyResult {
    Ignored,
    Applied,
    Duplicate,
    Stale,
}

impl DodoWebhook {
    pub fn new(config: DodoWebhookConfig) -> Result<Self, DodoError> {
        if config.webhook_secret.trim().is_empty()
            || config.pro_monthly_product_id.trim().is_empty()
            || config.pro_yearly_product_id.trim().is_empty()
        {
            return Err(DodoError::InvalidPayload(
                "webhook secret and both product ids are required".to_owned(),
            ));
        }
        Ok(Self { config })
    }

    pub fn verify(&self, headers: &HeaderMap, body: &[u8]) -> Result<u64, DodoError> {
        let webhook_id = header(headers, "webhook-id")?;
        let webhook_signature = header(headers, "webhook-signature")?;
        let webhook_timestamp = header(headers, "webhook-timestamp")?;
        let timestamp = webhook_timestamp
            .parse::<u64>()
            .map_err(|_| DodoError::InvalidTimestamp)?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| DodoError::InvalidTimestamp)?
            .as_secs();
        if now.abs_diff(timestamp) > self.config.max_timestamp_skew_seconds {
            return Err(DodoError::InvalidTimestamp);
        }

        let secret = self
            .config
            .webhook_secret
            .strip_prefix("whsec_")
            .unwrap_or(&self.config.webhook_secret);
        let secret = base64::engine::general_purpose::STANDARD
            .decode(secret)
            .map_err(|_| DodoError::InvalidSignature)?;
        let signed_payload = format!("{webhook_id}.{webhook_timestamp}.");
        let mut signed_payload = signed_payload.into_bytes();
        signed_payload.extend_from_slice(body);
        let mut mac =
            HmacSha256::new_from_slice(&secret).map_err(|_| DodoError::InvalidSignature)?;
        mac.update(&signed_payload);
        let valid = webhook_signature.split_whitespace().any(|signature| {
            let encoded = signature.strip_prefix("v1,").unwrap_or(signature);
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .ok()
                .map(|candidate| mac.clone().verify_slice(&candidate).is_ok())
                .unwrap_or(false)
        });
        if !valid {
            return Err(DodoError::InvalidSignature);
        }
        Ok(timestamp)
    }

    pub fn normalize(
        &self,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<Option<BillingEvent>, DodoError> {
        let verified_timestamp = self.verify(headers, body)?;
        let payload: DodoEnvelope = serde_json::from_slice(body)
            .map_err(|error| DodoError::InvalidPayload(error.to_string()))?;
        let Some(event_type) = normalize_event_type(&payload.event_type) else {
            return Ok(None);
        };
        // Current subscription webhooks put the resource directly in `data`.
        // Some Dodo webhook examples and older payloads wrap it in
        // `data.object`; accepting both prevents valid signed events from being
        // silently ignored during provider-side schema transitions.
        let data = payload.data.get("object").unwrap_or(&payload.data);
        let product_id = data
            .get("product_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if product_id != self.config.pro_monthly_product_id
            && product_id != self.config.pro_yearly_product_id
        {
            return Ok(None);
        }
        let account_id = data
            .get("metadata")
            .and_then(|metadata| metadata.get("account_id"))
            .and_then(Value::as_str)
            .filter(|account_id| !account_id.trim().is_empty())
            .ok_or(DodoError::MissingAccountMetadata)?;
        let status = normalize_status(&payload.event_type, data).ok_or_else(|| {
            DodoError::InvalidPayload("subscription status is missing".to_owned())
        })?;
        let occurred_at = payload
            .timestamp
            .as_deref()
            .and_then(parse_timestamp)
            .unwrap_or(verified_timestamp);

        Ok(Some(BillingEvent {
            protocol_version: BILLING_PROTOCOL_VERSION,
            provider: "dodo".to_owned(),
            provider_event_id: header(headers, "webhook-id")?.to_owned(),
            event_type,
            account_id: account_id.to_owned(),
            plan: SubscriptionPlan::Pro,
            status,
            provider_customer_id: customer_id(data),
            provider_subscription_id: data
                .get("subscription_id")
                .and_then(Value::as_str)
                .map(str::to_owned),
            current_period_end: data
                .get("next_billing_date")
                .and_then(Value::as_str)
                .and_then(parse_timestamp),
            cancel_at_period_end: data
                .get("cancel_at_next_billing_date")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            occurred_at,
        }))
    }

    pub fn apply(
        &self,
        headers: &HeaderMap,
        body: &[u8],
        billing: &BillingStore,
    ) -> Result<DodoApplyResult, DodoError> {
        let Some(event) = self.normalize(headers, body)? else {
            return Ok(DodoApplyResult::Ignored);
        };
        Ok(match billing.apply_event(&event)? {
            BillingApplyResult::Applied(_) => DodoApplyResult::Applied,
            BillingApplyResult::Duplicate(_) => DodoApplyResult::Duplicate,
            BillingApplyResult::Stale(_) => DodoApplyResult::Stale,
        })
    }

    pub async fn apply_postgres(
        &self,
        headers: &HeaderMap,
        body: &[u8],
        billing: &PostgresBillingStore,
    ) -> Result<DodoApplyResult, DodoError> {
        let Some(event) = self.normalize(headers, body)? else {
            return Ok(DodoApplyResult::Ignored);
        };
        Ok(match billing.apply_event(&event).await? {
            BillingApplyResult::Applied(_) => DodoApplyResult::Applied,
            BillingApplyResult::Duplicate(_) => DodoApplyResult::Duplicate,
            BillingApplyResult::Stale(_) => DodoApplyResult::Stale,
        })
    }
}

#[derive(Clone)]
struct DodoHttpState {
    webhook: Arc<DodoWebhook>,
    billing: PostgresBillingStore,
}

pub fn router(webhook: Arc<DodoWebhook>, billing: PostgresBillingStore) -> Router {
    Router::new()
        .route("/webhooks/dodo", post(receive_webhook))
        .with_state(DodoHttpState { webhook, billing })
}

async fn receive_webhook(
    State(state): State<DodoHttpState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<WebhookAck>, DodoRouteError> {
    state
        .webhook
        .apply_postgres(&headers, &body, &state.billing)
        .await?;
    Ok(Json(WebhookAck { received: true }))
}

#[derive(serde::Serialize)]
struct WebhookAck {
    received: bool,
}

#[derive(Debug, Deserialize)]
struct DodoEnvelope {
    #[serde(rename = "type")]
    event_type: String,
    #[serde(default)]
    timestamp: Option<String>,
    data: Value,
}

fn normalize_event_type(event_type: &str) -> Option<BillingEventType> {
    Some(match event_type {
        "subscription.active" => BillingEventType::SubscriptionStarted,
        "subscription.renewed" => BillingEventType::SubscriptionRenewed,
        "subscription.updated" => BillingEventType::SubscriptionChanged,
        "subscription.plan_changed" => BillingEventType::SubscriptionChanged,
        "subscription.on_hold" => BillingEventType::SubscriptionPastDue,
        "subscription.past_due" => BillingEventType::SubscriptionPastDue,
        "subscription.paused" => BillingEventType::SubscriptionPastDue,
        "subscription.unpaused" => BillingEventType::SubscriptionChanged,
        "subscription.cancelled" => BillingEventType::SubscriptionCanceled,
        "subscription.expired" => BillingEventType::SubscriptionEnded,
        "subscription.failed" => BillingEventType::SubscriptionEnded,
        _ => return None,
    })
}

fn normalize_status(event_type: &str, data: &Value) -> Option<SubscriptionStatus> {
    let status = data.get("status").and_then(Value::as_str);
    Some(match event_type {
        "subscription.active" | "subscription.renewed" => SubscriptionStatus::Active,
        "subscription.on_hold" | "subscription.past_due" | "subscription.paused" => {
            SubscriptionStatus::PastDue
        }
        "subscription.unpaused" => SubscriptionStatus::Active,
        "subscription.cancelled" | "subscription.expired" | "subscription.failed" => {
            SubscriptionStatus::Ended
        }
        "subscription.updated" | "subscription.plan_changed" => match status? {
            "active" => SubscriptionStatus::Active,
            "on_hold" | "past_due" | "paused" => SubscriptionStatus::PastDue,
            "cancelled" | "expired" | "failed" => SubscriptionStatus::Ended,
            _ => return None,
        },
        _ => return None,
    })
}

fn customer_id(data: &Value) -> Option<String> {
    data.get("customer")
        .and_then(|customer| customer.get("customer_id"))
        .and_then(Value::as_str)
        .or_else(|| data.get("customer_id").and_then(Value::as_str))
        .map(str::to_owned)
}

fn parse_timestamp(value: &str) -> Option<u64> {
    OffsetDateTime::parse(value, &Rfc3339)
        .ok()
        .and_then(|date| u64::try_from(date.unix_timestamp()).ok())
}

fn header<'a>(headers: &'a HeaderMap, name: &'static str) -> Result<&'a str, DodoError> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .ok_or(DodoError::MissingHeader(name))
}

fn required_env(name: &'static str) -> Result<String, DodoError> {
    std::env::var(name).map_err(|_| DodoError::MissingEnvironment(name))
}

#[derive(Debug)]
enum DodoRouteError {
    Dodo(DodoError),
}

impl From<DodoError> for DodoRouteError {
    fn from(error: DodoError) -> Self {
        Self::Dodo(error)
    }
}

impl IntoResponse for DodoRouteError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::Dodo(DodoError::InvalidSignature) => StatusCode::UNAUTHORIZED,
            Self::Dodo(DodoError::Billing(BillingStoreError::LockPoisoned)) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            Self::Dodo(DodoError::Postgres(_)) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Dodo(_) => StatusCode::BAD_REQUEST,
        };
        (status, "invalid Dodo webhook").into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderName, HeaderValue};
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;
    use hmac::Mac;

    fn webhook() -> DodoWebhook {
        DodoWebhook::new(DodoWebhookConfig {
            webhook_secret: format!("whsec_{}", STANDARD.encode(b"test-secret")),
            pro_monthly_product_id: "prod-monthly".to_owned(),
            pro_yearly_product_id: "prod-yearly".to_owned(),
            max_timestamp_skew_seconds: 300,
        })
        .unwrap()
    }

    fn client_config() -> DodoClientConfig {
        DodoClientConfig {
            api_key: "dodo_test_example".to_owned(),
            environment: "test_mode".to_owned(),
            return_url: "http://localhost:8080/app".to_owned(),
            pro_monthly_product_id: "prod-monthly".to_owned(),
            pro_yearly_product_id: "prod-yearly".to_owned(),
        }
    }

    #[test]
    fn checkout_configuration_is_validated_at_startup() {
        assert!(DodoClient::new(client_config()).is_ok());

        let mut bad_environment = client_config();
        bad_environment.environment = "test".to_owned();
        assert!(matches!(
            DodoClient::new(bad_environment),
            Err(DodoError::InvalidPayload(message))
                if message.contains("DODO_PAYMENTS_ENVIRONMENT")
        ));

        let mut bad_return_url = client_config();
        bad_return_url.return_url = "/app".to_owned();
        assert!(matches!(
            DodoClient::new(bad_return_url),
            Err(DodoError::InvalidPayload(message)) if message.contains("return URL")
        ));
    }

    fn signed_headers(body: &[u8]) -> HeaderMap {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .to_string();
        let id = "evt-dodo-1";
        let mut mac = HmacSha256::new_from_slice(b"test-secret").unwrap();
        mac.update(format!("{id}.{timestamp}.").as_bytes());
        mac.update(body);
        let signature = format!("v1,{}", STANDARD.encode(mac.finalize().into_bytes()));
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("webhook-id"),
            HeaderValue::from_str(id).unwrap(),
        );
        headers.insert(
            HeaderName::from_static("webhook-timestamp"),
            HeaderValue::from_str(&timestamp).unwrap(),
        );
        headers.insert(
            HeaderName::from_static("webhook-signature"),
            HeaderValue::from_str(&signature).unwrap(),
        );
        headers
    }

    #[test]
    fn verifies_and_normalizes_monthly_subscription() {
        let body = br#"{
            "type":"subscription.active",
            "timestamp":"2026-09-03T05:00:00Z",
            "data":{
                "product_id":"prod-monthly",
                "subscription_id":"sub-1",
                "customer":{"customer_id":"cus-1"},
                "metadata":{"account_id":"account-1"},
                "next_billing_date":"2026-10-03T05:00:00Z",
                "cancel_at_next_billing_date":false
            }
        }"#;
        let event = webhook()
            .normalize(&signed_headers(body), body)
            .unwrap()
            .unwrap();

        assert_eq!(event.account_id, "account-1");
        assert_eq!(event.status, SubscriptionStatus::Active);
        assert_eq!(event.provider_subscription_id.as_deref(), Some("sub-1"));
    }

    #[test]
    fn maps_yearly_product_to_the_same_pro_entitlement() {
        let body = br#"{
            "type":"subscription.active",
            "timestamp":"2026-09-03T05:00:00Z",
            "data":{
                "product_id":"prod-yearly",
                "subscription_id":"sub-yearly",
                "metadata":{"account_id":"account-1"}
            }
        }"#;
        let event = webhook()
            .normalize(&signed_headers(body), body)
            .unwrap()
            .unwrap();

        assert_eq!(event.plan, SubscriptionPlan::Pro);
        assert_eq!(event.status, SubscriptionStatus::Active);
        assert_eq!(
            event.provider_subscription_id.as_deref(),
            Some("sub-yearly")
        );
    }

    #[test]
    fn accepts_object_wrapped_subscription_payloads() {
        let body = br#"{
            "type":"subscription.updated",
            "timestamp":"2026-09-03T05:00:00Z",
            "data":{"object":{
                "product_id":"prod-monthly",
                "subscription_id":"sub-wrapped",
                "status":"active",
                "customer_id":"cus-wrapped",
                "metadata":{"account_id":"account-wrapped"}
            }}
        }"#;
        let event = webhook()
            .normalize(&signed_headers(body), body)
            .unwrap()
            .unwrap();

        assert_eq!(event.account_id, "account-wrapped");
        assert_eq!(event.status, SubscriptionStatus::Active);
        assert_eq!(
            event.provider_subscription_id.as_deref(),
            Some("sub-wrapped")
        );
        assert_eq!(event.provider_customer_id.as_deref(), Some("cus-wrapped"));
    }

    #[test]
    fn rejects_tampered_payload() {
        let body = br#"{"type":"subscription.active","data":{"product_id":"prod-monthly","metadata":{"account_id":"account-1"}}}"#;
        let mut headers = signed_headers(body);
        let tampered = br#"{"type":"subscription.cancelled","data":{"product_id":"prod-monthly","metadata":{"account_id":"account-1"}}}"#;
        assert!(matches!(
            webhook().normalize(&headers, tampered),
            Err(DodoError::InvalidSignature)
        ));
        headers.remove("webhook-signature");
        assert!(matches!(
            webhook().normalize(&headers, body),
            Err(DodoError::MissingHeader("webhook-signature"))
        ));
    }
}
