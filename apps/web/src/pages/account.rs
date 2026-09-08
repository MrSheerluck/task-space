use std::cell::RefCell;

use gloo_net::http::Request;
use serde::{Deserialize, Serialize};
use task_core::billing::Entitlement;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::{JsFuture, future_to_promise};
use web_sys::RequestCredentials;

use super::api::{api_url, send_request_with_timeout, send_with_timeout};

const AUTHENTICATED_SESSION_STORAGE_KEY: &str = "task_space_authenticated_session";

thread_local! {
    static REFRESH_IN_FLIGHT: RefCell<Option<js_sys::Promise>> = const { RefCell::new(None) };
}

#[derive(Clone, Debug, PartialEq)]
pub enum AccountState {
    Checking,
    Guest,
    SignedIn(Entitlement),
    Unavailable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckoutReturnState {
    None,
    Pending,
    Failed,
}

impl AccountState {
    pub fn is_authenticated(&self) -> bool {
        matches!(self, Self::SignedIn(_))
    }

    pub fn entitlement(&self) -> Option<&Entitlement> {
        match self {
            Self::SignedIn(entitlement) => Some(entitlement),
            _ => None,
        }
    }
}

pub fn remember_authenticated_session() {
    if let Some(window) = web_sys::window()
        && let Ok(Some(storage)) = window.session_storage()
    {
        let _ = storage.set_item(AUTHENTICATED_SESSION_STORAGE_KEY, "true");
    }
}

pub fn forget_authenticated_session() {
    if let Some(window) = web_sys::window()
        && let Ok(Some(storage)) = window.session_storage()
    {
        let _ = storage.remove_item(AUTHENTICATED_SESSION_STORAGE_KEY);
    }
}

pub async fn load_account_state() -> AccountState {
    let Ok(response) = account_entitlement().await else {
        return AccountState::Unavailable;
    };

    if response.status() == 401 {
        if refresh_session_once().await {
            return match account_entitlement().await {
                Ok(response) if (200..300).contains(&response.status()) => response
                    .json::<Entitlement>()
                    .await
                    .map(|entitlement| {
                        remember_authenticated_session();
                        AccountState::SignedIn(entitlement)
                    })
                    .unwrap_or(AccountState::Unavailable),
                Ok(response) if response.status() == 401 => AccountState::Guest,
                Ok(_) | Err(_) => AccountState::Unavailable,
            };
        }
    }

    match response.status() {
        200..=299 => response
            .json::<Entitlement>()
            .await
            .map(|entitlement| {
                remember_authenticated_session();
                AccountState::SignedIn(entitlement)
            })
            .unwrap_or(AccountState::Unavailable),
        401 => AccountState::Guest,
        _ => AccountState::Unavailable,
    }
}

/// Refresh the cookie session once, sharing a single in-flight rotation among
/// concurrent callers so refresh-token rotation cannot race.
pub async fn refresh_session_once() -> bool {
    if let Some(existing) = REFRESH_IN_FLIGHT.with(|flight| flight.borrow().clone()) {
        return JsFuture::from(existing)
            .await
            .ok()
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
    }
    let promise = future_to_promise(async {
        let success = send_with_timeout(
            Request::post(&api_url("/auth/refresh")).credentials(RequestCredentials::Include),
        )
        .await
        .is_ok_and(|response| (200..300).contains(&response.status()));
        Ok(JsValue::from_bool(success))
    });
    REFRESH_IN_FLIGHT.with(|flight| flight.replace(Some(promise.clone())));
    let result = JsFuture::from(promise)
        .await
        .ok()
        .and_then(|value| value.as_bool());
    REFRESH_IN_FLIGHT.with(|flight| {
        flight.borrow_mut().take();
    });
    result.unwrap_or(false)
}

async fn account_entitlement() -> Result<gloo_net::http::Response, gloo_net::Error> {
    // A guest 401 must not be reused after a successful sign-in on the same
    // browser. The entitlement is session state, so it should never be cached.
    let url = format!(
        "{}?check={}",
        api_url("/account/entitlement"),
        js_sys::Date::now()
    );
    send_with_timeout(Request::get(&url).credentials(RequestCredentials::Include)).await
}

#[derive(Debug, Deserialize)]
struct CheckoutResponse {
    checkout_url: String,
}

#[derive(Debug, Deserialize)]
struct BillingPortalResponse {
    portal_url: String,
}

#[derive(Debug, Serialize)]
struct CheckoutRequest<'a> {
    interval: &'a str,
}

pub async fn start_checkout(interval: &'static str) -> Result<String, String> {
    let mut refreshed = false;
    let response = loop {
        let request = Request::post(&api_url("/billing/checkout"))
            .credentials(RequestCredentials::Include)
            .json(&CheckoutRequest { interval })
            .map_err(|_| "the checkout request could not be prepared".to_owned())?;
        let response = send_request_with_timeout(request)
            .await
            .map_err(|_| "the billing service could not be reached".to_owned())?;
        if response.status() == 401 && !refreshed && refresh_session_once().await {
            refreshed = true;
            continue;
        }
        break response;
    };
    if !(200..300).contains(&response.status()) {
        let status = response.status();
        if status == 401 {
            return Err("your session expired; please sign in again before upgrading".to_owned());
        }
        let detail = response.text().await.unwrap_or_default();
        let detail = detail.trim();
        return Err(if detail.is_empty() {
            format!("checkout could not start (HTTP {status}); please try again")
        } else {
            format!("checkout could not start: {detail}")
        });
    }
    response
        .json::<CheckoutResponse>()
        .await
        .map(|checkout| checkout.checkout_url)
        .map_err(|_| "the billing service returned an unexpected response".to_owned())
}

pub async fn start_billing_portal() -> Result<String, String> {
    let mut refreshed = false;
    let response = loop {
        let response = send_with_timeout(
            Request::post(&api_url("/billing/portal")).credentials(RequestCredentials::Include),
        )
        .await
        .map_err(|_| "the billing service could not be reached".to_owned())?;
        if response.status() == 401 && !refreshed && refresh_session_once().await {
            refreshed = true;
            continue;
        }
        break response;
    };
    if !(200..300).contains(&response.status()) {
        let status = response.status();
        if status == 401 {
            return Err("your session expired; please sign in again".to_owned());
        }
        let detail = response.text().await.unwrap_or_default();
        let detail = detail.trim();
        return Err(if detail.is_empty() {
            format!("billing portal could not open (HTTP {status}); please try again")
        } else {
            format!("billing portal could not open: {detail}")
        });
    }
    response
        .json::<BillingPortalResponse>()
        .await
        .map(|portal| portal.portal_url)
        .map_err(|_| "the billing service returned an unexpected response".to_owned())
}

fn checkout_return_state_from_search(search: &str) -> CheckoutReturnState {
    let mut has_checkout_parameter = false;
    let mut returned_status = None;

    for parameter in search.trim_start_matches('?').split('&') {
        let Some((name, value)) = parameter.split_once('=') else {
            continue;
        };
        if matches!(name, "payment_id" | "subscription_id" | "status") {
            has_checkout_parameter = true;
        }
        if name == "status" {
            returned_status = Some(value.to_ascii_lowercase());
        }
    }

    if returned_status
        .as_deref()
        .is_some_and(|status| matches!(status, "failed" | "cancelled" | "canceled" | "expired"))
    {
        CheckoutReturnState::Failed
    } else if has_checkout_parameter {
        CheckoutReturnState::Pending
    } else {
        CheckoutReturnState::None
    }
}

pub fn checkout_return_state() -> CheckoutReturnState {
    let Some(window) = web_sys::window() else {
        return CheckoutReturnState::None;
    };
    let Ok(search) = window.location().search() else {
        return CheckoutReturnState::None;
    };
    checkout_return_state_from_search(&search)
}

async fn wait_ms(milliseconds: i32) {
    let promise = js_sys::Promise::new(&mut |resolve, _reject| {
        let Some(window) = web_sys::window() else {
            let _ = resolve.call0(&wasm_bindgen::JsValue::NULL);
            return;
        };
        let callback = Closure::once_into_js(move || {
            let _ = resolve.call0(&wasm_bindgen::JsValue::NULL);
        });
        let _ = window.set_timeout_with_callback_and_timeout_and_arguments_0(
            callback.unchecked_ref(),
            milliseconds,
        );
    });
    let _ = JsFuture::from(promise).await;
}

/// A successful checkout redirects back before the webhook is guaranteed to
/// have reached us. Give the server a short window to apply the verified
/// subscription event before showing the upgrade gate again.
pub async fn load_account_state_after_checkout() -> AccountState {
    let mut state = load_account_state().await;
    if checkout_return_state() != CheckoutReturnState::Pending
        || state.entitlement().is_some_and(Entitlement::can_sync)
    {
        return state;
    }

    for _ in 0..15 {
        wait_ms(2_000).await;
        state = load_account_state().await;
        if state.entitlement().is_some_and(Entitlement::can_sync) {
            break;
        }
    }
    state
}

#[cfg(test)]
mod tests {
    use super::{CheckoutReturnState, checkout_return_state_from_search};

    #[test]
    fn recognizes_failed_checkout_returns() {
        assert_eq!(
            checkout_return_state_from_search("?subscription_id=sub_123&status=failed"),
            CheckoutReturnState::Failed
        );
        assert_eq!(
            checkout_return_state_from_search("?status=cancelled"),
            CheckoutReturnState::Failed
        );
    }

    #[test]
    fn recognizes_checkout_returns_that_may_be_waiting_for_a_webhook() {
        assert_eq!(
            checkout_return_state_from_search("?payment_id=pay_123&status=succeeded"),
            CheckoutReturnState::Pending
        );
        assert_eq!(
            checkout_return_state_from_search("?subscription_id=sub_123"),
            CheckoutReturnState::Pending
        );
        assert_eq!(
            checkout_return_state_from_search("?unrelated=value"),
            CheckoutReturnState::None
        );
    }
}

pub async fn sign_out() {
    forget_authenticated_session();
    let _ = send_with_timeout(
        Request::post(&api_url("/auth/logout")).credentials(RequestCredentials::Include),
    )
    .await;
    if let Some(window) = web_sys::window() {
        let _ = window.location().set_href("/");
    }
}
