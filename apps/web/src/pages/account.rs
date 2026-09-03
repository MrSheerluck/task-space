use gloo_net::http::Request;
use serde::{Deserialize, Serialize};
use task_core::billing::Entitlement;
use web_sys::RequestCredentials;

use super::api::api_url;

const AUTHENTICATED_SESSION_STORAGE_KEY: &str = "task_space_authenticated_session";

#[derive(Clone, Debug, PartialEq)]
pub enum AccountState {
    Checking,
    Guest,
    SignedIn(Entitlement),
    Unavailable,
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

fn has_authenticated_session_marker() -> bool {
    web_sys::window()
        .and_then(|window| window.session_storage().ok().flatten())
        .and_then(|storage| {
            storage
                .get_item(AUTHENTICATED_SESSION_STORAGE_KEY)
                .ok()
                .flatten()
        })
        .is_some_and(|value| value == "true")
}

pub fn forget_authenticated_session() {
    if let Some(window) = web_sys::window()
        && let Ok(Some(storage)) = window.session_storage()
    {
        let _ = storage.remove_item(AUTHENTICATED_SESSION_STORAGE_KEY);
    }
}

pub async fn load_account_state() -> AccountState {
    let session_marker = has_authenticated_session_marker();
    let Ok(response) = account_entitlement().await else {
        return if session_marker {
            AccountState::SignedIn(Entitlement::free("session-check-pending"))
        } else {
            AccountState::Unavailable
        };
    };

    if response.status() == 401 {
        let refreshed = Request::post(&api_url("/auth/refresh"))
            .credentials(RequestCredentials::Include)
            .send()
            .await
            .is_ok_and(|response| (200..300).contains(&response.status()));
        if refreshed {
            return match account_entitlement().await {
                Ok(response) if (200..300).contains(&response.status()) => response
                    .json::<Entitlement>()
                    .await
                    .map(|entitlement| {
                        remember_authenticated_session();
                        AccountState::SignedIn(entitlement)
                    })
                    .unwrap_or(AccountState::Unavailable),
                Ok(response) if response.status() == 401 => {
                    if session_marker {
                        AccountState::SignedIn(Entitlement::free("session-check-pending"))
                    } else {
                        AccountState::Guest
                    }
                }
                Ok(_) | Err(_) => {
                    if session_marker {
                        AccountState::SignedIn(Entitlement::free("session-check-pending"))
                    } else {
                        AccountState::Unavailable
                    }
                }
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
        401 => {
            if session_marker {
                AccountState::SignedIn(Entitlement::free("session-check-pending"))
            } else {
                AccountState::Guest
            }
        }
        _ => {
            if session_marker {
                AccountState::SignedIn(Entitlement::free("session-check-pending"))
            } else {
                AccountState::Unavailable
            }
        }
    }
}

async fn account_entitlement() -> Result<gloo_net::http::Response, gloo_net::Error> {
    // A guest 401 must not be reused after a successful sign-in on the same
    // browser. The entitlement is session state, so it should never be cached.
    let url = format!(
        "{}?check={}",
        api_url("/account/entitlement"),
        js_sys::Date::now()
    );
    Request::get(&url)
        .credentials(RequestCredentials::Include)
        .send()
        .await
}

#[derive(Debug, Deserialize)]
struct CheckoutResponse {
    checkout_url: String,
}

#[derive(Debug, Serialize)]
struct CheckoutRequest<'a> {
    interval: &'a str,
}

pub async fn start_checkout(interval: &'static str) -> Result<String, String> {
    let request = Request::post(&api_url("/billing/checkout"))
        .credentials(RequestCredentials::Include)
        .json(&CheckoutRequest { interval })
        .map_err(|_| "the checkout request could not be prepared".to_owned())?;
    let response = request
        .send()
        .await
        .map_err(|_| "the billing service could not be reached".to_owned())?;
    if !(200..300).contains(&response.status()) {
        return Err("we could not start checkout; please try again".to_owned());
    }
    response
        .json::<CheckoutResponse>()
        .await
        .map(|checkout| checkout.checkout_url)
        .map_err(|_| "the billing service returned an unexpected response".to_owned())
}

pub async fn sign_out() {
    forget_authenticated_session();
    let _ = Request::get(&api_url("/auth/logout"))
        .credentials(RequestCredentials::Include)
        .send()
        .await;
    if let Some(window) = web_sys::window() {
        let _ = window.location().set_href("/");
    }
}
