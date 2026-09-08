use gloo_net::Error as GlooError;
use gloo_net::http::{Request, RequestBuilder, Response};
use wasm_bindgen::JsCast;
use wasm_bindgen::closure::Closure;
use wasm_bindgen_futures::JsFuture;
use web_sys::AbortController;

const API_REQUEST_TIMEOUT_MS: i32 = 15_000;

/// Send an API request with an abort deadline so a hung fetch cannot block the
/// local-first queue forever. Callers keep their existing retry classification
/// because an abort is surfaced as the same transport error as a disconnect.
pub async fn send_with_timeout(builder: RequestBuilder) -> Result<Response, GlooError> {
    let controller = AbortController::new()
        .map_err(|_| GlooError::GlooError("could not create request timeout".to_owned()))?;
    let signal = controller.signal();
    let Some(window) = web_sys::window() else {
        return builder.abort_signal(Some(&signal)).send().await;
    };
    let callback = Closure::wrap(Box::new(move || controller.abort()) as Box<dyn FnMut()>);
    let timeout = window
        .set_timeout_with_callback_and_timeout_and_arguments_0(
            callback.as_ref().unchecked_ref(),
            API_REQUEST_TIMEOUT_MS,
        )
        .ok();
    let result = builder.abort_signal(Some(&signal)).send().await;
    if let Some(timeout) = timeout {
        window.clear_timeout_with_handle(timeout);
    }
    drop(callback);
    result
}

/// Send an already-built request with the same abort deadline. JSON helpers in
/// `gloo-net` build a `Request` eagerly, so the signal is supplied through the
/// Fetch `RequestInit` rather than the builder.
pub async fn send_request_with_timeout(request: Request) -> Result<Response, GlooError> {
    let controller = AbortController::new()
        .map_err(|_| GlooError::GlooError("could not create request timeout".to_owned()))?;
    let signal = controller.signal();
    let Some(window) = web_sys::window() else {
        return request.send().await;
    };
    let callback = Closure::wrap(Box::new(move || controller.abort()) as Box<dyn FnMut()>);
    let timeout = window
        .set_timeout_with_callback_and_timeout_and_arguments_0(
            callback.as_ref().unchecked_ref(),
            API_REQUEST_TIMEOUT_MS,
        )
        .ok();
    let init = web_sys::RequestInit::new();
    init.set_signal(Some(&signal));
    let raw_request: web_sys::Request = request.into();
    let result = JsFuture::from(window.fetch_with_request_and_init(&raw_request, &init))
        .await
        .map_err(|_| GlooError::GlooError("the request failed".to_owned()))
        .and_then(|value| {
            value
                .dyn_into::<web_sys::Response>()
                .map(Response::from)
                .map_err(|_| {
                    GlooError::GlooError("the server returned an invalid response".to_owned())
                })
        });
    if let Some(timeout) = timeout {
        window.clear_timeout_with_handle(timeout);
    }
    drop(callback);
    result
}

/// Return an API URL that works behind the production same-origin proxy and
/// during local development with Trunk on :8080 and the API on :3000.
pub fn api_url(path: &str) -> String {
    let Some(window) = web_sys::window() else {
        return path.to_owned();
    };
    let location = window.location();
    // Browsers accept a trailing dot in local hostnames (`localhost.`), but it
    // should not make local development fall back to the SPA route.
    let raw_hostname = location.hostname().unwrap_or_default().to_ascii_lowercase();
    let hostname = raw_hostname.trim_end_matches('.').to_owned();
    let port = location.port().unwrap_or_default();
    let local_trunk = matches!(
        hostname.as_str(),
        "localhost" | "127.0.0.1" | "::1" | "[::1]"
    ) && !port.is_empty()
        && port != "80"
        && port != "443"
        && port != "3000";

    if local_trunk {
        // Keep a trailing dot such as `localhost.` intact. Browsers treat it
        // as a distinct cookie host, so stripping it here can hide a valid
        // session cookie from the authenticated API requests.
        let api_hostname = if raw_hostname.ends_with('.') {
            raw_hostname
        } else if hostname == "::1" {
            "[::1]".to_owned()
        } else {
            hostname
        };
        format!("http://{api_hostname}:3000{path}")
    } else {
        path.to_owned()
    }
}
