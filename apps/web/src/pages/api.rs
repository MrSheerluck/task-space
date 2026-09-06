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
    let local_trunk = matches!(hostname.as_str(), "localhost" | "127.0.0.1" | "[::1]")
        && !port.is_empty()
        && port != "80"
        && port != "443"
        && port != "3000";

    if local_trunk {
        // Keep a trailing dot such as `localhost.` intact. Browsers treat it
        // as a distinct cookie host, so stripping it here can hide a valid
        // session cookie from the authenticated API requests.
        let api_hostname = if raw_hostname.ends_with('.') {
            raw_hostname
        } else {
            hostname
        };
        format!("http://{api_hostname}:3000{path}")
    } else {
        path.to_owned()
    }
}
