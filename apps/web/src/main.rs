mod pages;

use leptos::mount::mount_to_body;
use leptos::prelude::*;
use leptos_router::components::{Route, Router, Routes};
use leptos_router::path;
use pages::auth::{ForgotPassword, ResetPassword, SignIn, SignUp, VerifyEmail};
use pages::board::Board;
use pages::home::Home;

// Bump this marker when a static runtime/proxy fix needs to invalidate an
// otherwise immutable browser asset without changing the visible UI.
const BUILD_MARKER: &str = "2026-09-08-wasm-proxy-fix";

#[component]
pub fn App() -> impl IntoView {
    view! {
        <div data-build-marker=BUILD_MARKER>
            <Router>
                <Routes fallback=|| {
                    view! {
                        <main class="min-h-screen grid place-items-center">
                            <div class="text-center">
                                <h1 class="font-handwriting text-6xl">"hmm, that page is not in the pile"</h1>
                                <a href="/" class="underline text-ink-soft">
                                    "back to the board"
                                </a>
                            </div>
                        </main>
                    }
                }>
                    <Route path=path!("/") view=Home/>
                    <Route path=path!("/app") view=Board/>
                    <Route path=path!("/signin") view=SignIn/>
                    <Route path=path!("/signup") view=SignUp/>
                    <Route path=path!("/forgot-password") view=ForgotPassword/>
                    <Route path=path!("/reset-password") view=ResetPassword/>
                    <Route path=path!("/verify-email") view=VerifyEmail/>
                </Routes>
            </Router>
        </div>
    }
}

fn main() {
    console_error_panic_hook::set_once();
    mount_to_body(App);
}
