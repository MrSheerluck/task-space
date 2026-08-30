mod pages;

use leptos::mount::mount_to_body;
use leptos::prelude::*;
use leptos_router::components::{Route, Router, Routes};
use leptos_router::path;
use pages::auth::{ForgotPassword, SignIn, SignUp};
use pages::board::Board;
use pages::home::Home;

#[component]
pub fn App() -> impl IntoView {
    view! {
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
            </Routes>
        </Router>
    }
}

fn main() {
    console_error_panic_hook::set_once();
    mount_to_body(App);
}
