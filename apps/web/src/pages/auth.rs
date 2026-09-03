use leptos::prelude::*;

#[component]
fn AuthShell(
    title: &'static str,
    body: &'static str,
    page: &'static str,
    cta: &'static str,
    alt: &'static str,
    alt_href: &'static str,
) -> impl IntoView {
    view! {
        <main class="min-h-screen grid place-items-center px-6">
            <div class="w-full max-w-sm">
                <a href="/" class="font-handwriting text-4xl block mb-6">
                    "Task Space"
                </a>
                <div class="rounded-md border border-ink-soft/10 bg-paper-shelf/60 p-6 shadow-sm">
                    <h1 class="font-handwriting text-4xl">
                        {title}
                    </h1>
                    <p class="mt-2 text-sm text-ink-soft">
                        {body}
                    </p>
                    <div class="mt-4 rounded-[3px] bg-note-yellow text-note-ink-yellow px-3 py-2 text-sm rotate-[-0.5deg]">
                        "forms land in M0.6, auth wiring in M5"
                    </div>
                    <a href=page class="block text-center mt-4 rounded-[3px] bg-marker text-ink px-4 py-2 text-sm font-medium">
                        {cta}
                    </a>
                    <div class="mt-4 text-center text-sm text-ink-soft">
                        <a href=alt_href class="underline">
                            {alt}
                        </a>
                    </div>
                </div>
                <a href="/" class="mt-6 block text-center text-sm text-ink-soft underline">
                    "back to the landing page"
                </a>
            </div>
        </main>
    }
}

#[component]
pub fn SignIn() -> impl IntoView {
    view! {
        <AuthShell
            title="welcome back"
            body="Sign in to sync your spaces across devices (Pro). Your local board never needs an account."
            page="/app"
            cta="continue"
            alt="no account yet? sign up"
            alt_href="/signup"
        />
    }
}

#[component]
pub fn SignUp() -> impl IntoView {
    view! {
        <AuthShell
            title="new here?"
            body="Create an account to try multi-device sync. Free local use stays free, always."
            page="/app"
            cta="continue"
            alt="already have an account? sign in"
            alt_href="/signin"
        />
    }
}

#[component]
pub fn ForgotPassword() -> impl IntoView {
    view! {
        <AuthShell
            title="forgot the pin?"
            body="You'll get a password reset link. We can't wait to pin you back to your board."
            page="/signin"
            cta="send reset link"
            alt="remembered it? sign in"
            alt_href="/signin"
        />
    }
}
