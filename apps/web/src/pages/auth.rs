use gloo_net::http::Request;
use leptos::ev::SubmitEvent;
use leptos::prelude::*;
use leptos::task::spawn_local;
use serde::{Deserialize, Serialize};
use web_sys::{RequestCredentials, UrlSearchParams};

use super::account::{load_account_state, remember_authenticated_session};
use super::api::api_url;

const PENDING_AUTH_STORAGE_KEY: &str = "task_space_pending_auth";

#[derive(Clone, Debug, Default, Deserialize)]
struct AuthApiResponse {
    status: String,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    pending_authentication_token: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct PasswordPayload {
    email: String,
    password: String,
}

#[derive(Clone, Debug, Serialize)]
struct ResetRequestPayload {
    email: String,
}

#[derive(Clone, Debug, Serialize)]
struct ResetConfirmationPayload {
    token: String,
    new_password: String,
}

#[derive(Clone, Debug, Serialize)]
struct VerificationPayload {
    code: String,
    pending_authentication_token: Option<String>,
}

async fn post_auth<T: Serialize>(path: &str, payload: &T) -> Result<AuthApiResponse, String> {
    let builder = Request::post(&api_url(path))
        .credentials(RequestCredentials::Include)
        .json(payload)
        .map_err(|_| "the form could not be sent".to_owned())?;
    let response = builder
        .send()
        .await
        .map_err(|_| "the server could not be reached".to_owned())?;
    let status = response.status();
    let body = response
        .json::<AuthApiResponse>()
        .await
        .map_err(|_| "the server returned an unexpected response".to_owned())?;
    if status >= 400 {
        Err(body
            .message
            .unwrap_or_else(|| "something went wrong; please try again".to_owned()))
    } else {
        Ok(body)
    }
}

fn redirect(path: &str) {
    if let Some(window) = web_sys::window() {
        let _ = window.location().set_href(path);
    }
}

fn redirect_if_authenticated() {
    spawn_local(async {
        if load_account_state().await.is_authenticated() {
            redirect("/app");
        }
    });
}

fn remember_pending_authentication_token(response: &AuthApiResponse) {
    let Some(token) = response.pending_authentication_token.as_deref() else {
        return;
    };
    let Some(window) = web_sys::window() else {
        return;
    };
    if let Ok(Some(storage)) = window.session_storage() {
        let _ = storage.set_item(PENDING_AUTH_STORAGE_KEY, token);
    }
}

fn pending_authentication_token() -> Option<String> {
    let window = web_sys::window()?;
    let storage = window.session_storage().ok()??;
    storage.get_item(PENDING_AUTH_STORAGE_KEY).ok()?
}

fn forget_pending_authentication_token() {
    if let Some(window) = web_sys::window()
        && let Ok(Some(storage)) = window.session_storage()
    {
        let _ = storage.remove_item(PENDING_AUTH_STORAGE_KEY);
    }
}

#[component]
fn AuthFrame(title: &'static str, body: &'static str, children: Children) -> impl IntoView {
    view! {
        <main class="min-h-screen grid place-items-center px-6 py-10">
            <div class="w-full max-w-sm">
                <a href="/" class="mb-6 flex items-center gap-2 font-handwriting text-4xl">
                    <span class="grid h-8 w-8 place-items-center bg-ink font-sans text-lg text-paper">"S"</span>
                    <span>"Task Space"</span>
                </a>
                <div class="rounded-md border border-ink-soft/10 bg-paper-shelf/60 p-6 shadow-sm">
                    <h1 class="font-handwriting text-4xl">{title}</h1>
                    <p class="mt-2 text-sm text-ink-soft">{body}</p>
                    {children()}
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
    let email = RwSignal::new(String::new());
    let password = RwSignal::new(String::new());
    let error = RwSignal::new(None::<String>);
    let submitting = RwSignal::new(false);
    redirect_if_authenticated();

    let submit = move |event: SubmitEvent| {
        event.prevent_default();
        if submitting.get_untracked() {
            return;
        }
        error.set(None);
        submitting.set(true);
        let payload = PasswordPayload {
            email: email.get_untracked().trim().to_owned(),
            password: password.get_untracked(),
        };
        spawn_local(async move {
            match post_auth("/auth/sign-in", &payload).await {
                Ok(response) if response.status == "authenticated" => {
                    remember_authenticated_session();
                    redirect("/app")
                }
                Ok(response) if response.status == "verification_required" => {
                    remember_pending_authentication_token(&response);
                    redirect("/verify-email")
                }
                Ok(response) => error.set(response.message),
                Err(message) => error.set(Some(message)),
            }
            submitting.set(false);
        });
    };

    view! {
        <AuthFrame
            title="welcome back"
            body="Sign in to sync your spaces across devices. Your local board never needs an account."
        >
            <form class="mt-5 space-y-4" on:submit=submit>
                <label class="block text-sm text-ink-soft">
                    "email"
                    <input
                        class="mt-1 w-full rounded-[3px] border border-ink-soft/25 bg-paper px-3 py-2 text-ink outline-none focus:border-ink"
                        type="email"
                        autocomplete="email"
                        required=true
                        placeholder="you@example.com"
                        on:input=move |event| email.set(event_target_value(&event))
                    />
                </label>
                <label class="block text-sm text-ink-soft">
                    "password"
                    <input
                        class="mt-1 w-full rounded-[3px] border border-ink-soft/25 bg-paper px-3 py-2 text-ink outline-none focus:border-ink"
                        type="password"
                        autocomplete="current-password"
                        required=true
                        minlength="10"
                        on:input=move |event| password.set(event_target_value(&event))
                    />
                </label>
                {move || error.get().map(|message| view! {
                    <p class="rounded-[3px] bg-note-pink px-3 py-2 text-sm text-note-ink-pink">{message}</p>
                })}
                <button
                    class="w-full rounded-[3px] bg-marker px-4 py-2 text-sm font-medium text-ink hover:brightness-95 disabled:opacity-60"
                    type="submit"
                    disabled=move || submitting.get()
                >
                    {move || if submitting.get() { "checking..." } else { "sign in" }}
                </button>
            </form>
            <a href="/forgot-password" class="mt-4 block text-center text-xs text-ink-soft underline">
                "forgot your password?"
            </a>
            <div class="mt-4 text-center text-sm text-ink-soft">
                <a href="/signup" class="underline">"no account yet? sign up"</a>
            </div>
        </AuthFrame>
    }
}

#[component]
pub fn SignUp() -> impl IntoView {
    let email = RwSignal::new(String::new());
    let password = RwSignal::new(String::new());
    let confirmation = RwSignal::new(String::new());
    let error = RwSignal::new(None::<String>);
    let submitting = RwSignal::new(false);
    redirect_if_authenticated();

    let submit = move |event: SubmitEvent| {
        event.prevent_default();
        if submitting.get_untracked() {
            return;
        }
        if password.get_untracked() != confirmation.get_untracked() {
            error.set(Some("the passwords do not match".to_owned()));
            return;
        }
        error.set(None);
        submitting.set(true);
        let payload = PasswordPayload {
            email: email.get_untracked().trim().to_owned(),
            password: password.get_untracked(),
        };
        spawn_local(async move {
            match post_auth("/auth/sign-up", &payload).await {
                Ok(response) if response.status == "authenticated" => {
                    remember_authenticated_session();
                    redirect("/app")
                }
                Ok(response) if response.status == "verification_required" => {
                    remember_pending_authentication_token(&response);
                    redirect("/verify-email")
                }
                Ok(response) => error.set(response.message),
                Err(message) => error.set(Some(message)),
            }
            submitting.set(false);
        });
    };

    view! {
        <AuthFrame
            title="new here?"
            body="Create an account to try multi-device sync. Free local use stays free, always."
        >
            <form class="mt-5 space-y-4" on:submit=submit>
                <label class="block text-sm text-ink-soft">
                    "email"
                    <input
                        class="mt-1 w-full rounded-[3px] border border-ink-soft/25 bg-paper px-3 py-2 text-ink outline-none focus:border-ink"
                        type="email"
                        autocomplete="email"
                        required=true
                        placeholder="you@example.com"
                        on:input=move |event| email.set(event_target_value(&event))
                    />
                </label>
                <label class="block text-sm text-ink-soft">
                    "password"
                    <input
                        class="mt-1 w-full rounded-[3px] border border-ink-soft/25 bg-paper px-3 py-2 text-ink outline-none focus:border-ink"
                        type="password"
                        autocomplete="new-password"
                        required=true
                        minlength="10"
                        on:input=move |event| password.set(event_target_value(&event))
                    />
                </label>
                <label class="block text-sm text-ink-soft">
                    "repeat password"
                    <input
                        class="mt-1 w-full rounded-[3px] border border-ink-soft/25 bg-paper px-3 py-2 text-ink outline-none focus:border-ink"
                        type="password"
                        autocomplete="new-password"
                        required=true
                        minlength="10"
                        on:input=move |event| confirmation.set(event_target_value(&event))
                    />
                </label>
                {move || error.get().map(|message| view! {
                    <p class="rounded-[3px] bg-note-pink px-3 py-2 text-sm text-note-ink-pink">{message}</p>
                })}
                <button
                    class="w-full rounded-[3px] bg-marker px-4 py-2 text-sm font-medium text-ink hover:brightness-95 disabled:opacity-60"
                    type="submit"
                    disabled=move || submitting.get()
                >
                    {move || if submitting.get() { "creating..." } else { "create account" }}
                </button>
            </form>
            <div class="mt-4 text-center text-sm text-ink-soft">
                <a href="/signin" class="underline">"already have an account? sign in"</a>
            </div>
        </AuthFrame>
    }
}

#[component]
pub fn ForgotPassword() -> impl IntoView {
    let email = RwSignal::new(String::new());
    let error = RwSignal::new(None::<String>);
    let message = RwSignal::new(None::<String>);
    let submitting = RwSignal::new(false);

    let submit = move |event: SubmitEvent| {
        event.prevent_default();
        if submitting.get_untracked() {
            return;
        }
        error.set(None);
        message.set(None);
        submitting.set(true);
        let payload = ResetRequestPayload {
            email: email.get_untracked().trim().to_owned(),
        };
        spawn_local(async move {
            match post_auth("/auth/password-reset", &payload).await {
                Ok(response) => message.set(response.message),
                Err(value) => error.set(Some(value)),
            }
            submitting.set(false);
        });
    };

    view! {
        <AuthFrame
            title="forgot the pin?"
            body="Enter your email and we'll send a link to choose a new password."
        >
            <form class="mt-5 space-y-4" on:submit=submit>
                <label class="block text-sm text-ink-soft">
                    "email"
                    <input
                        class="mt-1 w-full rounded-[3px] border border-ink-soft/25 bg-paper px-3 py-2 text-ink outline-none focus:border-ink"
                        type="email"
                        autocomplete="email"
                        required=true
                        placeholder="you@example.com"
                        on:input=move |event| email.set(event_target_value(&event))
                    />
                </label>
                {move || error.get().map(|value| view! {
                    <p class="rounded-[3px] bg-note-pink px-3 py-2 text-sm text-note-ink-pink">{value}</p>
                })}
                {move || message.get().map(|value| view! {
                    <p class="rounded-[3px] bg-note-green px-3 py-2 text-sm text-note-ink-green">{value}</p>
                })}
                <button
                    class="w-full rounded-[3px] bg-marker px-4 py-2 text-sm font-medium text-ink hover:brightness-95 disabled:opacity-60"
                    type="submit"
                    disabled=move || submitting.get()
                >
                    {move || if submitting.get() { "sending..." } else { "send reset link" }}
                </button>
            </form>
            <div class="mt-4 text-center text-sm text-ink-soft">
                <a href="/signin" class="underline">"remembered it? sign in"</a>
            </div>
        </AuthFrame>
    }
}

#[component]
pub fn ResetPassword() -> impl IntoView {
    let token = query_parameter("token").unwrap_or_default();
    let password = RwSignal::new(String::new());
    let confirmation = RwSignal::new(String::new());
    let error = RwSignal::new(None::<String>);
    let submitting = RwSignal::new(false);

    let submit = move |event: SubmitEvent| {
        event.prevent_default();
        if submitting.get_untracked() {
            return;
        }
        if token.is_empty() {
            error.set(Some("this reset link is missing its token".to_owned()));
            return;
        }
        if password.get_untracked() != confirmation.get_untracked() {
            error.set(Some("the passwords do not match".to_owned()));
            return;
        }
        error.set(None);
        submitting.set(true);
        let payload = ResetConfirmationPayload {
            token: token.clone(),
            new_password: password.get_untracked(),
        };
        spawn_local(async move {
            match post_auth("/auth/password-reset/confirm", &payload).await {
                Ok(_) => redirect("/signin"),
                Err(value) => {
                    error.set(Some(value));
                    submitting.set(false);
                }
            }
        });
    };

    view! {
        <AuthFrame
            title="a fresh pin"
            body="Choose a new password for your Task Space account."
        >
            <form class="mt-5 space-y-4" on:submit=submit>
                <label class="block text-sm text-ink-soft">
                    "new password"
                    <input
                        class="mt-1 w-full rounded-[3px] border border-ink-soft/25 bg-paper px-3 py-2 text-ink outline-none focus:border-ink"
                        type="password"
                        autocomplete="new-password"
                        required=true
                        minlength="10"
                        on:input=move |event| password.set(event_target_value(&event))
                    />
                </label>
                <label class="block text-sm text-ink-soft">
                    "repeat password"
                    <input
                        class="mt-1 w-full rounded-[3px] border border-ink-soft/25 bg-paper px-3 py-2 text-ink outline-none focus:border-ink"
                        type="password"
                        autocomplete="new-password"
                        required=true
                        minlength="10"
                        on:input=move |event| confirmation.set(event_target_value(&event))
                    />
                </label>
                {move || error.get().map(|value| view! {
                    <p class="rounded-[3px] bg-note-pink px-3 py-2 text-sm text-note-ink-pink">{value}</p>
                })}
                <button
                    class="w-full rounded-[3px] bg-marker px-4 py-2 text-sm font-medium text-ink hover:brightness-95 disabled:opacity-60"
                    type="submit"
                    disabled=move || submitting.get()
                >
                    {move || if submitting.get() { "saving..." } else { "set new password" }}
                </button>
            </form>
        </AuthFrame>
    }
}

#[component]
pub fn VerifyEmail() -> impl IntoView {
    let code = RwSignal::new(String::new());
    let error = RwSignal::new(None::<String>);
    let submitting = RwSignal::new(false);

    let submit = move |event: SubmitEvent| {
        event.prevent_default();
        if submitting.get_untracked() {
            return;
        }
        error.set(None);
        submitting.set(true);
        let payload = VerificationPayload {
            code: code.get_untracked().trim().to_owned(),
            pending_authentication_token: pending_authentication_token(),
        };
        spawn_local(async move {
            match post_auth("/auth/verify-email", &payload).await {
                Ok(_) => {
                    remember_authenticated_session();
                    forget_pending_authentication_token();
                    redirect("/app")
                }
                Err(value) => {
                    error.set(Some(value));
                    submitting.set(false);
                }
            }
        });
    };

    view! {
        <AuthFrame
            title="one last scribble"
            body="Enter the six-digit code we sent to your email to verify your account."
        >
            <form class="mt-5 space-y-4" on:submit=submit>
                <label class="block text-sm text-ink-soft">
                    "verification code"
                    <input
                        class="mt-1 w-full rounded-[3px] border border-ink-soft/25 bg-paper px-3 py-2 text-center tracking-[0.35em] text-ink outline-none focus:border-ink"
                        type="text"
                        inputmode="numeric"
                        autocomplete="one-time-code"
                        maxlength="6"
                        required=true
                        on:input=move |event| code.set(event_target_value(&event))
                    />
                </label>
                {move || error.get().map(|value| view! {
                    <p class="rounded-[3px] bg-note-pink px-3 py-2 text-sm text-note-ink-pink">{value}</p>
                })}
                <button
                    class="w-full rounded-[3px] bg-marker px-4 py-2 text-sm font-medium text-ink hover:brightness-95 disabled:opacity-60"
                    type="submit"
                    disabled=move || submitting.get()
                >
                    {move || if submitting.get() { "verifying..." } else { "verify email" }}
                </button>
            </form>
            <div class="mt-4 text-center text-sm text-ink-soft">
                <a href="/signin" class="underline">"back to sign in"</a>
            </div>
        </AuthFrame>
    }
}

fn query_parameter(name: &str) -> Option<String> {
    let search = web_sys::window()?.location().search().ok()?;
    UrlSearchParams::new_with_str(&search).ok()?.get(name)
}
