#!/usr/bin/env python3
"""Headless local-first sync smoke test.

Without a session cookie, this proves the local-first half of the contract:
edits survive the network disappearing, tabs exchange actual local updates,
the durable IndexedDB outbox is non-empty while offline, and a fresh reload
rehydrates the same board. A second context disables BroadcastChannel and Web
Locks to exercise the storage-event/lease fallback. It also verifies that a
corrupt canonical snapshot fails closed and surfaces the repair state instead
of being reported as saved. With a real entitled session cookie, the same
script can exercise authenticated outbox acknowledgement and two isolated
browser profiles against the server.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from playwright.sync_api import BrowserContext, Page, TimeoutError, sync_playwright


def wait_for_board(page: Page, authenticated: bool = False) -> None:
    page.locator("#task-space-board").wait_for(state="visible", timeout=15_000)
    page.get_by_text("Task Space", exact=True).wait_for(state="visible", timeout=15_000)
    # Finish the unauthenticated account check before taking the context
    # offline. Otherwise the test can intentionally interrupt that request
    # and leave the board behind its temporary "account check unavailable"
    # safety screen.
    if authenticated:
        try:
            page.get_by_role("button", name="sign out", exact=True).wait_for(
                state="visible", timeout=20_000
            )
        except TimeoutError as error:
            body = page.locator("body").inner_text(timeout=2_000)
            raise RuntimeError(
                "authenticated account check did not settle; visible page text:\n"
                + body[:4_000]
            ) from error
    else:
        try:
            page.get_by_role("link", name="sign in", exact=True).wait_for(
                state="visible", timeout=15_000
            )
        except TimeoutError as error:
            body = page.locator("body").inner_text(timeout=2_000)
            raise RuntimeError(
                "guest account check did not settle; visible page text:\n" + body[:4_000]
            ) from error


def add_note(page: Page, text: str) -> None:
    page.get_by_role("button", name="+ new note", exact=True).click()
    editor = page.get_by_role("textbox", name="Edit task")
    editor.wait_for(state="visible", timeout=5_000)
    editor.fill(text)
    # A converged sibling tab can already have a note at the default
    # placement, so its card may visually overlap the editor's finish button.
    # The button is still the correct control; force only bypasses that test
    # geometry overlap.
    page.get_by_role("button", name="done editing", exact=True).click(force=True)


def wait_for_text(page: Page, text: str, timeout: int = 15_000) -> None:
    page.get_by_text(text, exact=True).wait_for(state="visible", timeout=timeout)


def record_network_response(response: Any, network: list[str]) -> None:
    if not any(path in response.url for path in ("/auth/", "/account/", "/sync/")):
        return
    message = f"{response.status} {response.url}"
    if response.status >= 400:
        try:
            message += f" body={response.text()[:500]}"
        except Exception as error:  # pragma: no cover - diagnostic fallback
            message += f" body-read-failed={error}"
    elif response.status == 200 and (
        "/sync/v2/" in response.url or response.url.endswith("/sync/pull")
    ):
        # Keep diagnostics bounded and document-free. These fields tell us
        # whether a successful transport response was actually a usable
        # reconcile/pull payload without logging encoded CRDT content.
        try:
            payload = response.json()
            if isinstance(payload, dict):
                summary: dict[str, Any] = {}
                for key, value in payload.items():
                    if key in {"update", "state_vector"} and isinstance(value, str):
                        summary[f"{key}Chars"] = len(value)
                    elif isinstance(value, list):
                        summary[key] = {"count": len(value)}
                    elif isinstance(value, (str, int, float, bool)) or value is None:
                        summary[key] = value
                message += f" summary={summary}"
        except Exception as error:  # pragma: no cover - diagnostic fallback
            message += f" json-read-failed={error}"
    network.append(message)


def read_sync_records(page: Page) -> list[dict[str, Any]]:
    return page.evaluate(
        """
        () => new Promise((resolve, reject) => {
          const request = indexedDB.open("task-space");
          request.onerror = () => reject(request.error || new Error("IndexedDB open failed"));
          request.onsuccess = () => {
            const db = request.result;
            if (!db.objectStoreNames.contains("sync-records")) {
              resolve([]);
              return;
            }
            const transaction = db.transaction("sync-records", "readonly");
            const read = transaction.objectStore("sync-records").getAll();
            read.onerror = () => reject(read.error || new Error("IndexedDB read failed"));
            read.onsuccess = () => resolve(read.result || []);
          };
        })
        """
    )


def seed_session_cookie(
    context: BrowserContext, app_url: str, session_cookie: str | None
) -> None:
    if not session_cookie:
        return
    parsed_url = urlsplit(app_url)
    if not parsed_url.hostname:
        raise ValueError(f"authenticated browser URL has no hostname: {app_url}")
    context.add_cookies(
        [
            {
                "name": "task_space_session",
                "value": session_cookie,
                "domain": parsed_url.hostname,
                "path": "/",
                "secure": parsed_url.scheme == "https",
                "httpOnly": True,
            }
        ]
    )


def seed_authenticated_namespace(context: BrowserContext, account_id: str | None) -> None:
    """Start a disposable authenticated run in its account namespace.

    Real sign-in flows populate these markers before redirecting to `/app`.
    The deterministic browser acceptance server seeds the cookie directly, so
    it supplies the same post-login marker to avoid exercising guest-adoption
    UI during a server-convergence test.
    """
    if not account_id or not account_id.strip():
        return
    encoded_account_id = json.dumps(account_id)
    context.add_init_script(
        f"""
        try {{
          localStorage.setItem("task_space_authenticated_session", "true");
          localStorage.setItem("task_space_active_account_id", {encoded_account_id});
        }} catch (_) {{}}
        """
    )


def wait_for_authenticated_outbox_empty(
    page: Page, timeout: int = 30_000, diagnostics: list[str] | None = None
) -> None:
    def read_active_queues() -> dict[str, Any]:
        return page.evaluate(
            """
            async () => {
              const moduleUrl = performance.getEntriesByType("resource")
                .map((entry) => entry.name)
                .find((url) => /\/snippets\/[^/]+\/inline0\.js$/.test(url));
              if (!moduleUrl) throw new Error("could not locate sync IndexedDB module");
              const syncModule = await import(moduleUrl);
              return {
                crdt: await syncModule.taskSpaceLoadCrdtUpdates(),
                metadata: await syncModule.taskSpaceLoadMetadataUpdates(),
              };
            }
            """
        )

    deadline = time.monotonic() + timeout / 1_000
    last_records: list[dict[str, Any]] = []
    last_queues: dict[str, Any] = {}
    while time.monotonic() < deadline:
        last_records = read_sync_records(page)
        last_queues = read_active_queues()
        account_records = [
            record
            for record in last_records
            if str(record.get("principal") or "").startswith("account:")
        ]
        pending = len(last_queues.get("crdt") or []) + len(last_queues.get("metadata") or [])
        if pending == 0 and account_records:
            # IndexedDB acknowledgement alone is not enough: the browser
            # must also expose the server-confirmed state to the user.
            try:
                wait_for_text(page, "synced", timeout=timeout)
            except TimeoutError as error:
                raise AssertionError(
                    "authenticated outbox is empty but the UI did not report synced;\n"
                    + "body:\n"
                    + page.locator("body").inner_text(timeout=2_000)[:4_000]
                    + "\nactive queues:\n"
                    + repr(last_queues)
                    + ("\nnetwork:\n" + "\n".join(diagnostics[-40:]) if diagnostics else "")
                ) from error
            return
        page.wait_for_timeout(500)
    raise AssertionError(
        "authenticated reconnect did not acknowledge the local outbox: "
        + repr(summarize_sync_records(last_records))
        + "\nactive queues: "
        + repr(last_queues)
        + ("\nnetwork:\n" + "\n".join(diagnostics[-40:]) if diagnostics else "")
    )


def read_workspace_record(page: Page, principal: str) -> Any:
    return page.evaluate(
        """
        (key) => new Promise((resolve, reject) => {
          const request = indexedDB.open("task-space");
          request.onerror = () => reject(request.error || new Error("IndexedDB open failed"));
          request.onsuccess = () => {
            const db = request.result;
            if (!db.objectStoreNames.contains("workspace")) {
              resolve(null);
              return;
            }
            const read = db.transaction("workspace", "readonly")
              .objectStore("workspace").get(key);
            read.onerror = () => reject(read.error || new Error("IndexedDB read failed"));
            read.onsuccess = () => resolve(read.result || null);
          };
        })
        """,
        f"{principal}:current",
    )


def assert_inbox_ack_is_scoped(page: Page) -> None:
    result = page.evaluate(
        """
        async () => {
          const moduleUrl = performance.getEntriesByType("resource")
            .map((entry) => entry.name)
            .find((url) => /\/snippets\/[^/]+\/inline0\.js$/.test(url));
          if (!moduleUrl) throw new Error("could not locate sync IndexedDB module");
          const syncModule = await import(moduleUrl);
          const openDb = () => new Promise((resolve, reject) => {
            const request = indexedDB.open("task-space");
            request.onerror = () => reject(request.error || new Error("IndexedDB open failed"));
            request.onsuccess = () => resolve(request.result);
          });
          const readRecords = async () => {
            const db = await openDb();
            return await new Promise((resolve, reject) => {
              const request = db.transaction("sync-records", "readonly")
                .objectStore("sync-records").getAll();
              request.onerror = () => reject(request.error || new Error("sync-records read failed"));
              request.onsuccess = () => resolve(request.result || []);
            });
          };
          const records = await readRecords();
          const record = records.find((item) => item && item.snapshot && item.principal);
          if (!record) throw new Error("no canonical sync record available for inbox test");
          const source = `browser-smoke-inbox-${crypto.randomUUID()}`;
          const acknowledgedGeneration = 7001;
          const retainedGeneration = 7002;
          await syncModule.taskSpaceSetSyncPrincipal(record.principal);
          const originalWorkspaceRaw = await syncModule.taskSpaceLoadWorkspace();
          const originalWorkspace = JSON.parse(originalWorkspaceRaw);
          const manifestProjection = {
            ...originalWorkspace,
            spaces: (originalWorkspace.spaces || []).map((space) => ({
              ...space,
              updated_at: Date.now() + 10_000,
              board: { schema_version: 3, notes: [], groups: [], tombstones: [] },
            })),
          };
          await syncModule.taskSpaceSaveWorkspace(
            JSON.stringify(manifestProjection), record.principal,
          );
          const mergedWorkspace = JSON.parse(await syncModule.taskSpaceLoadWorkspace());
          const originalBoardItems = (originalWorkspace.spaces || [])
            .reduce((count, space) => count + (space.board?.notes?.length || 0)
              + (space.board?.groups?.length || 0), 0);
          const mergedBoardItems = (mergedWorkspace.spaces || [])
            .reduce((count, space) => count + (space.board?.notes?.length || 0)
              + (space.board?.groups?.length || 0), 0);
          if (originalBoardItems > 0 && mergedBoardItems === 0) {
            throw new Error("empty manifest projection erased populated local boards");
          }
          await syncModule.taskSpaceSaveWorkspace(originalWorkspaceRaw, record.principal);
          await syncModule.taskSpaceQueueIncomingCrdtUpdate(
            record.principal, record.spaceId, source, acknowledgedGeneration, "AA",
          );
          await syncModule.taskSpaceQueueIncomingCrdtUpdate(
            record.principal, record.spaceId, source, retainedGeneration, "AQ",
          );
          await syncModule.taskSpaceSaveCrdt(
            record.principal,
            record.spaceId,
            record.snapshot,
            record.localGeneration,
            source,
            acknowledgedGeneration,
          );
          const db = await openDb();
          const keys = await new Promise((resolve, reject) => {
            const request = db.transaction("crdt-inbox", "readonly")
              .objectStore("crdt-inbox").getAllKeys();
            request.onerror = () => reject(request.error || new Error("crdt-inbox read failed"));
            request.onsuccess = () => resolve(request.result || []);
          });
          const acknowledgedKey = `${record.principal}:inbox:${record.spaceId}:${source}:${acknowledgedGeneration}`;
          const retainedKey = `${record.principal}:inbox:${record.spaceId}:${source}:${retainedGeneration}`;
          await new Promise((resolve, reject) => {
            const transaction = db.transaction("crdt-inbox", "readwrite");
            transaction.objectStore("crdt-inbox").delete(retainedKey);
            transaction.onerror = () => reject(transaction.error || new Error("crdt-inbox cleanup failed"));
            transaction.oncomplete = () => resolve(true);
          });
          if (keys.includes(acknowledgedKey)) throw new Error("acknowledged inbox row was not removed");
          if (!keys.includes(retainedKey)) throw new Error("unrelated inbox row was removed");
          return { status: "ok", retained: true };
        }
        """
    )
    if result != {"status": "ok", "retained": True}:
        raise AssertionError(f"inbox acknowledgement regression failed: {result!r}")


def assert_coordinator_fencing_liveness(first: Page, second: Page) -> None:
    """Verify a suspended-style stale Web Lock can be recovered by a sibling."""
    principal = first.evaluate(
        "() => `browser-smoke-coordinator:${crypto.randomUUID()}`"
    )
    module_probe = """
      async ({principal, action}) => {
        const moduleUrl = performance.getEntriesByType("resource")
          .map((entry) => entry.name)
          .find((url) => /\/snippets\/[^/]+\/inline0\.js$/.test(url));
        if (!moduleUrl) throw new Error("could not locate sync transport module");
        const syncModule = await import(moduleUrl);
        if (action === "acquire") {
          return await syncModule.taskSpaceAcquireSyncLease(principal);
        }
        if (action === "owner") {
          return syncModule.taskSpaceIsSyncLeaseOwner(principal);
        }
        syncModule.taskSpaceReleaseSyncLease(principal);
        return true;
      }
    """
    first_acquired = first.evaluate(module_probe, {"principal": principal, "action": "acquire"})
    if first_acquired is not True:
        raise AssertionError(f"first coordinator could not acquire lease: {first_acquired!r}")

    second_blocked = second.evaluate(
        module_probe, {"principal": principal, "action": "acquire"}
    )
    if second_blocked is not False:
        first.evaluate(module_probe, {"principal": principal, "action": "release"})
        raise AssertionError(
            "second tab acquired an unexpired coordinator lease alongside the first"
        )

    lease_key = f"{principal}:sync-lease"
    second.evaluate(
        """
        (key) => {
          const current = JSON.parse(localStorage.getItem(key) || "null");
          localStorage.setItem(key, JSON.stringify({
            owner: "expired-test-owner",
            epoch: Number(current?.epoch || 0) + 1,
            expiresAt: Date.now() - 1,
          }));
        }
        """,
        lease_key,
    )
    second_takeover = second.evaluate(
        module_probe, {"principal": principal, "action": "acquire"}
    )
    stale_first_owner = first.evaluate(
        module_probe, {"principal": principal, "action": "owner"}
    )
    second_owner = second.evaluate(
        module_probe, {"principal": principal, "action": "owner"}
    )
    first.evaluate(module_probe, {"principal": principal, "action": "release"})
    second.evaluate(module_probe, {"principal": principal, "action": "release"})
    if second_takeover is not True or stale_first_owner is not False or second_owner is not True:
        raise AssertionError(
            "coordinator fencing did not recover cleanly: "
            f"takeover={second_takeover!r}, stale_first={stale_first_owner!r}, "
            f"second_owner={second_owner!r}"
        )


def corrupt_snapshot(page: Page, record: dict[str, Any]) -> None:
    page.evaluate(
        """
        ({recordKey, crdtKey}) => new Promise((resolve, reject) => {
          const request = indexedDB.open("task-space");
          request.onerror = () => reject(request.error || new Error("IndexedDB open failed"));
          request.onsuccess = () => {
            const db = request.result;
            const transaction = db.transaction(["sync-records", "crdt"], "readwrite");
            const records = transaction.objectStore("sync-records");
            const read = records.get(recordKey);
            read.onerror = () => reject(read.error || new Error("IndexedDB read failed"));
            read.onsuccess = () => {
              const value = read.result || {};
              value.snapshot = "not-base64";
              records.put(value, recordKey);
              transaction.objectStore("crdt").put("not-base64", crdtKey);
            };
            transaction.onerror = () => reject(transaction.error || new Error("IndexedDB write failed"));
            transaction.oncomplete = () => resolve(true);
          };
        })
        """,
        {
            "recordKey": f"{record['principal']}:space:{record['spaceId']}",
            "crdtKey": f"{record['principal']}:space:{record['spaceId']}",
        },
    )


def assert_local_storage_backup_export(page: Page, principal: str) -> None:
    try:
        with page.expect_download(timeout=10_000) as download_info:
            page.get_by_role("button", name="export local backup", exact=True).click()
        download = download_info.value
        backup_path = download.path()
        if backup_path is None:
            raise AssertionError("local backup download did not expose a temporary path")
        payload = json.loads(Path(backup_path).read_text())
    except TimeoutError as error:
        body = page.locator("body").inner_text(timeout=2_000)
        export_probe = page.evaluate(
            """
            async (principal) => {
              const moduleUrl = performance.getEntriesByType("resource")
                .map((entry) => entry.name)
                .find((url) => /\/snippets\/[^/]+\/inline0\.js$/.test(url));
              if (!moduleUrl) return { ok: false, error: "module not found" };
              try {
                const module = await import(moduleUrl);
                const raw = await module.taskSpaceExportIndexedDbBackup(principal);
                return { ok: true, length: String(raw || "").length };
              } catch (exportError) {
                return { ok: false, error: String(exportError) };
              }
            }
            """,
            principal,
        )
        raise AssertionError(
            "repair state did not expose a local backup download; visible page text:\n"
            + body[:4_000]
            + f"\nexport probe: {export_probe!r}"
        ) from error

    if payload.get("format") != "task-space-indexeddb-backup":
        raise AssertionError(f"unexpected local backup format: {payload!r}")
    if payload.get("principal") != principal:
        raise AssertionError(f"local backup principal mismatch: {payload!r}")
    records = payload.get("stores", {}).get("sync-records", [])
    if not any(
        entry.get("value", {}).get("snapshot") == "not-base64"
        for entry in records
        if isinstance(entry, dict) and isinstance(entry.get("value"), dict)
    ):
        raise AssertionError("local backup omitted the corrupt canonical sync record")


def assert_sync_diagnostics_export(page: Page) -> None:
    page.get_by_role("button", name="details", exact=True).dispatch_event("click")
    with page.expect_download(timeout=10_000) as download_info:
        page.get_by_role("button", name="export diagnostics", exact=True).dispatch_event("click")
    download = download_info.value
    path = download.path()
    if path is None:
        raise AssertionError("sync diagnostics download did not expose a temporary path")
    payload = json.loads(Path(path).read_text())
    if payload.get("format") != "task-space-sync-diagnostics":
        raise AssertionError(f"unexpected sync diagnostics format: {payload!r}")
    for key in ("principal", "activeSpaceId", "status", "pendingCount", "local", "transport"):
        if key not in payload:
            raise AssertionError(f"sync diagnostics omitted {key}: {payload!r}")
    for forbidden in ("notes", "groups", "snapshot", "workspace", "credentials"):
        if forbidden in payload:
            raise AssertionError(f"sync diagnostics leaked {forbidden}: {payload!r}")


def summarize_sync_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "principal": record.get("principal"),
            "spaceId": record.get("spaceId"),
            "snapshotChars": len(record.get("snapshot") or ""),
            "outbox": [
                {
                    "localGeneration": item.get("localGeneration"),
                    "updateChars": len(item.get("update") or ""),
                }
                for item in record.get("outbox") or []
            ],
        }
        for record in records
    ]


def assert_broadcast_channel_delivery(first: Page, second: Page) -> tuple[str | None, str]:
    records = read_sync_records(first)
    principal = records[0]["principal"] if records else "guest"
    # The app includes the namespace and principal in the channel name so a
    # principal switch cannot receive an older account's payloads.
    channel_name = f"1:{principal}:{principal}"
    storage_key = f"task-space.local-sync.v1:{principal}"
    has_broadcast_channel = second.evaluate("() => typeof BroadcastChannel === 'function'")
    if has_broadcast_channel:
        second.evaluate(
            """
            (name) => {
              window.__taskSpaceChannelProbe = new Promise((resolve) => {
                const channel = new BroadcastChannel(name);
                channel.onmessage = (event) => {
                  channel.close();
                  resolve(event.data);
                };
              });
            }
            """,
            channel_name,
        )
        first.evaluate(
            """
            (name) => {
              const channel = new BroadcastChannel(name);
              channel.postMessage("task-space-browser-smoke");
              channel.close();
            }
            """,
            channel_name,
        )
        delivered = second.evaluate(
            """
            async () => Promise.race([
              window.__taskSpaceChannelProbe,
              new Promise((resolve) => setTimeout(() => resolve(null), 3_000)),
            ])
            """
        )
    else:
        second.evaluate(
            """
            (key) => {
              window.__taskSpaceStorageProbe = new Promise((resolve) => {
                window.addEventListener("storage", (event) => {
                  if (event.key === key && event.newValue === "task-space-browser-smoke") {
                    resolve(event.newValue);
                  }
                }, { once: true });
              });
            }
            """,
            storage_key,
        )
        first.evaluate(
            """
            (key) => localStorage.setItem(key, "task-space-browser-smoke")
            """,
            storage_key,
        )
        delivered = second.evaluate(
            """
            async () => Promise.race([
              window.__taskSpaceStorageProbe,
              new Promise((resolve) => setTimeout(() => resolve(null), 3_000)),
            ])
            """
        )
    if delivered != "task-space-browser-smoke":
        transport = "BroadcastChannel" if has_broadcast_channel else "storage events"
        raise AssertionError(f"{transport} did not deliver: {delivered!r}")
    if has_broadcast_channel:
        second.evaluate(
            """
            (name) => {
              window.__taskSpaceChannelMessages = [];
              window.__taskSpaceChannelCapture = new BroadcastChannel(name);
              window.__taskSpaceChannelCapture.onmessage = (event) =>
                window.__taskSpaceChannelMessages.push(event.data);
            }
            """,
            channel_name,
        )
    else:
        second.evaluate(
            """
            (key) => {
              window.__taskSpaceStorageMessages = [];
              window.addEventListener("storage", (event) => {
                if (event.key === key && event.newValue) {
                  window.__taskSpaceStorageMessages.push(event.newValue);
                }
              });
            }
            """,
            storage_key,
        )
    return (channel_name if has_broadcast_channel else None, storage_key)


def assert_no_page_errors(errors: list[str], label: str) -> None:
    if errors:
        raise AssertionError(f"{label} reported page errors: {errors}")


def assert_expired_session_keeps_account_namespace(
    context: BrowserContext, app_url: str
) -> None:
    """Verify an expired remembered session does not fall back to an empty guest board."""
    context.add_init_script(
        """
        try {
          localStorage.setItem("task_space_authenticated_session", "true");
          localStorage.setItem("task_space_active_account_id", "expired-regression");
        } catch (_) {}
        """
    )
    if authenticated:
        seed_authenticated_namespace(
            context, os.environ.get("TASK_SPACE_AUTHENTICATED_ACCOUNT_ID")
        )
    if "ngrok" in urlsplit(app_url).netloc:
        context.set_extra_http_headers({"ngrok-skip-browser-warning": "1"})
    page = context.new_page()
    try:
        page.goto(app_url, wait_until="domcontentloaded")
        page.locator("#task-space-board").wait_for(state="visible", timeout=15_000)
        page.get_by_text("session expired", exact=False).first.wait_for(
            state="visible", timeout=25_000
        )
        page.get_by_role("link", name="sign in again", exact=True).first.wait_for(
            state="visible", timeout=5_000
        )
        add_note(page, "expired session local edit")
        wait_for_text(page, "expired session local edit")
        records = read_sync_records(page)
        if not any(
            record.get("principal") == "account:expired-regression"
            for record in records
        ):
            raise AssertionError(
                "expired session reopened a non-account namespace: "
                + repr(summarize_sync_records(records))
            )
    finally:
        page.close()


def run_context(
    context: BrowserContext,
    app_url: str,
    label: str,
    authenticated: bool = False,
    session_cookie: str | None = None,
) -> None:
    errors: list[str] = []
    network: list[str] = []
    console_messages: list[str] = []
    dialogs: list[str] = []
    # Start with a stale refresh lease so the real account bootstrap exercises
    # takeover instead of only the uncontended path. This is intentionally
    # expired and contains no credential material.
    context.add_init_script(
        """
        try {
          localStorage.setItem(
            "task-space:auth-refresh-lock",
            JSON.stringify({ owner: "browser-smoke-stale-holder", expiresAt: Date.now() - 1 })
          );
        } catch (_) {}
        """
    )
    # Fresh browser profiles may stop at a tunnel interstitial before the
    # application is requested. Named Cloudflare tunnels do not need a bypass
    # header; keep the provider-specific header only for legacy ngrok origins.
    if "ngrok" in urlsplit(app_url).netloc:
        context.set_extra_http_headers({"ngrok-skip-browser-warning": "1"})
    seed_session_cookie(context, app_url, session_cookie)
    first = context.new_page()
    second = context.new_page()
    fresh = context.new_page()
    for page in (first, second, fresh):
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.on(
            "console",
            lambda message: console_messages.append(f"{message.type}: {message.text}")
            if "task-space" in message.text.lower()
            else None,
        )
        page.on(
            "response",
            lambda response: record_network_response(response, network),
        )
        page.on(
            "requestfailed",
            lambda request: network.append(
                f"FAILED {request.url}: {request.failure}"
            )
            if any(path in request.url for path in ("/auth/", "/account/", "/sync/"))
            else None,
        )
        page.on(
            "dialog",
            lambda dialog: (
                dialogs.append(f"{dialog.type}: {dialog.message}"),
                dialog.dismiss(),
            ),
        )
        page.goto(app_url, wait_until="domcontentloaded")
        try:
            wait_for_board(page, authenticated)
        except RuntimeError as error:
            raise RuntimeError(f"{error}\nnetwork:\n" + "\n".join(network[-30:])) from error

    # Let the asynchronous IndexedDB hydration finish before starting the
    # offline interaction; the production path must also handle earlier edits,
    # but this keeps the tab-convergence assertion focused on replication.
    for page in (first, second, fresh):
        page.wait_for_timeout(5_000)

    assert_coordinator_fencing_liveness(first, second)
    context.set_offline(True)
    channel_name, storage_key = assert_broadcast_channel_delivery(first, second)
    add_note(first, f"{label} first tab")
    try:
        wait_for_text(second, f"{label} first tab")
    except TimeoutError as error:
        records = read_sync_records(first)
        candidate = max(records, key=lambda record: len(record.get("outbox") or []), default=None)
        candidate_principal = candidate["principal"] if candidate else "guest"
        focus_refresh_applied = False
        second.evaluate("() => window.dispatchEvent(new Event('focus'))")
        try:
            wait_for_text(second, f"{label} first tab", timeout=3_000)
            focus_refresh_applied = True
        except TimeoutError:
            pass
        synthetic_applied = False
        if candidate:
            queued = candidate["outbox"][-1]
            synthetic_payload = {
                    "protocolVersion": 1,
                    "principal": candidate["principal"],
                    "spaceId": candidate["spaceId"],
                    "localGeneration": queued.get("localGeneration", 0),
                    "originDeviceId": "synthetic-device",
                    "originTabId": "synthetic-tab",
                    "update": queued["update"],
            }
            if channel_name:
                second.evaluate(
                    """
                    ({name, payload}) => {
                      const channel = new BroadcastChannel(name);
                      channel.postMessage(JSON.stringify(payload));
                      channel.close();
                    }
                    """,
                    {"name": channel_name, "payload": synthetic_payload},
                )
            else:
                second.evaluate(
                    """
                    ({key, payload}) => localStorage.setItem(key, JSON.stringify(payload))
                    """,
                    {"key": storage_key, "payload": synthetic_payload},
                )
            try:
                wait_for_text(second, f"{label} first tab", timeout=3_000)
                synthetic_applied = True
            except TimeoutError:
                pass
        first_tab_id = first.evaluate(
            "() => sessionStorage.getItem('task-space.local-tab-id.v1')"
        )
        second_tab_id = second.evaluate(
            "() => sessionStorage.getItem('task-space.local-tab-id.v1')"
        )
        raise RuntimeError(
            f"{label} sibling tab did not receive first edit;\n"
            f"first records: {summarize_sync_records(records)!r}\n"
            f"second records: {summarize_sync_records(read_sync_records(second))!r}\n"
            f"workspace: {str(read_workspace_record(first, candidate_principal))[:2_000]}\n"
            f"transport messages: {second.evaluate('() => window.__taskSpaceChannelMessages || window.__taskSpaceStorageMessages || []')}\n"
            f"channel: {channel_name or storage_key}\n"
            f"tab ids: {first_tab_id} / {second_tab_id}\n"
            f"console: {console_messages[-30:]}\n"
            f"focus refresh applied: {focus_refresh_applied}\n"
            f"synthetic replay applied: {synthetic_applied}\n"
            f"second page: {second.locator('body').inner_text(timeout=2_000)[:4_000]}"
        ) from error

    add_note(second, f"{label} second tab")
    wait_for_text(first, f"{label} second tab")

    records = read_sync_records(first)
    pending = sum(len(record.get("outbox") or []) for record in records)
    if pending < 2:
        raise AssertionError(f"{label} expected at least two durable outbox entries, got {records!r}")

    context.set_offline(False)
    fresh.reload(wait_until="domcontentloaded")
    wait_for_board(fresh, authenticated)
    wait_for_text(fresh, f"{label} first tab")
    wait_for_text(fresh, f"{label} second tab")
    if authenticated:
        wait_for_authenticated_outbox_empty(fresh, diagnostics=network)

    assert_inbox_ack_is_scoped(fresh)
    assert_sync_diagnostics_export(fresh)

    records = read_sync_records(fresh)
    candidate_records = (
        [
            record
            for record in records
            if str(record.get("principal") or "").startswith("account:")
        ]
        if authenticated
        else records
    )
    candidate = max(
        candidate_records,
        key=lambda record: (bool(record.get("snapshot")), len(record.get("outbox") or [])),
        default=None,
    )
    if not candidate or not candidate.get("principal"):
        raise AssertionError(f"{label} did not create a canonical sync record: {records!r}")
    if authenticated and not str(candidate["principal"]).startswith("account:"):
        raise AssertionError(f"authenticated flow used a non-account principal: {records!r}")
    corrupt_snapshot(fresh, candidate)
    fresh.reload(wait_until="domcontentloaded")
    wait_for_board(fresh, authenticated)
    try:
        fresh.get_by_text("local data needs repair", exact=False).first.wait_for(
            state="visible", timeout=15_000
        )
    except TimeoutError as error:
        raise AssertionError(
            f"{label} did not surface local repair state after corrupting the canonical snapshot;\n"
            + fresh.locator("body").inner_text(timeout=2_000)[:4_000]
        ) from error
    assert_local_storage_backup_export(fresh, str(candidate["principal"]))
    assert_no_page_errors(errors, label)
    if dialogs:
        raise AssertionError(f"{label} unexpectedly opened browser dialogs: {dialogs}")

    for page in (first, second, fresh):
        page.close()


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_for_cdp(port: int, process: subprocess.Popen[bytes], log_path: Path) -> None:
    deadline = time.monotonic() + 15
    endpoint = f"http://127.0.0.1:{port}/json/version"
    while time.monotonic() < deadline:
        if process.poll() is not None:
            details = log_path.read_text(errors="replace")[-4_000:]
            raise RuntimeError(
                f"browser exited before CDP became ready (code {process.returncode}):\n{details}"
            )
        try:
            with urllib.request.urlopen(endpoint, timeout=1) as response:
                if response.status == 200:
                    return
        except (OSError, urllib.error.URLError):
            time.sleep(0.1)
    details = log_path.read_text(errors="replace")[-4_000:]
    raise RuntimeError(f"timed out waiting for browser CDP endpoint:\n{details}")


def stop_browser(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def run_browser_once(
    playwright: Any,
    executable: str,
    app_url: str,
    label: str,
    init_script: str | None = None,
    authenticated: bool = False,
    session_cookie: str | None = None,
    viewport: tuple[int, int] | None = None,
) -> None:
    with tempfile.TemporaryDirectory(prefix="task-space-browser-") as user_data_dir:
        port = find_free_port()
        log_path = Path(user_data_dir) / "browser.log"
        with log_path.open("wb") as log_file:
            browser_args = [
                    executable,
                    "--headless=new",
                    "--no-sandbox",
                    "--disable-gpu",
                    "--disable-crash-reporter",
                    "--disable-breakpad",
                    "--noerrdialogs",
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--disable-dev-shm-usage",
                    "--remote-debugging-address=127.0.0.1",
                    f"--remote-debugging-port={port}",
                    f"--user-data-dir={user_data_dir}",
                    "about:blank",
                ]
            if viewport:
                browser_args.insert(1, f"--window-size={viewport[0]},{viewport[1]}")
            process = subprocess.Popen(
                browser_args,
                stdout=log_file,
                stderr=subprocess.STDOUT,
            )
            browser = None
            try:
                wait_for_cdp(port, process, log_path)
                browser = playwright.chromium.connect_over_cdp(f"http://127.0.0.1:{port}")
                if not browser.contexts:
                    raise RuntimeError("CDP browser did not expose a default context")
                context = browser.contexts[0]
                if init_script:
                    context.add_init_script(init_script)
                run_context(context, app_url, label, authenticated, session_cookie)
                if not authenticated:
                    assert_expired_session_keeps_account_namespace(context, app_url)
            finally:
                if browser is not None:
                    browser.close()
                stop_browser(process)


def run_webkit_once(
    playwright: Any,
    app_url: str,
    label: str,
    init_script: str | None = None,
    authenticated: bool = False,
    session_cookie: str | None = None,
    viewport: tuple[int, int] | None = None,
) -> None:
    browser = playwright.webkit.launch(headless=True)
    try:
        context = browser.new_context(viewport=viewport)
        if init_script:
            context.add_init_script(init_script)
        run_context(context, app_url, label, authenticated, session_cookie)
    finally:
        browser.close()


def run_isolated_profile_pair(
    playwright: Any,
    executables: list[str],
    app_url: str,
    session_cookie: str,
    label: str = "authenticated-isolated-profiles",
) -> None:
    """Exercise two independent browser profiles through the real sync API."""
    if len(executables) != 2:
        raise ValueError("isolated profile pair requires exactly two executables")
    with tempfile.TemporaryDirectory(prefix="task-space-browser-pair-") as root:
        root_path = Path(root)
        processes: list[subprocess.Popen[bytes]] = []
        browsers: list[Any] = []
        contexts: list[BrowserContext] = []
        pages: list[Page] = []
        errors: list[str] = []
        console_messages: list[str] = []
        network: list[str] = []
        network_by_profile: dict[str, list[str]] = {}
        dialogs: list[str] = []
        try:
            for profile_label, executable in zip(("profile-a", "profile-b"), executables):
                profile = root_path / profile_label
                profile.mkdir()
                port = find_free_port()
                log_path = profile / "browser.log"
                with log_path.open("wb") as log_file:
                    process = subprocess.Popen(
                        [
                            executable,
                            "--headless=new",
                            "--no-sandbox",
                            "--disable-gpu",
                            "--disable-crash-reporter",
                            "--disable-breakpad",
                            "--noerrdialogs",
                            "--no-first-run",
                            "--no-default-browser-check",
                            "--disable-dev-shm-usage",
                            "--remote-debugging-address=127.0.0.1",
                            f"--remote-debugging-port={port}",
                            f"--user-data-dir={profile}",
                            "about:blank",
                        ],
                        stdout=log_file,
                        stderr=subprocess.STDOUT,
                    )
                processes.append(process)
                wait_for_cdp(port, process, log_path)
                browser = playwright.chromium.connect_over_cdp(
                    f"http://127.0.0.1:{port}"
                )
                browsers.append(browser)
                if not browser.contexts:
                    raise RuntimeError(
                        f"{profile_label} did not expose a default browser context"
                    )
                context = browser.contexts[0]
                contexts.append(context)
                profile_network: list[str] = []
                network_by_profile[profile_label] = profile_network
                if "ngrok" in urlsplit(app_url).netloc:
                    context.set_extra_http_headers({"ngrok-skip-browser-warning": "1"})
                context.add_init_script(
                    """
                    try {
                      localStorage.setItem(
                        "task-space:auth-refresh-lock",
                        JSON.stringify({ owner: "browser-smoke-stale-holder", expiresAt: Date.now() - 1 })
                      );
                    } catch (_) {}
                    """
                )
                seed_authenticated_namespace(
                    context, os.environ.get("TASK_SPACE_AUTHENTICATED_ACCOUNT_ID")
                )
                seed_session_cookie(context, app_url, session_cookie)
                page = context.new_page()
                page.on("pageerror", lambda error: errors.append(str(error)))
                page.on(
                    "response",
                    lambda response, page_network=profile_network: (
                        record_network_response(response, network),
                        record_network_response(response, page_network),
                    ),
                )
                page.on(
                    "console",
                    lambda message: console_messages.append(
                        f"console {message.type}: {message.text}"
                    ),
                )
                page.on(
                    "requestfailed",
                    lambda request, page_network=profile_network: (
                        network.append(f"FAILED {request.url}: {request.failure}"),
                        page_network.append(f"FAILED {request.url}: {request.failure}"),
                    )
                    if any(path in request.url for path in ("/auth/", "/account/", "/sync/"))
                    else None,
                )
                page.on(
                    "dialog",
                    lambda dialog: (
                        dialogs.append(f"{dialog.type}: {dialog.message}"),
                        dialog.dismiss(),
                    ),
                )
                page.goto(app_url, wait_until="domcontentloaded")
                wait_for_board(page, authenticated=True)
                pages.append(page)

            page_a, page_b = pages
            context_a, context_b = contexts
            page_a.wait_for_timeout(5_000)
            page_b.wait_for_timeout(5_000)

            add_note(page_a, "isolated profile A online")
            try:
                wait_for_text(page_b, "isolated profile A online", timeout=30_000)
            except TimeoutError as error:
                raise RuntimeError(
                    "isolated profile B did not receive the online edit;\n"
                    + "network:\n"
                    + "\n".join(network[-60:])
                    + "\nprofile A records:\n"
                    + repr(summarize_sync_records(read_sync_records(page_a)))
                    + "\nprofile B records:\n"
                    + repr(summarize_sync_records(read_sync_records(page_b)))
                    + "\nprofile A network:\n"
                    + "\n".join(network_by_profile.get("profile-a", [])[-40:])
                    + "\nprofile B network:\n"
                    + "\n".join(network_by_profile.get("profile-b", [])[-40:])
                    + "\nprofile B body:\n"
                    + page_b.locator("body").inner_text(timeout=2_000)[:4_000]
                    + "\nprofile B localStorage:\n"
                    + repr(page_b.evaluate("() => Object.fromEntries(Object.entries(localStorage))"))
                    + "\npage errors:\n"
                    + repr(errors[-30:])
                    + "\nconsole:\n"
                    + repr(console_messages[-30:])
                ) from error
            wait_for_authenticated_outbox_empty(page_a, diagnostics=network)
            wait_for_authenticated_outbox_empty(page_b, diagnostics=network)

            context_a.set_offline(True)
            add_note(page_a, "isolated profile A offline")
            add_note(page_b, "isolated profile B online")
            wait_for_text(page_b, "isolated profile B online", timeout=30_000)

            context_a.set_offline(False)
            wait_for_text(page_a, "isolated profile B online", timeout=30_000)
            wait_for_text(page_b, "isolated profile A offline", timeout=30_000)
            wait_for_authenticated_outbox_empty(page_a, diagnostics=network)
            wait_for_authenticated_outbox_empty(page_b, diagnostics=network)

            for page in (page_a, page_b):
                page.reload(wait_until="domcontentloaded")
                wait_for_board(page, authenticated=True)
                wait_for_text(page, "isolated profile A online")
                wait_for_text(page, "isolated profile A offline")
                wait_for_text(page, "isolated profile B online")
                records = read_sync_records(page)
                if not any(
                    str(record.get("principal") or "").startswith("account:")
                    for record in records
                ):
                    raise AssertionError(
                        "isolated profile used a non-account principal: "
                        + repr(summarize_sync_records(records))
                    )
            assert_no_page_errors(errors, label)
            if dialogs:
                raise AssertionError(
                    f"{label} unexpectedly opened browser dialogs: {dialogs}"
                )
        finally:
            for browser in browsers:
                browser.close()
            for process in processes:
                stop_browser(process)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--url",
        default=os.environ.get("TASK_SPACE_BROWSER_URL", "http://127.0.0.1:8081/app"),
        help="frontend app URL, including /app (or TASK_SPACE_BROWSER_URL)",
    )
    parser.add_argument(
        "--browser-executable",
        default=None,
        help="Chromium-compatible executable; defaults to Playwright or installed Chrome",
    )
    parser.add_argument(
        "--engine",
        choices=("chromium", "webkit"),
        default="chromium",
        help="browser engine to exercise (default: chromium)",
    )
    parser.add_argument(
        "--session-cookie",
        default=os.environ.get("TASK_SPACE_SESSION_COOKIE"),
        help="real task_space_session cookie for the authenticated acceptance path",
    )
    parser.add_argument(
        "--isolated-profiles",
        action="store_true",
        help="with --session-cookie, run two separate Chromium/Helium profiles",
    )
    parser.add_argument(
        "--cross-browser",
        action="store_true",
        help="with --session-cookie, run isolated Chrome and Helium profiles",
    )
    parser.add_argument(
        "--mobile",
        action="store_true",
        help="run the local-first gate at a narrow mobile viewport",
    )
    args = parser.parse_args()

    fallback_init = """
      Object.defineProperty(window, "BroadcastChannel", { configurable: true, value: undefined });
      try { Object.defineProperty(navigator, "locks", { configurable: true, value: undefined }); } catch (_) {}
    """
    with sync_playwright() as playwright:
        authenticated = bool(args.session_cookie)
        if args.cross_browser:
            if args.isolated_profiles:
                raise RuntimeError("--cross-browser cannot be combined with --isolated-profiles")
            if args.mobile:
                raise RuntimeError("--cross-browser cannot be combined with --mobile")
            if not args.session_cookie:
                raise RuntimeError("--cross-browser requires --session-cookie")
            if args.engine != "chromium":
                raise RuntimeError("--cross-browser currently supports Chromium engines only")
            chrome_candidates = [
                args.browser_executable,
                os.environ.get("TASK_SPACE_BROWSER_EXECUTABLE"),
                "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                playwright.chromium.executable_path,
            ]
            helium_candidates = [
                os.environ.get("TASK_SPACE_HELIUM_EXECUTABLE"),
                "/Applications/Helium.app/Contents/MacOS/Helium",
            ]
            chrome_executable = next(
                (path for path in chrome_candidates if path and Path(path).is_file()), None
            )
            helium_executable = next(
                (path for path in helium_candidates if path and Path(path).is_file()), None
            )
            if chrome_executable is None or helium_executable is None:
                raise RuntimeError(
                    "cross-browser mode requires both Chrome and Helium executables"
                )
            run_isolated_profile_pair(
                playwright,
                [chrome_executable, helium_executable],
                args.url,
                args.session_cookie,
                label="authenticated-cross-browser",
            )
            print(json.dumps({"status": "ok", "cross_browser": ["chrome", "helium"]}))
            return
        if args.isolated_profiles:
            if args.mobile:
                raise RuntimeError("--mobile cannot be combined with --isolated-profiles")
            if not args.session_cookie:
                raise RuntimeError("--isolated-profiles requires --session-cookie")
            if args.engine != "chromium":
                raise RuntimeError("--isolated-profiles currently supports Chromium engines only")
            candidates = [
                args.browser_executable,
                os.environ.get("TASK_SPACE_BROWSER_EXECUTABLE"),
                "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                "/Applications/Helium.app/Contents/MacOS/Helium",
                playwright.chromium.executable_path,
            ]
            executable = next(
                (path for path in candidates if path and Path(path).is_file()), None
            )
            if executable is None:
                raise RuntimeError("no Chromium-compatible browser executable was found")
            run_isolated_profile_pair(
                playwright,
                [executable, executable],
                args.url,
                args.session_cookie,
            )
            print(json.dumps({"status": "ok", "isolated_profiles": 2}))
            return
        if args.engine == "webkit":
            viewport = (390, 844) if args.mobile else None
            run_webkit_once(
                playwright,
                args.url,
                "webkit-regular",
                authenticated=authenticated,
                session_cookie=args.session_cookie,
                viewport=viewport,
            )
            run_webkit_once(
                playwright,
                args.url,
                "webkit-fallback",
                fallback_init,
                authenticated,
                args.session_cookie,
                viewport=viewport,
            )
            result = {"status": "ok", "engine": "webkit", "contexts": ["regular", "fallback"]}
            if args.mobile:
                result["viewport"] = "390x844"
            print(json.dumps(result))
            return
        candidates = [
            args.browser_executable,
            os.environ.get("TASK_SPACE_BROWSER_EXECUTABLE"),
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            "/Applications/Helium.app/Contents/MacOS/Helium",
            playwright.chromium.executable_path,
        ]
        executable = next((path for path in candidates if path and Path(path).is_file()), None)
        if executable is None:
            raise RuntimeError("no Chromium-compatible browser executable was found")
        # Launch Chrome directly and attach over CDP. This avoids Playwright's
        # pipe launcher, which can fail on macOS hosts where crashpad child
        # handshakes are restricted even though normal headless Chrome works.
        run_browser_once(
            playwright,
            executable,
            args.url,
            "regular",
            authenticated=authenticated,
            session_cookie=args.session_cookie,
            viewport=(390, 844) if args.mobile else None,
        )
        run_browser_once(
            playwright,
            executable,
            args.url,
            "fallback",
            fallback_init,
            authenticated,
            args.session_cookie,
            viewport=(390, 844) if args.mobile else None,
        )

    result = {"status": "ok", "contexts": ["regular", "fallback"]}
    if args.mobile:
        result["viewport"] = "390x844"
    print(json.dumps(result))


if __name__ == "__main__":
    main()
