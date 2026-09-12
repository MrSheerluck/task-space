.PHONY: dev css fonts check cloudflare-tunnel browser-sync browser-webkit browser-mobile browser-cross browser-authenticated provider-acceptance workos-authenticated postgres-acceptance migration-acceptance capacity-acceptance restart-chaos process-kill-chaos backup-restore-acceptance alert-delivery observability-check rollout-preflight

dev: css ## serve the web app (trunk) with hot reload; tailwind watches in background
	cd apps/web && (npx @tailwindcss/cli -i src/input.css -o src/main.css --watch --poll=500 &) && env -u TRUNK_NO_COLOR -u NO_COLOR trunk serve

css: fonts ## build tailwind tokens once
	cd apps/web && npx @tailwindcss/cli -i src/input.css -o src/main.css

fonts: ## inline the handwriting font as data URI (no CDN, trunk-safe)
	@node -e "const fs=require('fs');const b=fs.readFileSync('apps/web/src/fonts/caveat-latin-var.woff2');fs.writeFileSync('apps/web/src/fonts-inline.css','@font-face{font-family:\"Caveat\";src:url(data:font/woff2;base64,'+b.toString('base64')+') format(\"woff2\");font-weight:400 700;font-display:swap;}')"

check: ## compile everything
	cargo check --workspace

cloudflare-tunnel: ## expose the local deployment through the named Cloudflare Tunnel
	cloudflared tunnel --no-autoupdate run --url http://127.0.0.1:8081 task-space-dev

browser-sync: ## exercise offline tabs, IndexedDB recovery, and fallbacks in headless Chromium
	python3 scripts/browser-sync-smoke.py

browser-webkit: ## exercise the same local-first gate in WebKit
	python3 scripts/browser-sync-smoke.py --engine webkit

browser-mobile: ## exercise the local-first gate at a narrow mobile viewport
	python3 scripts/browser-sync-smoke.py --mobile

browser-cross: ## exercise authenticated Chrome and Helium convergence
	python3 scripts/browser-sync-smoke.py --cross-browser

browser-authenticated: ## exercise authenticated isolated profiles, WebKit, and mobile
	./scripts/browser-authenticated-acceptance.sh

provider-acceptance: ## exercise the configured Dodo test product, checkout, and signed webhook replay
	./scripts/provider-acceptance.sh

workos-authenticated: ## exercise the real WorkOS session and protected browser convergence
	./scripts/workos-authenticated-acceptance.sh

postgres-acceptance: ## run database-backed sync acceptance tests inside the Compose network
	./scripts/run-postgres-acceptance.sh

migration-acceptance: ## validate clean-install and legacy-schema migrations in disposable PostgreSQL
	./scripts/migration-acceptance.sh

capacity-acceptance: ## run the larger 10k-space/100k-update migration rehearsal
	TASK_SPACE_MIGRATION_SIZED_SPACES=10000 TASK_SPACE_MIGRATION_SIZED_UPDATES=100000 ./scripts/migration-acceptance.sh

restart-chaos: ## restart local acceptance API/PostgreSQL and verify recovery
	TASK_SPACE_CHAOS_CONFIRM=I_UNDERSTAND_LOCAL_RESTART_TEST ./scripts/restart-acceptance-smoke.sh

process-kill-chaos: ## abruptly terminate local acceptance API/PostgreSQL and verify recovery
	TASK_SPACE_CHAOS_CONFIRM=I_UNDERSTAND_LOCAL_KILL_TEST ./scripts/process-kill-acceptance.sh

alert-delivery: ## exercise Prometheus to Alertmanager webhook delivery locally
	./scripts/alert-delivery-acceptance.sh

backup-restore-acceptance: ## restore a live local backup into a disposable database and sync-test it
	./scripts/backup-restore-acceptance.sh

observability-check: ## validate Prometheus config and alert rules with the pinned tool image
	./scripts/check-observability.sh

rollout-preflight: ## check readiness, app delivery, fail-closed auth, and optional metrics
	./scripts/rollout-preflight.sh
