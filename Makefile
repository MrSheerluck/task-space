.PHONY: dev check

dev: ## serve the web app (trunk) with hot reload
	cd apps/web && trunk serve

check: ## compile everything
	cargo check --workspace
