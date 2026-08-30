.PHONY: dev css fonts check

dev: css ## serve the web app (trunk) with hot reload; tailwind watches in background
	cd apps/web && (npx @tailwindcss/cli -i src/input.css -o src/main.css --watch &) && trunk serve

css: fonts ## build tailwind tokens once
	cd apps/web && npx @tailwindcss/cli -i src/input.css -o src/main.css

fonts: ## inline the handwriting font as data URI (no CDN, trunk-safe)
	@node -e "const fs=require('fs');const b=fs.readFileSync('apps/web/src/fonts/caveat-latin-var.woff2');fs.writeFileSync('apps/web/src/fonts-inline.css','@font-face{font-family:\"Caveat\";src:url(data:font/woff2;base64,'+b.toString('base64')+') format(\"woff2\");font-weight:400 700;font-display:swap;}')"

check: ## compile everything
	cargo check --workspace
