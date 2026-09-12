FROM nginx:1.27-alpine
COPY apps/web/dist /usr/share/nginx/html
# wasm-bindgen keeps a stable snippet directory name across builds even when
# inline JavaScript changes. Cache-bust the JavaScript module URLs while
# preserving the directory/module name embedded in the WASM import table.
RUN set -eu; \
    for snippet_dir in /usr/share/nginx/html/snippets/*; do \
        [ -d "$snippet_dir" ] || continue; \
        snippet_name="$(basename "$snippet_dir")"; \
        snippet_version="$(find "$snippet_dir" -type f -exec sha256sum {} \; | sha256sum | cut -c1-16)"; \
        find /usr/share/nginx/html -type f -name '*.js' \
            -exec sed -i \
            -e "s|from './snippets/${snippet_name}/inline0.js'|from './snippets/${snippet_name}/inline0.js?v=${snippet_version}'|g" \
            -e "s|from \"./snippets/${snippet_name}/inline0.js\"|from \"./snippets/${snippet_name}/inline0.js?v=${snippet_version}\"|g" \
            -e "s|from './snippets/${snippet_name}/inline1.js'|from './snippets/${snippet_name}/inline1.js?v=${snippet_version}'|g" \
            -e "s|from \"./snippets/${snippet_name}/inline1.js\"|from \"./snippets/${snippet_name}/inline1.js?v=${snippet_version}\"|g" {} +; \
        find /usr/share/nginx/html -type f -name '*.html' \
            -exec sed -i \
            -e "s|/snippets/${snippet_name}/inline0.js\"|/snippets/${snippet_name}/inline0.js?v=${snippet_version}\"|g" \
            -e "s|/snippets/${snippet_name}/inline1.js\"|/snippets/${snippet_name}/inline1.js?v=${snippet_version}\"|g" {} +; \
    done; \
    for js_file in /usr/share/nginx/html/task-web-*.js; do \
        [ -f "$js_file" ] || continue; \
        js_name="$(basename "$js_file")"; \
        js_stem="${js_name%.js}"; \
        js_version="$(sha256sum "$js_file" | cut -c1-16)"; \
        versioned_js_name="${js_stem}-${js_version}.js"; \
        mv "$js_file" "/usr/share/nginx/html/$versioned_js_name"; \
        find /usr/share/nginx/html -type f \( -name '*.html' -o -name '*.js' \) \
            -exec sed -i "s|/${js_name}|/${versioned_js_name}|g" {} +; \
        # The JS file was rewritten to point at the versioned snippet path, so
        # discard Trunk's now-stale SRI value. The filename is content-versioned
        # and the snippet modules retain their own verified SRI attributes. \
        sed -i "s|href=\"/${versioned_js_name}\" crossorigin=\"anonymous\" integrity=\"sha384-[^\"]*\"|href=\"/${versioned_js_name}\" crossorigin=\"anonymous\"|g" /usr/share/nginx/html/index.html; \
    done; \
    for wasm_file in /usr/share/nginx/html/task-web-*_bg.wasm; do \
        [ -f "$wasm_file" ] || continue; \
        wasm_name="$(basename "$wasm_file")"; \
        wasm_stem="${wasm_name%.wasm}"; \
        wasm_version="$(sha256sum "$wasm_file" | cut -c1-16)"; \
        versioned_wasm_name="${wasm_stem}-${wasm_version}.wasm"; \
        mv "$wasm_file" "/usr/share/nginx/html/$versioned_wasm_name"; \
        find /usr/share/nginx/html -type f -name '*.html' \
            -exec sed -i "s|/${wasm_name}|/${versioned_wasm_name}|g" {} +; \
    done
COPY deploy/nginx.conf /etc/nginx/conf.d/default.conf
