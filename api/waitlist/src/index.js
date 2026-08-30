const ALLOWED_ORIGIN = "*";

const headers = {
  "access-control-allow-methods": "GET, POST, OPTIONS",
  "access-control-allow-headers": "content-type",
};

export default {
  async fetch(request, env) {
    const origin = request.headers.get("origin") || "";
    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: { ...headers, "access-control-allow-origin": ALLOWED_ORIGIN },
      });
    }

    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/waitlist/count") {
      const row = await env.WAITLIST_DB.prepare(
        "SELECT COUNT(*) AS count FROM waitlist",
      ).first();
      return Response.json({ count: row?.count ?? 0 }, { headers: { ...headers, "access-control-allow-origin": ALLOWED_ORIGIN } });
    }

    if (request.method === "POST" && url.pathname === "/waitlist") {
      let payload;
      try {
        payload = await request.json();
      } catch {
        return new Response("invalid json", {
          status: 400,
          headers: { ...headers, "access-control-allow-origin": ALLOWED_ORIGIN },
        });
      }

      const email = String(payload?.email ?? "").trim().toLowerCase();
      const source = String(payload?.source ?? "landing").slice(0, 64);
      if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
        return new Response("invalid email", {
          status: 400,
          headers: { ...headers, "access-control-allow-origin": ALLOWED_ORIGIN },
        });
      }

      await env.WAITLIST_DB.prepare(
        "INSERT OR IGNORE INTO waitlist (email, source) VALUES (?1, ?2)",
      )
        .bind(email, source)
        .run();

      return Response.json(
        { ok: true, message: "you're on the list" },
        { headers: { ...headers, "access-control-allow-origin": ALLOWED_ORIGIN } },
      );
    }

    return new Response("not found", { status: 404 });
  },
};
