// The browser remains on a supported Linux origin; Workers only forward access traffic.
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (
      !/^\/(view(?:\/socket)?|viewer\.(?:js|css)|execution\/[a-f0-9]{64}(?:\/(?:session|auth)|\/files\/[A-Za-z0-9._-]+)?)$/.test(
        url.pathname,
      )
    )
      return new Response(null, { status: 404 });
    const origin = new URL(env.BROWSER_ORIGIN);
    if (
      origin.protocol !== "https:" ||
      origin.username ||
      origin.password ||
      origin.pathname !== "/" ||
      origin.search ||
      origin.hash
    )
      return new Response("Invalid browser origin", { status: 503 });
    url.protocol = origin.protocol;
    url.host = origin.host;
    // Do not retry or follow redirects: CDP and input may have already executed.
    try {
      return await fetch(new Request(url, request), { redirect: "manual" });
    } catch {
      return new Response("Browser origin unavailable", { status: 502 });
    }
  },
};
