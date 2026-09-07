// define base url, do NOT include suffix `?` or `/`
const baseUrl = new URL("http://api.bilibili.com/x/web-interface/newlist");

export default {
  async fetch(request, env, ctx) {
    // parse request url
    const requestUrl = new URL(request.url);
    const searchString = requestUrl.search;

    // make sub request and get res
    const userAgent = request.headers.get("user-agent");
    const init = userAgent ? { headers: { "User-Agent": userAgent } } : undefined;
    let res = await fetch(baseUrl.href + searchString, init);

    // optionally modify res
    //

    return res;
  },
};
