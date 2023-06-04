// define base url, do NOT include suffix `?` or `/`
const baseUrl = new URL('http://api.bilibili.com/x/tag/archive/tags');

export default {
  async fetch(request, env, ctx) {
    // parse request url
    const requestUrl = new URL(request.url);
    const searchString = requestUrl.search;

    // make sub request and get res
    let res =  await fetch(baseUrl.href + searchString);

    // optionally modify res
    //

    return res;
  }
}
