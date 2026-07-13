// Trimmed video_view worker: fetches the full view from bilibili, then
// returns only the fields stat-record jobs consume (see VideoViewTrimmed /
// RecordNew in the spider). Full view responses run 200KB-2.8MB for
// season/multi-part videos while the record path needs ~250B of it; trimming
// here keeps that bloat off the spider's constrained inbound link.
//
// NOT a transparent proxy (unlike workers/template/aws_lambda.mjs) -- this is
// a distinct endpoint contract, registered as get_video_view_trimmed. Error
// semantics ARE transparent: non-0 code or unexpected shape returns the
// upstream body unmodified, so the spider's CodeError handling is identical
// on both endpoints.
const baseUrl = new URL("http://api.bilibili.com/x/web-interface/view");

// stat values are passed through untouched (no numeric coercion: view can be
// the string "--" for hidden counts); vt/vv are missing on older videos
const STAT_KEYS = [
  "aid", "view", "danmaku", "reply", "favorite", "coin", "share",
  "now_rank", "his_rank", "like", "dislike", "vt", "vv",
];

export const handler = async (event) => {
  const queryParams = event.queryStringParameters || {};

  const url = new URL(baseUrl.href);
  Object.keys(queryParams).forEach((key) => {
    url.searchParams.append(key, queryParams[key]);
  });

  try {
    const response = await fetch(url.href);
    const body = await response.text();

    let parsed;
    try {
      parsed = JSON.parse(body);
    } catch {
      // non-JSON upstream body: pass through as-is
      return { statusCode: response.status, body };
    }

    if (
      parsed.code !== 0 ||
      typeof parsed.data !== "object" || parsed.data === null ||
      typeof parsed.data.stat !== "object" || parsed.data.stat === null
    ) {
      // error responses (deleted/hidden videos etc.) pass through unmodified
      return { statusCode: response.status, body };
    }

    const stat = {};
    for (const key of STAT_KEYS) {
      stat[key] = parsed.data.stat[key] ?? null;
    }

    return {
      statusCode: response.status,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        code: parsed.code,
        message: parsed.message,
        ttl: parsed.ttl,
        data: {
          bvid: parsed.data.bvid,
          aid: parsed.data.aid,
          stat,
        },
      }),
    };
  } catch (error) {
    console.error("Error occurred while fetching data:", error);

    return {
      statusCode: 500,
      body: JSON.stringify({
        message: error.message || "Internal Server Error",
      }),
    };
  }
};
