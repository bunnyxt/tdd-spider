// Fetch the full video view upstream, then return only the fields consumed by
// the stat-record path. Non-zero codes and unexpected bodies pass through.
const functions = require("@google-cloud/functions-framework");

const baseUrl = new URL("http://api.bilibili.com/x/web-interface/view");
const STAT_KEYS = [
  "aid", "view", "danmaku", "reply", "favorite", "coin", "share",
  "now_rank", "his_rank", "like", "dislike", "vt", "vv",
];

functions.http("proxyRequest", async (req, res) => {
  const url = new URL(baseUrl.href);
  Object.keys(req.query || {}).forEach((key) => {
    url.searchParams.append(key, req.query[key]);
  });

  const headers = req.headers || {};
  const userAgent = headers["user-agent"] || headers["User-Agent"];
  const init = userAgent ? { headers: { "User-Agent": userAgent } } : undefined;

  try {
    const response = await fetch(url.href, init);
    const body = await response.text();

    let parsed;
    try {
      parsed = JSON.parse(body);
    } catch {
      res.status(response.status).send(body);
      return;
    }

    if (
      parsed.code !== 0 ||
      typeof parsed.data !== "object" || parsed.data === null ||
      typeof parsed.data.stat !== "object" || parsed.data.stat === null
    ) {
      res.status(response.status).send(body);
      return;
    }

    const stat = {};
    for (const key of STAT_KEYS) {
      stat[key] = parsed.data.stat[key] ?? null;
    }

    res.status(response.status).set("Content-Type", "application/json").send(
      JSON.stringify({
        code: parsed.code,
        message: parsed.message,
        ttl: parsed.ttl,
        data: {
          bvid: parsed.data.bvid,
          aid: parsed.data.aid,
          stat,
        },
      }),
    );
  } catch (error) {
    console.error("Error occurred while fetching data:", error);
    res.status(500).json({
      message: error.message || "Internal Server Error",
    });
  }
});
