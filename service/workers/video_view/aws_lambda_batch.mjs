// Batched trimmed video_view worker: fetches N video views from bilibili
// CONCURRENTLY inside one invocation and returns per-item results. Companion
// to aws_lambda.mjs (the single-aid trimmed worker), deployed as a SEPARATE
// function so the single-aid path is never touched by this code.
//
// ============================== CONTRACT (v1) ==============================
//
// Request:
//   GET /?aids=<token>[,<token>...]
//
//   - `aids` is a comma-separated list. Tokens are forwarded to the upstream
//     `aid` query parameter AS-IS: no numeric validation, no trimming, no
//     de-duplication. The upstream API is the authority on token validity,
//     exactly like the single-aid worker (transparent-proxy philosophy).
//     Duplicate tokens are each fetched independently and echoed 1:1.
//     De-duplication is the CALLER's responsibility.
//   - Missing `aids`, an empty `aids`, or more than MAX_AIDS tokens is a
//     caller bug, answered loudly with HTTP 400 (shape below), never with a
//     partial result.
//
// Response (HTTP 200 whenever the request shape is valid, even if every
// single item failed -- failure is reported per item):
//
//   {
//     "v": 1,                 // batch protocol version; callers MUST verify
//     "requested": <int>,     // number of tokens received
//     "results": [ <item>, ... ]   // same length and SAME ORDER as `aids`
//   }
//
// Every item echoes its input token as `aid` (the raw string, exactly as it
// appeared between the commas) and carries `ms` (wall-clock milliseconds this
// item took, for calibration and debugging). Item variants, by `kind`:
//
//   { "aid", "ms", "kind": "json", "status": <upstream HTTP status>,
//     "body": <object> }
//       Upstream body parsed as JSON. When it has code === 0 and a
//       data.stat object, `body` is the TRIMMED envelope -- identical
//       field-for-field to what the single-aid worker returns (STAT_KEYS
//       with `?? null` backfill). Anything else (deleted/hidden videos,
//       -404, -400, ...) is the upstream JSON passed through UNMODIFIED,
//       so the caller's per-item error handling matches the single path.
//
//   { "aid", "ms", "kind": "non_json", "status": <upstream HTTP status>,
//     "body_snippet": <string> }
//       Upstream body was not JSON (e.g. an HTML error page). First
//       BODY_SNIPPET_MAX chars only.
//
//   { "aid", "ms", "kind": "item_timeout", "timeout_ms": <int> }
//       This item's fetch exceeded PER_ITEM_TIMEOUT_MS and was aborted.
//       Other items are unaffected.
//
//   { "aid", "ms", "kind": "fetch_error", "detail": <string> }
//       The fetch itself failed (DNS, connect, TLS, ...). Other items are
//       unaffected.
//
// Errors that are NOT per-item:
//   HTTP 400  { "v": 1, "error": <string> }   -- malformed request (see above)
//   HTTP 500  { "v": 1, "error": <string> }   -- unexpected handler failure
//   Plus whatever the platform itself produces (function timeout, crash).
//   Callers treat any non-200 / unparsable top level as a whole-batch
//   failure and own the retry policy; this worker never retries upstream.
//
// Execution model: all items are fetched concurrently (duration of the
// invocation ~= max of item durations, plus parse overhead). The upstream
// User-Agent is whatever the runtime's fetch sends, matching the current
// single-aid worker's behavior on purpose -- changing the upstream request
// fingerprint is out of scope for the batching change.
//
// Tunables (initial hypotheses pending load-test calibration -- override via
// Lambda environment variables without a code change):
//   PER_ITEM_TIMEOUT_MS  default 4500   derived from the single-fetch p99.9
//                                       (~3.3s) plus headroom; the function
//                                       timeout must stay ABOVE this value
//   MAX_AIDS             default 50     upper bound on tokens per request
// ===========================================================================

const baseUrl = new URL("http://api.bilibili.com/x/web-interface/view");

// stat values are passed through untouched (no numeric coercion: view can be
// the string "--" for hidden counts); vt/vv are missing on older videos.
// MUST stay identical to the single-aid worker's list (aws_lambda.mjs).
const STAT_KEYS = [
  "aid", "view", "danmaku", "reply", "favorite", "coin", "share",
  "now_rank", "his_rank", "like", "dislike", "vt", "vv",
];

const DEFAULT_PER_ITEM_TIMEOUT_MS = 4500;
const DEFAULT_MAX_AIDS = 50;
const BODY_SNIPPET_MAX = 2048;

function intEnv(name, fallback) {
  const parsed = parseInt(process.env[name], 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

// build the item's `body`: trimmed envelope for a healthy view response,
// upstream JSON unmodified for everything else -- same rules, same key
// order as the single-aid worker
function trimOrPassthrough(parsed) {
  if (
    parsed.code !== 0 ||
    typeof parsed.data !== "object" || parsed.data === null ||
    typeof parsed.data.stat !== "object" || parsed.data.stat === null
  ) {
    return parsed;
  }
  const stat = {};
  for (const key of STAT_KEYS) {
    stat[key] = parsed.data.stat[key] ?? null;
  }
  return {
    code: parsed.code,
    message: parsed.message,
    ttl: parsed.ttl,
    data: {
      bvid: parsed.data.bvid,
      aid: parsed.data.aid,
      stat,
    },
  };
}

async function fetchOne(token, timeoutMs) {
  const started = Date.now();
  const item = { aid: token, ms: 0 };
  try {
    const url = new URL(baseUrl.href);
    url.searchParams.set("aid", token);
    const response = await fetch(url.href, {
      signal: AbortSignal.timeout(timeoutMs),
    });
    const text = await response.text();
    item.status = response.status;
    try {
      item.body = trimOrPassthrough(JSON.parse(text));
      item.kind = "json";
    } catch {
      item.kind = "non_json";
      item.body_snippet = text.slice(0, BODY_SNIPPET_MAX);
    }
  } catch (error) {
    if (error && (error.name === "TimeoutError" || error.name === "AbortError")) {
      item.kind = "item_timeout";
      item.timeout_ms = timeoutMs;
    } else {
      item.kind = "fetch_error";
      item.detail = (error && error.message) || String(error);
    }
  }
  item.ms = Date.now() - started;
  return item;
}

function badRequest(message) {
  return {
    statusCode: 400,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ v: 1, error: message }),
  };
}

export const handler = async (event) => {
  try {
    const timeoutMs = intEnv("PER_ITEM_TIMEOUT_MS", DEFAULT_PER_ITEM_TIMEOUT_MS);
    const maxAids = intEnv("MAX_AIDS", DEFAULT_MAX_AIDS);

    const aids = (event.queryStringParameters || {}).aids;
    if (typeof aids !== "string" || aids.length === 0) {
      return badRequest("missing aids parameter");
    }
    const tokens = aids.split(",");
    if (tokens.length > maxAids) {
      return badRequest(`too many aids: ${tokens.length} > ${maxAids}`);
    }

    // all items in flight at once; fetchOne never rejects, so Promise.all
    // preserves order and cannot fail as a whole
    const results = await Promise.all(
      tokens.map((token) => fetchOne(token, timeoutMs)));

    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ v: 1, requested: tokens.length, results }),
    };
  } catch (error) {
    console.error("Unexpected batch handler failure:", error);
    return {
      statusCode: 500,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        v: 1,
        error: (error && error.message) || "Internal Server Error",
      }),
    };
  }
};
