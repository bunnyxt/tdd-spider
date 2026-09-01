// Contract tests for the batched trimmed video_view worker
// (service/workers/video_view/aws_lambda_batch.mjs).
//
// Run from the repo root:  node --test tests/
//
// All upstream traffic is mocked by replacing globalThis.fetch -- no real
// network request is ever made. Each test rebuilds the mock, so tests are
// order-independent.

import { test, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { handler } from "../service/workers/video_view/aws_lambda_batch.mjs";

const realFetch = globalThis.fetch;

// a healthy upstream view payload for the given aid, with the season/staff
// bloat the trimmer must strip
function viewPayload(aid) {
  return {
    code: 0,
    message: "0",
    ttl: 1,
    data: {
      bvid: `BV_mock_${aid}`,
      aid: Number(aid),
      videos: 1,
      tid: 30,
      title: "mock title",
      staff: [{ mid: 1, name: "x" }],
      stat: {
        aid: Number(aid),
        view: 100,
        danmaku: 1,
        reply: 2,
        favorite: 3,
        coin: 4,
        share: 5,
        now_rank: 0,
        his_rank: 13,
        like: 6,
        dislike: 0,
        // vt / vv intentionally absent -> trimmer must backfill null
      },
    },
  };
}

function jsonResponse(obj, status = 200) {
  return {
    status,
    text: async () => JSON.stringify(obj),
  };
}

function mockFetch(perAid) {
  // perAid: async (aidToken, init) -> Response-like; records call order
  const calls = [];
  globalThis.fetch = async (href, init) => {
    const aid = new URL(href).searchParams.get("aid");
    calls.push(aid);
    return perAid(aid, init);
  };
  return calls;
}

async function invoke(aids) {
  const event =
    aids === undefined
      ? { queryStringParameters: {} }
      : { queryStringParameters: { aids } };
  // AbortSignal.timeout timers are unref'ed; with fetch mocked there is no
  // socket keeping the event loop alive, so hold it open until the handler
  // settles (a real runtime never needs this)
  const keepAlive = setInterval(() => {}, 100);
  try {
    const res = await handler(event);
    return { res, body: JSON.parse(res.body) };
  } finally {
    clearInterval(keepAlive);
  }
}

beforeEach(() => {
  delete process.env.PER_ITEM_TIMEOUT_MS;
  delete process.env.MAX_AIDS;
});

afterEach(() => {
  globalThis.fetch = realFetch;
  delete process.env.PER_ITEM_TIMEOUT_MS;
  delete process.env.MAX_AIDS;
});

test("normal batch: trimmed items, input order, token echo, v/requested", async () => {
  mockFetch(async (aid) => jsonResponse(viewPayload(aid)));
  const { res, body } = await invoke("170001,42,7");

  assert.equal(res.statusCode, 200);
  assert.equal(body.v, 1);
  assert.equal(body.requested, 3);
  assert.equal(body.results.length, 3);
  assert.deepEqual(body.results.map((r) => r.aid), ["170001", "42", "7"]);

  for (const item of body.results) {
    assert.equal(item.kind, "json");
    assert.equal(item.status, 200);
    assert.equal(typeof item.ms, "number");
    // trimmed envelope: exactly code/message/ttl/data, data exactly
    // bvid/aid/stat -- the 200KB-class keys (videos, title, staff...) gone
    assert.deepEqual(Object.keys(item.body), ["code", "message", "ttl", "data"]);
    assert.deepEqual(Object.keys(item.body.data), ["bvid", "aid", "stat"]);
    assert.equal(item.body.data.bvid, `BV_mock_${item.aid}`);
    // absent vt/vv backfilled to null, same as the single-aid worker
    assert.equal(item.body.data.stat.vt, null);
    assert.equal(item.body.data.stat.vv, null);
    assert.equal(item.body.data.stat.view, 100);
  }
});

test("upstream error code passes through unmodified", async () => {
  const notFound = { code: -404, message: "啥都木有", ttl: 1 };
  mockFetch(async (aid) =>
    aid === "2" ? jsonResponse(notFound) : jsonResponse(viewPayload(aid)));
  const { body } = await invoke("1,2");

  assert.equal(body.results[0].kind, "json");
  assert.equal(body.results[0].body.code, 0);
  assert.equal(body.results[1].kind, "json");
  assert.deepEqual(body.results[1].body, notFound); // byte-for-byte passthrough
});

test("non-JSON upstream becomes non_json with truncated snippet", async () => {
  const html = "<!DOCTYPE html>" + "x".repeat(5000);
  mockFetch(async () => ({ status: 404, text: async () => html }));
  const { body } = await invoke("1");

  const item = body.results[0];
  assert.equal(item.kind, "non_json");
  assert.equal(item.status, 404);
  assert.equal(item.body_snippet.length, 2048);
  assert.ok(item.body_snippet.startsWith("<!DOCTYPE html>"));
  assert.equal(item.body, undefined);
});

test("per-item timeout aborts only that item", async () => {
  process.env.PER_ITEM_TIMEOUT_MS = "40";
  mockFetch(async (aid, init) => {
    if (aid === "slow") {
      // never resolves; rejects with the abort reason when the signal fires,
      // which is how undici's fetch surfaces AbortSignal.timeout
      return new Promise((_, reject) => {
        init.signal.addEventListener("abort", () => reject(init.signal.reason));
      });
    }
    return jsonResponse(viewPayload(aid));
  });
  const { body } = await invoke("1,slow,3");

  assert.equal(body.results[0].kind, "json");
  assert.equal(body.results[2].kind, "json");
  const slow = body.results[1];
  assert.equal(slow.aid, "slow");
  assert.equal(slow.kind, "item_timeout");
  assert.equal(slow.timeout_ms, 40);
  assert.ok(slow.ms >= 30, `ms should reflect the wait, got ${slow.ms}`);
});

test("duplicate aids are fetched independently and echoed 1:1", async () => {
  const calls = mockFetch(async (aid) => jsonResponse(viewPayload(aid)));
  const { body } = await invoke("9,9,9");

  assert.equal(body.requested, 3);
  assert.deepEqual(calls, ["9", "9", "9"]); // one upstream fetch per token
  assert.deepEqual(body.results.map((r) => r.aid), ["9", "9", "9"]);
});

test("invalid tokens are forwarded as-is; upstream verdict passes through", async () => {
  const badReq = { code: -400, message: "请求错误", ttl: 1 };
  const calls = mockFetch(async (aid) =>
    /^\d+$/.test(aid) ? jsonResponse(viewPayload(aid)) : jsonResponse(badReq));
  const { body } = await invoke("abc,, 5,6");

  // every token forwarded untouched, including empty and space-prefixed
  assert.deepEqual(calls, ["abc", "", " 5", "6"]);
  assert.deepEqual(body.results.map((r) => r.aid), ["abc", "", " 5", "6"]);
  assert.deepEqual(body.results[0].body, badReq);
  assert.deepEqual(body.results[1].body, badReq);
  assert.deepEqual(body.results[2].body, badReq);
  assert.equal(body.results[3].body.code, 0);
});

test("missing and empty aids answer 400, no upstream fetch", async () => {
  const calls = mockFetch(async () => {
    throw new Error("must not fetch");
  });
  for (const args of [undefined, ""]) {
    const { res, body } = await invoke(args);
    assert.equal(res.statusCode, 400);
    assert.equal(body.v, 1);
    assert.match(body.error, /aids/);
  }
  assert.equal(calls.length, 0);
});

test("more than MAX_AIDS tokens answers 400, no upstream fetch", async () => {
  process.env.MAX_AIDS = "3";
  const calls = mockFetch(async () => {
    throw new Error("must not fetch");
  });
  const { res, body } = await invoke("1,2,3,4");

  assert.equal(res.statusCode, 400);
  assert.match(body.error, /too many aids: 4 > 3/);
  assert.equal(calls.length, 0);
});

test("network failure becomes fetch_error for that item only", async () => {
  mockFetch(async (aid) => {
    if (aid === "down") throw new TypeError("fetch failed: ECONNRESET");
    return jsonResponse(viewPayload(aid));
  });
  const { body } = await invoke("1,down");

  assert.equal(body.results[0].kind, "json");
  const down = body.results[1];
  assert.equal(down.kind, "fetch_error");
  assert.match(down.detail, /ECONNRESET/);
});

test("mixed batch keeps order across all result kinds", async () => {
  process.env.PER_ITEM_TIMEOUT_MS = "40";
  mockFetch(async (aid, init) => {
    switch (aid) {
      case "ok":
        return jsonResponse(viewPayload("1"));
      case "gone":
        return jsonResponse({ code: -404, message: "nope", ttl: 1 });
      case "html":
        return { status: 502, text: async () => "<html>bad gateway</html>" };
      case "slow":
        return new Promise((_, reject) => {
          init.signal.addEventListener("abort", () => reject(init.signal.reason));
        });
      default:
        throw new Error("boom");
    }
  });
  const { body } = await invoke("ok,gone,html,slow,err");

  assert.deepEqual(
    body.results.map((r) => [r.aid, r.kind]),
    [
      ["ok", "json"],
      ["gone", "json"],
      ["html", "non_json"],
      ["slow", "item_timeout"],
      ["err", "fetch_error"],
    ]);
});
