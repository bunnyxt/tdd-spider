addEventListener("fetch", event => {
  event.respondWith(handleRequest(event.request))
});

async function handleRequest(request) {
  const { searchParams } = new URL(request.url);

  const aid = searchParams.get('aid');

  const response = await fetch(`http://api.bilibili.com/x/tag/archive/tags?aid=${aid}`);

  const text = await response.text();

  return new Response(text);
};
