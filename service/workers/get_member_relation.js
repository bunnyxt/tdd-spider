addEventListener("fetch", event => {
  event.respondWith(handleRequest(event.request))
});

async function handleRequest(request) {
  const { searchParams } = new URL(request.url);

  const mid = searchParams.get('mid');

  const response = await fetch(`http://api.bilibili.com/x/relation/stat?vmid=${mid}`);

  const text = await response.text();

  return new Response(text);
};
