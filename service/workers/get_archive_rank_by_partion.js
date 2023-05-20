addEventListener("fetch", event => {
  event.respondWith(handleRequest(event.request))
});

async function handleRequest(request) {
  const { searchParams } = new URL(request.url);

  const tid = searchParams.get('tid');
  const pn = searchParams.get('pn');
  const ps = searchParams.get('ps');

  const response = await fetch(`http://api.bilibili.com/archive_rank/getarchiverankbypartion?tid=${tid}&pn=${pn}&ps=${ps}`);

  const text = await response.text();

  return new Response(text);
};
