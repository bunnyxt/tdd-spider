const baseUrl = new URL("http://api.bilibili.com/x/web-interface/card");

module.exports = async function (context, req) {
  // Append query parameters to the base URL
  const url = new URL(baseUrl.href); // Clone baseUrl to avoid modifying the original
  Object.keys(req.query).forEach((key) => {
    url.searchParams.append(key, req.query[key]);
  });

  try {
    // Fetch data from the API
    const response = await fetch(url.href);

    // Get the response body as text (JSON or other formats)
    const body = await response.text();

    // Return the status code and body as-is
    context.res = {
      status: response.status,
      body: body,
    };
  } catch (error) {
    console.error("Error occurred while fetching data:", error);

    // Return the error in case of a failure
    context.res = {
      status: 500,
      body: JSON.stringify({
        message: error.message || "Internal Server Error",
      }),
    };
  }
};
