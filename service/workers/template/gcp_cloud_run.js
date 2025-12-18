const functions = require("@google-cloud/functions-framework");

const baseUrl = new URL("http://api.bilibili.com/x/web-interface/newlist");

functions.http("proxyRequest", async (req, res) => {
  const queryParams = req.query || {};

  // Append query parameters to the base URL
  const url = new URL(baseUrl.href); // Clone baseUrl to avoid modifying the original
  Object.keys(queryParams).forEach((key) => {
    url.searchParams.append(key, queryParams[key]);
  });

  try {
    // Fetch data from the API
    const response = await fetch(url.href);

    // Get the response body as text (JSON or other formats)
    const body = await response.text();

    // Return the status code and body as-is
    res.status(response.status).send(body);
  } catch (error) {
    console.error("Error occurred while fetching data:", error);

    // Return the error in case of a failure
    res.status(500).json({
      message: error.message || "Internal Server Error",
    });
  }
});
