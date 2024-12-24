const baseUrl = new URL("http://api.bilibili.com/x/web-interface/newlist");

export const handler = async (event) => {
  const queryParams = event.queryStringParameters || {};

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
    return {
      statusCode: response.status,
      body,
    };
  } catch (error) {
    console.error("Error occurred while fetching data:", error);

    // Return the error in case of a failure
    return {
      statusCode: 500,
      body: JSON.stringify({
        message: error.message || "Internal Server Error",
      }),
    };
  }
};
