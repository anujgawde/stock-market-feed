// Import required modules
var UpstoxClient = require("upstox-js-sdk");
const WebSocket = require("ws").WebSocket;
const protobuf = require("protobufjs");

// Initialize global variables
let protobufRoot = null;
let defaultClient = UpstoxClient.ApiClient.instance;
let apiVersion = "2.0";
let OAUTH2 = defaultClient.authentications["OAUTH2"];

// TODO: Access tokens are supposed to be generated when client logs in with their Upstox account (This is to be done via the redirection method).
// Access tokens are to be sent here to recieve real time market feed.
OAUTH2.accessToken = "";

const getMarketFeedUrl = async () => {
  return new Promise((resolve, reject) => {
    let apiInstance = new UpstoxClient.WebsocketApi();

    apiInstance.getMarketDataFeedAuthorize(
      apiVersion,
      (error, data, response) => {
        if (error) reject(error);
        else resolve(data.data.authorizedRedirectUri);
      }
    );
  });
};

const connectWebSocket = async (wsUrl) => {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(wsUrl, {
      headers: {
        "Api-Version": apiVersion,
        Authorization: "Bearer " + OAUTH2.accessToken,
      },
      followRedirects: true,
    });

    ws.on("open", () => {
      console.log("connected");
      resolve(ws);
      ``;
      // Set up interval to log data every 3 seconds
      setInterval(() => {
        const pingData = {
          guid: "someguid",
          method: "sub",
          data: {
            mode: "full",
            instrumentKeys: ["NSE_INDEX|Nifty Bank", "NSE_INDEX|Nifty 50"],
          },
        };
        ws.send(Buffer.from(JSON.stringify(pingData)));
        console.log("ping message sent");
      }, 3000);
    });

    ws.on("close", () => {
      console.log("disconnected, attempting to reconnect...");
      setTimeout(async () => {
        try {
          const newWsUrl = await getMarketFeedUrl();
          await connectWebSocket(newWsUrl);
        } catch (error) {
          console.error("Reconnection error:", error);
        }
      }, 5000);
    });

    ws.on("message", (data) => {
      console.log(JSON.stringify(decodeProtobuf(data)));
    });

    ws.on("error", (error) => {
      console.log("error:", error);
      reject(error);
    });
  });
};

const initProtobuf = async () => {
  protobufRoot = await protobuf.load(__dirname + "/MarketDataFeed.proto");
  console.log("Protobuf part initialization complete");
};

const decodeProtobuf = (buffer) => {
  if (!protobufRoot) {
    console.warn("Protobuf part not initialized yet!");
    return null;
  }

  const FeedResponse = protobufRoot.lookupType(
    "com.upstox.marketdatafeeder.rpc.proto.FeedResponse"
  );
  return FeedResponse.decode(buffer);
};

(async () => {
  try {
    await initProtobuf();
    const wsUrl = await getMarketFeedUrl();
    await connectWebSocket(wsUrl);
  } catch (error) {
    console.error("An error occurred:", error);
  }
})();
