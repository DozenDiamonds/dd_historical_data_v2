const WebSocketClient = require("./webSocketClient.js");
const { TOTP } = require("totp-generator");
const StreamWriter = require("./streamWriter.js");
const {
  secondThousandStocks,
  FirstThousandStocks,
  fivthThousandTickers,
  fourthThousandStocks,
  threeThousandStocks,
  seventhThousandTickers,
  sixthThousandTickers,
} = require("./stockList.js");
const moment = require("moment-timezone");

// Shared CSV writer
const writer = new StreamWriter("all_day_tick4.csv");

const tokenToNameMap = new Map();

// -------------------------------------------------------
// Preload token map
// -------------------------------------------------------
function preloadTokenMap(masterLists) {
  for (const list of masterLists) {
    for (const item of list) {
      tokenToNameMap.set(item.ticker_security_code, item.ticker);
    }
  }
  console.log("✓ Token map preloaded:", tokenToNameMap.size, "items");
}

// Call on startup
preloadTokenMap([
  FirstThousandStocks,
  secondThousandStocks,
  threeThousandStocks,
  fourthThousandStocks,
  fivthThousandTickers,
  sixthThousandTickers,
  seventhThousandTickers,
]);

// -------------------------------------------------------
// ✔ Final Safe Resolver
// -------------------------------------------------------
function resolveTickerName(token) {
  // 1) Fast lookup
  let ticker = tokenToNameMap.get(token);
  if (ticker) return ticker;

  // 2) Lookup in all master lists if missing
  const allLists = [
    FirstThousandStocks,
    secondThousandStocks,
    threeThousandStocks,
    fourthThousandStocks,
    fivthThousandTickers,
    sixthThousandTickers,
    seventhThousandTickers,
  ];

  for (const list of allLists) {
    const match = list.find(s => s.ticker_security_code === token);
    if (match) {
      tokenToNameMap.set(token, match.ticker); // Cache permanently
      return match.ticker;
    }
  }

  // 3) Hard fail (rare)
  console.warn("Token not found in ANY list:", token);
  return null;
}

// -------------------------------------------------------
// Tick Handler
// -------------------------------------------------------
function handleTick(socketName, data) {
  try {
    if (!data) return;
     
      const token = (data.token || "").replace(/"/g, "");
      
    if (data === "pong" || data === "PONG") {
      console.log(`[${socketName}] PONG`);
      return;
    }
    const ticker = resolveTickerName(token);
    if (!ticker) return;

    const price = parseFloat(data.last_traded_price || 0) / 100;
    const avgPrice = parseFloat(data.avg_traded_price || 0) / 100;
    const volume = parseFloat(data.vol_traded || 0);
    const oi = parseFloat(data.open_interest || 0) || 0;
    const totalBuyQty = parseFloat(data.total_buy_quantity || 0);
    const totalSellQty = parseFloat(data.total_sell_quantity || 0);
    const high = parseFloat(data.high_price_day || 0) / 100;
    const low = parseFloat(data.low_price_day || 0) / 100;
    const open = parseFloat(data.open_price_day || 0) / 100;
    const close = parseFloat(data.close_price || 0) / 100;

    const timestamp = Number(data.last_traded_timestamp) * 1000;
    const date_time = moment(timestamp)
      .tz("Asia/Kolkata")
      .format("YYYY-MM-DD HH:mm:ss");

    const row = `${ticker},${token},${date_time},${price},${avgPrice},${volume},${oi},${totalBuyQty},${totalSellQty},${open},${high},${low},${close}\n`;

    writer.appendBatch([row]).catch(err => {
      console.warn(`[${socketName}] Write failed`, err);
    });

  } catch (err) {
    console.warn(`[${socketName}] Tick error`, err);
  }
}

// -------------------------------------------------------
// Subscribe Helper
// -------------------------------------------------------
function subscribeTokens(socket, batch) {
  const bseTokens = batch
    .filter(t => t.ticker_exchange === "BSE")
    .map(t => t.ticker_security_code);

  const nseTokens = batch
    .filter(t => t.ticker_exchange === "NSE")
    .map(t => t.ticker_security_code);

  if (bseTokens.length) {
    socket.webSocket.fetchData({
      correlationID: "bse_0",
      action: 1,
      mode: 3,
      exchangeType: 3,
      tokens: bseTokens,
    });
  }

  if (nseTokens.length) {
    socket.webSocket.fetchData({
      correlationID: "nse_0",
      action: 1,
      mode: 3,
      exchangeType: 1,
      tokens: nseTokens,
    });
  }
}

// -------------------------------------------------------
// MAIN
// -------------------------------------------------------
(async function main() {
  try {
    // -------------------------
    // WebSocket 1
    // -------------------------
    const otp1 =
      TOTP.generate("Z7FECDUC3C4QV65OZ42DJXXZHA").otp ||
      TOTP.generate("Z7FECDUC3C4QV65OZ42DJXXZHA");

    const socket1 = await WebSocketClient.create({
      apiKey: "s1Yf9hbT",
      clientCode: "N118372",
      password: "7290",
      totp: otp1,
      name: "AngelSocket1",
    });

    socket1.onTick = data => handleTick("Socket1", data);
    const batch1 = secondThousandStocks.slice(0, 1000);
    subscribeTokens(socket1, batch1);

    // -------------------------
    // WebSocket 2
    // -------------------------
    const otp2 = TOTP.generate("EFMARK3LNRQ2AELUQ6B4RCZAMQ").otp;
    console.log("here is second:", otp2);

    const socket2 = await WebSocketClient.create({
      apiKey: "PusPiItS",
      clientCode: "AAAN721882",
      password: "0707",
      totp: otp2,
      name: "AngelSocket2",
    });

    socket2.onTick = data => handleTick("Socket2", data);
    const batch2 = FirstThousandStocks.slice(0, 1000);
    subscribeTokens(socket2, batch2);

    // -------------------------
    // WebSocket 3
    // -------------------------
    const otp3 = TOTP.generate("N72O7SVH4LA2PSFQVWXSQCFGLE").otp;

    const socket3 = await WebSocketClient.create({
      apiKey: "IC35Azf5 ",
      clientCode: "AAAO758700",
      password: "4083",
      totp: otp3,
      name: "AngelSocket3",
    });

    socket3.onTick = d => handleTick("Socket3", d);
    subscribeTokens(socket3, threeThousandStocks.slice(0, 1000));

    // -------------------------
    // WebSocket 4
    // -------------------------
    const otp4 = TOTP.generate("QOL7S7FJM6DPNW7B5B3T3CPY7I").otp;

    const socket4 = await WebSocketClient.create({
      apiKey: "C0R73OmP",
      clientCode: " AAAG163956",
      password: "2903",
      totp: otp4,
      name: "AngelSocket4",
    });

    socket4.onTick = d => handleTick("Socket4", d);
    subscribeTokens(socket4, fourthThousandStocks.slice(0, 1000));

    // -------------------------
    // WebSocket 5
    // -------------------------
    const otp5 = TOTP.generate("E55DTEDXBSI2IBDADJ5ITTP3ZY").otp;

    const socket5 = await WebSocketClient.create({
      apiKey: "APOrk5ObEI ",
      clientCode: "AABL771424",
      password: "4207",
      totp: otp5,
      name: "AngelSocket5",
    });

    socket5.onTick = d => handleTick("Socket5", d);
    subscribeTokens(socket5, fivthThousandTickers.slice(0, 1000));

  } catch (err) {
    console.error("Error initializing WebSockets:", err.message);
  }
})();
