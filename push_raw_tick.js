const WebSocketClient = require("./webSocketClient.js");
const { TOTP } = require("totp-generator");
const StreamWriter = require("./streamWriter.js");
const { secondThousandStocks, FirstThousandStocks } = require("./stockList.js");

// Shared CSV writer
const writer = new StreamWriter("all_day_tick.csv");
// const totalSize = secondThousandStocks.length;
// console.log("Total stocks:", totalSize)
// // Common tick handler
function handleTick(socketName, data) {
  try {
    if (!data) return console.warn(`[${socketName}] Empty tick, skipping...`);

    const token = (data.token || "").replace(/"/g, "");
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

    //console.log(`[${socketName}] [${token}] LTP: ${price}, Volume: ${volume}, Avg: ${avgPrice}, OI: ${oi}`);
    console.log("incoming")
    // Write to CSV
    const row = `${token},${price},${avgPrice},${volume},${oi},${totalBuyQty},${totalSellQty},${open},${high},${low},${close}\n`;
    writer.appendBatch([row]).catch(err => console.warn(`[${socketName}] CSV write failed:`, err.message));

  } catch (err) {
    console.warn(`[${socketName}] Tick parse error:`, err.message, data);
  }
}

// Helper to subscribe tokens
function subscribeTokens(socket, batch) {
  const bseTokens = batch.filter(t => t.ticker_exchange === "BSE").map(t => t.ticker_security_code);
  const nseTokens = batch.filter(t => t.ticker_exchange === "NSE").map(t => t.ticker_security_code);

  if (bseTokens.length) {
    socket.webSocket.fetchData({ correlationID: "bse_0", action: 1, mode: 3, exchangeType: 3, tokens: bseTokens });
  }
  if (nseTokens.length) {
    socket.webSocket.fetchData({ correlationID: "nse_0", action: 1, mode: 3, exchangeType: 1, tokens: nseTokens });
  }
}

// Main async function
(async function main() {
  try {
    // -------------------------
    // WebSocket 1
    // -------------------------
    const otp1 = TOTP.generate("Z7FECDUC3C4QV65OZ42DJXXZHA").otp || TOTP.generate("Z7FECDUC3C4QV65OZ42DJXXZHA");
    const socket1 = await WebSocketClient.create({
      apiKey: "s1Yf9hbT",
      clientCode: "N118372",
      password: "7290",
      totp: otp1,
      name: "AngelSocket1",
    });
    socket1.onTick = data => handleTick("Socket1", data);

    // const batch1 = [
    //   { ticker_exchange: "NSE", ticker_id: "11324", ticker: "MAHABANK", ticker_security_code: "11377" },
    // ];
    const batch1 = secondThousandStocks.slice(0, 1000)
    subscribeTokens(socket1, batch1);

    // -------------------------
    // WebSocket 2
    // -------------------------
    const otp2 = TOTP.generate("EFMARK3LNRQ2AELUQ6B4RCZAMQ").otp;
    console.log("here is  second  " ,otp2)
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

  } catch (err) {
    console.error("Error initializing WebSockets:", err.message);
  }
})();
 