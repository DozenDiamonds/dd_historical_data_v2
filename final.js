const WebSocketClient = require("./webSocketClientV2");
const { TOTP } = require("totp-generator");
const StreamWriter = require("./streamWriter.js");
const { secondThousandStocks, FirstThousandStocks } = require("./stockList.js");
const moment = require("moment-timezone");

// ----------------------
// Stream Writer for Candles
// ----------------------
const writer = new StreamWriter("all_day_candle.csv");

// ----------------------
// Candle Storage
// ----------------------
const candleMap2 = new Map();
const tokenToNameMap = new Map();

// ----------------------
// Helper: Get 1-min candle key
// ----------------------
function getMinuteKey(token, timestamp) {
  const minuteTime = moment(timestamp).tz("Asia/Kolkata").seconds(0).milliseconds(0);
  return `${token}_${minuteTime.toISOString()}`;
}

// ----------------------
// Tick -> Candle Builder
// ----------------------
function handleTick(socketName, data) {
  try {
    if (!data) return;

    const token = (data.token || "").replace(/"/g, "");
    const timestamp = Number(data.last_traded_timestamp) * 1000; // ms
    const price = parseFloat(data.last_traded_price || 0) / 100;
    const volume = parseFloat(data.vol_traded || 0);
    const oi = parseFloat(data.open_interest || 0);
    const avgPrice = parseFloat(data.avg_traded_price || 0) / 100;
    const totalBuyQty = parseFloat(data.total_buy_quantity || 0);
    const totalSellQty = parseFloat(data.total_sell_quantity || 0);
    const exchangeMap = { "1": "NSE", "3": "BSE" };
    const exchange = exchangeMap[data.exchange_type];

    // resolve ticker
    let ticker = tokenToNameMap.get(token);
    if (!ticker) {
      const match = [...secondThousandStocks, ...FirstThousandStocks].find(s => s.ticker_security_code === token);
      if (match) {
        ticker = match.ticker;
        tokenToNameMap.set(token, ticker);
      } else return;
    }

    const minuteKey = getMinuteKey(token, timestamp);
    const existing = candleMap2.get(minuteKey);

    if (existing) {
      existing.high = Math.max(existing.high, price);
      existing.low = Math.min(existing.low, price);
      existing.close = price;
      existing.volume += volume;
      existing.openInterest = oi;
      existing.avgPrice = avgPrice;
      existing.totalBuyQty = totalBuyQty;
      existing.totalSellQty = totalSellQty;
    } else {
      const candleTime = moment(timestamp).tz("Asia/Kolkata").seconds(0).milliseconds(0);
      candleMap2.set(minuteKey, {
        token,
        ticker,
        timestamp: candleTime,
        open: price,
        high: price,
        low: price,
        close: price,
        volume,
        openInterest: oi,
        avgPrice,
        totalBuyQty,
        totalSellQty,
        exchange,
      });
    }
  } catch (err) {
    console.warn(`[${socketName}] Tick parse error:`, err.message);
  }
}

// ----------------------
// Write candle map to CSV
// ----------------------
async function writeDataToCSV(candleMap) {
  for (const [key, candle] of candleMap.entries()) {
    const timestamp = moment(candle.timestamp).tz("Asia/Kolkata");
    const date = timestamp.format("YYYY-MM-DD");
    const time = timestamp.format("HH:mm:ss");

    const row = `${candle.ticker},${candle.token},${date},${time},${candle.open},${candle.high},${candle.low},${candle.close},${candle.volume},${candle.openInterest},${candle.avgPrice},${candle.totalBuyQty},${candle.totalSellQty},${candle.exchange}\n`;

    await writer.appendBatch([row]);
    candleMap.delete(key);
    console.log(`✅ Candle written to CSV: ${candle.ticker} @ ${time}`);
  }
}

// ----------------------
// Subscribe helper
// ----------------------
function subscribeTokens(socket, batch) {
  const bseTokens = batch.filter(t => t.ticker_exchange === "BSE").map(t => t.ticker_security_code);
  const nseTokens = batch.filter(t => t.ticker_exchange === "NSE").map(t => t.ticker_security_code);

  if (bseTokens.length) socket.webSocket.fetchData({ correlationID: "bse_0", action: 1, mode: 3, exchangeType: 3, tokens: bseTokens });
  if (nseTokens.length) socket.webSocket.fetchData({ correlationID: "nse_0", action: 1, mode: 3, exchangeType: 1, tokens: nseTokens });
}

// ----------------------
// Main Async Function
// ----------------------

(async function main() {
  try {
    // WebSocket 1
    const otp1 = TOTP.generate("Z7FECDUC3C4QV65OZ42DJXXZHA").otp;
    const socket1 = await WebSocketClient.create({
      apiKey: "s1Yf9hbT",
      clientCode: "N118372",
      password: "7290",
      totp: otp1,
      name: "AngelSocket1",
    });
    socket1.onTick = data => handleTick("Socket1", data);
    subscribeTokens(socket1, secondThousandStocks.slice(0, 1000));

    // WebSocket 2
    const otp2 = TOTP.generate("EFMARK3LNRQ2AELUQ6B4RCZAMQ").otp;
    const socket2 = await WebSocketClient.create({
      apiKey: "PusPiItS",
      clientCode: "AAAN721882",
      password: "0707",
      totp: otp2,
      name: "AngelSocket2",
    });
    socket2.onTick = data => handleTick("Socket2", data);
    subscribeTokens(socket2, FirstThousandStocks.slice(0, 1000));

    // ----------------------
    // Interval: Check every 5 seconds for 1-min candles
    // ----------------------
    setInterval(async () => {
      const now = moment().tz("Asia/Kolkata").seconds(0).milliseconds(0);

      for (const [key, candle] of candleMap2.entries()) {
        const candleTime = moment(candle.timestamp).tz("Asia/Kolkata").seconds(0).milliseconds(0);
        if (now.diff(candleTime) >= 60 * 1000) {
          try {
            await writeDataToCSV(candleMap2);
          } catch (err) {
            console.error(` Failed to write candle for ${candle.ticker}`, err);
          }
        }
      }
    }, 5000);

  } catch (err) {
    console.error("Error initializing WebSockets:", err.message);
  }
})();
//--------------- - -------
