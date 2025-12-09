const fs = require("fs");
const path = require("path");
const cron = require("node-cron");
const moment = require("moment-timezone");

const { get_ticks, close_all_sockets } = require("./push_raw_tick");
const uploadLatestCSV = require("./csvUploader");

// -------------------------------------------------------------------
// 1) Ensure the cron_logs folder exists for daily log rotation
// -------------------------------------------------------------------
const logDir = path.join(__dirname, "cron_logs");
if (!fs.existsSync(logDir)) fs.mkdirSync(logDir);

// -------------------------------------------------------------------
// 2) Function to get today's log file path (auto-rotates daily)
// -------------------------------------------------------------------
function getLogFilePath() {
  const today = moment().tz("Asia/Kolkata").format("YYYY-MM-DD");
  return path.join(logDir, `cron_${today}.log`);
}

// -------------------------------------------------------------------
// 3) Logger function: writes log to file with timestamp and prints to console
// -------------------------------------------------------------------
function log(message) {
  const timestamp = moment().tz("Asia/Kolkata").format("YYYY-MM-DD HH:mm:ss");
  const fullMessage = `[${timestamp}] ${message}\n`;
  fs.appendFileSync(getLogFilePath(), fullMessage);
  console.log(fullMessage.trim());
}

// -------------------------------------------------------------------
// 4) Cron Job: Start WebSocket tick collection at 09:15 AM (Mon–Fri IST)
// -------------------------------------------------------------------
cron.schedule("15 9 * * 1-5", async () => {
  log("Starting WebSocket tick collection...");
  try {
    await get_ticks();
    log("get_ticks started successfully.");
  } catch (err) {
    log("Error starting get_ticks: " + err.message);
  }
}, { timezone: "Asia/Kolkata" });

// -------------------------------------------------------------------
// 5) Cron Job: Close all WebSocket connections at 03:45 PM (Mon–Fri IST)
// -------------------------------------------------------------------
cron.schedule("45 15 * * 1-5", () => {
  log("Closing all WebSocket connections...");
  try {
    close_all_sockets();
    log("All sockets closed successfully.");
  } catch (err) {
    log("Error closing sockets: " + err.message);
  }
}, { timezone: "Asia/Kolkata" });

// -------------------------------------------------------------------
// 6) Cron Job: Upload latest CSV to S3 and delete from local at 04:15 PM (Mon–Fri IST)
// -------------------------------------------------------------------
cron.schedule("15 16 * * 1-5", async () => {
  log("Starting CSV upload cron...");
  try {
    await uploadLatestCSV();
    log("CSV upload cron finished successfully.");
  } catch (err) {
    log("CSV upload failed: " + err.message);
  }
}, { timezone: "Asia/Kolkata" });

// -------------------------------------------------------------------
// 7) Initial log to indicate cron service is running
// -------------------------------------------------------------------
log("Cron service started: get_ticks at 09:15 AM, close sockets at 03:45 PM, upload CSV at 04:15 PM (Mon–Fri IST)");
