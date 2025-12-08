const fs = require("fs");
const path = require("path");
const uploadLargeFileMultipart = require("./s3_uploader");
require("dotenv").config();



// -------------------- Get Latest CSV --------------------
function getLatestCSVFile() {
  const dir = __dirname;

  const files = fs.readdirSync(dir)
    .filter(file => file.endsWith(".csv"))
    .map(file => ({
      name: file,
      fullPath: path.join(dir, file),
      timestamp: fs.statSync(path.join(dir, file)).mtime.getTime()
    }));

  if (files.length === 0) {
    return null;
  }

  files.sort((a, b) => b.timestamp - a.timestamp);
  return files[0].fullPath;
}

// -------------------- Safe Delete --------------------
function safeDelete(filePath) {
  try {
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
      console.log("Deleted:", filePath);
    }
  } catch (err) {
    console.error("Delete error:", err.message);
  }
}

// -------------------- Test Function --------------------
async function testUpload() {
  console.log("Testing CSV Upload...");

  const latestCSV = getLatestCSVFile();
  if (!latestCSV) {
    console.log("No CSV found.");
    return;
  }

  console.log("Latest CSV found:", latestCSV);

  try {
    await uploadLargeFileMultipart(process.env.S3_BUCKET, latestCSV);
    console.log("Upload complete, deleting file...");
    safeDelete(latestCSV);
  } catch (err) {
    console.error("Upload failed:", err.message);
  }
}

testUpload();
