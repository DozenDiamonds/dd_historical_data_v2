const fs = require("fs");
const path = require("path");
const uploadLargeFileMultipart = require("./s3_uploader");
require("dotenv").config();


// -------------------- Get latest CSV --------------------
function getLatestCSVFile() {
  const dir = __dirname;

  const files = fs.readdirSync(dir)
    .filter(file => file.endsWith(".csv"))
    .map(file => ({
      name: file,
      fullPath: path.join(dir, file),
      timestamp: fs.statSync(path.join(dir, file)).mtime.getTime()
    }));

  if (files.length === 0) return null;

  files.sort((a, b) => b.timestamp - a.timestamp);
  return files[0].fullPath;
}

// -------------------- Safe delete --------------------
function safeDelete(filePath) {
  try {
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
      console.log("Deleted CSV:", filePath);
    }
  } catch (err) {
    console.error("Could not delete CSV:", err.message);
  }
}

// -------------------- Main function to upload latest CSV --------------------
async function uploadLatestCSV() {
  const latestCSV = getLatestCSVFile();
  if (!latestCSV) {
    console.log("No CSV file found to upload.");
    return;
  }

  console.log("Latest CSV file:", latestCSV);

  try {
    await uploadLargeFileMultipart(process.env.S3_BUCKET, latestCSV);
    console.log("Upload completed. Deleting file...");
    safeDelete(latestCSV);
  } catch (err) {
    console.error("Upload failed:", err.message);
  }
}

// Export the function
module.exports = uploadLatestCSV;
