const {
  S3Client,
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand
} = require("@aws-sdk/client-s3");
require("dotenv").config();
const fs = require("fs");
const path = require("path");
const mime = require("mime-types");
const { createLogger, transports, format } = require("winston");

// ------------------- GLOBAL S3 KEY PREFIX -------------------
const KEY_PREFIX = "historical_data_v2/";

// ------------------- LOGGER -------------------
const logger = createLogger({
  level: "info",
  format: format.combine(
    format.timestamp(),
    format.printf(({ timestamp, level, message }) => {
      return `[${timestamp}] [${level.toUpperCase()}] ${message}`;
    })
  ),
  transports: [
    new transports.Console(),
    new transports.File({ filename: "s3_multipart_upload.log" })
  ]
});

// ------------------- S3 CLIENT -------------------
const s3 = new S3Client({
  region: "ap-south-1",
  credentials: {
    accessKeyId: process.env.AWS_KEY,
    secretAccessKey: process.env.AWS_SECRET,
  },
});

// ------------------- PROGRESS BAR FUNCTION -------------------
function drawProgressBar(percentage) {
  const width = 40;
  const filled = Math.round((percentage / 100) * width);
  const empty = width - filled;

  return `[${"=".repeat(filled)}${" ".repeat(empty)}] ${percentage.toFixed(2)}%`;
}

// ------------------- MAIN MULTIPART UPLOAD FUNCTION -------------------
async function uploadLargeFileMultipart(bucket, filePath) {
  const fileName = path.basename(filePath);
  const contentType = mime.lookup(fileName) || "application/octet-stream";

  logger.info(`Starting multipart upload → ${fileName}`);

  const PART_SIZE = 100 * 1024 * 1024;

  if (!fs.existsSync(filePath)) {
    logger.error("File not found: " + filePath);
    throw new Error("File not found");
  }

  const fileSize = fs.statSync(filePath).size;
  const fileStream = fs.createReadStream(filePath, { highWaterMark: PART_SIZE });

  let uploadedBytes = 0;
  let uploadId = null;
  let partNumber = 1;
  const uploadedParts = [];

  try {
    const createRes = await s3.send(
      new CreateMultipartUploadCommand({
        Bucket: bucket,
        Key: KEY_PREFIX + fileName,
        ContentType: contentType,
      })
    );

    uploadId = createRes.UploadId;
    logger.info(`Multipart started (UploadId: ${uploadId})`);

    for await (const chunk of fileStream) {
      logger.info(`Uploading part #${partNumber} (${chunk.length} bytes)`);

      let retries = 3;
      let partResponse;

      while (retries > 0) {
        try {
          partResponse = await s3.send(
            new UploadPartCommand({
              Bucket: bucket,
              Key: KEY_PREFIX + fileName,
              UploadId: uploadId,
              PartNumber: partNumber,
              Body: chunk,
            })
          );
          break;
        } catch (err) {
          retries--;
          logger.error(`Part ${partNumber} failed. Retries left: ${retries}`);
          if (retries === 0) throw err;
        }
      }

      uploadedBytes += chunk.length;

      const percent = (uploadedBytes / fileSize) * 100;
      process.stdout.write(
        `\r${drawProgressBar(percent)}  (${(uploadedBytes / 1024 / 1024).toFixed(2)}MB / ${(fileSize / 1024 / 1024).toFixed(2)}MB)`
      );

      uploadedParts.push({
        ETag: partResponse.ETag,
        PartNumber: partNumber,
      });

      partNumber++;
    }

    process.stdout.write("\n");

    await s3.send(
      new CompleteMultipartUploadCommand({
        Bucket: bucket,
        Key: KEY_PREFIX + fileName,
        UploadId: uploadId,
        MultipartUpload: { Parts: uploadedParts },
      })
    );

    logger.info(`Upload completed successfully → ${fileName}`);
    return { success: true, fileName };

  } catch (err) {
    logger.error("Upload failed: " + err.message);

    if (uploadId) {
      logger.error("Aborting multipart upload...");
      await s3.send(
        new AbortMultipartUploadCommand({
          Bucket: bucket,
          Key: KEY_PREFIX + fileName,
          UploadId: uploadId,
        })
      );
    }

    throw err;
  }
}

module.exports = uploadLargeFileMultipart; 
// (async () => {
//   try {
//     await uploadLargeFileMultipart(
//       process.env.S3_BUCKET,
//       "C:\\Users\\HP\\Desktop\\dd_h2\\all_day_tick_2025-12-04.csv"     // <- change your file here
//     );
//   } catch (err) {
//     console.error("UPLOAD ERROR:", err);
//   }
// })();   