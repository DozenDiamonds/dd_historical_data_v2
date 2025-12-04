const {
  S3Client,
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand
} = require("@aws-sdk/client-s3");

const fs = require("fs");
const path = require("path");
const mime = require("mime-types");
const { createLogger, transports, format } = require("winston");

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

// ------------------- MAIN MULTIPART UPLOAD FUNCTION -------------------
async function uploadLargeFileMultipart(bucket, filePath) {
  const fileName = path.basename(filePath);
  const contentType = mime.lookup(fileName) || "application/octet-stream";

  logger.info(`Starting multipart upload → ${fileName}`);

  // Chunk size (100 MB per part)
  const PART_SIZE = 100 * 1024 * 1024;

  // Validate file
  if (!fs.existsSync(filePath)) {
    logger.error("File not found: " + filePath);
    throw new Error("File not found");
  }

  const fileSize = fs.statSync(filePath).size;
  const fileStream = fs.createReadStream(filePath, { highWaterMark: PART_SIZE });

  let uploadId = null;
  let partNumber = 1;
  const uploadedParts = [];

  try {
    // 1️⃣ CREATE MULTIPART UPLOAD
    const createRes = await s3.send(
      new CreateMultipartUploadCommand({
        Bucket: bucket,
        Key: "large_uploads/" + fileName,
        ContentType: contentType,
      })
    );

    uploadId = createRes.UploadId;
    logger.info(`Multipart started (UploadId: ${uploadId})`);

    // 2️⃣ UPLOAD PARTS IN STREAM
    for await (const chunk of fileStream) {
      logger.info(`Uploading part #${partNumber} (${chunk.length} bytes)`);

      let retries = 3;
      let partResponse;

      while (retries > 0) {
        try {
          partResponse = await s3.send(
            new UploadPartCommand({
              Bucket: bucket,
              Key: "large_uploads/" + fileName,
              UploadId: uploadId,
              PartNumber: partNumber,
              Body: chunk,
            })
          );
          break;
        } catch (err) {
          retries--;
          logger.error(`Part ${partNumber} failed, retries left: ${retries}`);
          if (retries === 0) throw err;
        }
      }

      uploadedParts.push({
        ETag: partResponse.ETag,
        PartNumber: partNumber,
      });

      logger.info(`Part #${partNumber} uploaded successfully`);
      partNumber++;
    }

    // 3️⃣ COMPLETE MULTIPART
    await s3.send(
      new CompleteMultipartUploadCommand({
        Bucket: bucket,
        Key: "large_uploads/" + fileName,
        UploadId: uploadId,
        MultipartUpload: { Parts: uploadedParts },
      })
    );

    logger.info(`Upload completed successfully → ${fileName}`);
    return { success: true, fileName };

  } catch (err) {
    logger.error("Upload failed: " + err.message);

    // If upload failed, abort it so S3 doesn't store partial data
    if (uploadId) {
      logger.error("Aborting multipart upload...");
      await s3.send(
        new AbortMultipartUploadCommand({
          Bucket: bucket,
          Key: "large_uploads/" + fileName,
          UploadId: uploadId,
        })
      );
    }

    throw err;
  }
}

// ------------------- EXPORT FUNCTION -------------------
module.exports = uploadLargeFileMultipart;
