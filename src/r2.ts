/**
 * R2 upload via the S3-compatible API. The bucket stays private; the Worker
 * streams playback itself, so no presigned URLs are minted here.
 */
import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { createReadStream } from "node:fs";
import { stat } from "node:fs/promises";

export interface R2Config {
  accountId: string;
  accessKeyId: string;
  secretAccessKey: string;
  bucket: string;
}

export function createR2Client(cfg: R2Config): S3Client {
  return new S3Client({
    region: "auto",
    endpoint: `https://${cfg.accountId}.r2.cloudflarestorage.com`,
    // R2 requires path-style addressing. Without this the SDK builds a
    // virtual-hosted URL (https://<bucket>.<account>.r2.cloudflarestorage.com)
    // whose hostname has no matching TLS certificate, so the handshake
    // fails with SSL alert 40 before any S3 call is made.
    forcePathStyle: true,
    credentials: {
      accessKeyId: cfg.accessKeyId,
      secretAccessKey: cfg.secretAccessKey,
    },
  });
}

export async function uploadFile(
  client: S3Client,
  bucket: string,
  key: string,
  filePath: string,
  contentType: string,
): Promise<void> {
  // Stat first, for two reasons (crash 2026-11-17: a thumb upload killed
  // the whole ingest process):
  // 1. The S3 SDK needs an explicit ContentLength for a stream body —
  //    without it the request fails with
  //    `Invalid value "undefined" for header "x-amz-decoded-content-length"`.
  // 2. If the file is missing, stat throws a clean ENOENT here (caught
  //    by the caller's try/catch) instead of letting createReadStream
  //    emit an unhandled 'error' event that crashes the Node process.
  const { size } = await stat(filePath);
  const stream = createReadStream(filePath);
  // Safety net: if the file vanishes mid-upload, the stream error must
  // reject the PutObject promise, never surface as an unhandled 'error'
  // event that kills the process.
  stream.on("error", () => {});
  await client.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: stream,
      ContentType: contentType,
      ContentLength: size,
    }),
  );
}
