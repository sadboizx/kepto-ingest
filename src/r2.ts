/**
 * R2 upload via the S3-compatible API. The bucket stays private; the Worker
 * streams playback itself, so no presigned URLs are minted here.
 */
import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { createReadStream } from "node:fs";

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
  await client.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: createReadStream(filePath),
      ContentType: contentType,
    }),
  );
}
