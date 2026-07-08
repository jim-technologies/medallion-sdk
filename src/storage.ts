import { Buffer } from "node:buffer";

import { MedallionError } from "./errors.js";
import type { ProtocolStorageClient } from "./protocol.js";
import type {
  RequestOptions,
  StorageUploadInput,
  StorageUploadResponse,
} from "./types.js";

export class StorageClient {
  constructor(private readonly protocol: ProtocolStorageClient) {}

  async upload(
    input: StorageUploadInput,
    options: RequestOptions = {},
  ): Promise<StorageUploadResponse> {
    const org = input.org ?? input.bucket;
    if (org === undefined || org.trim().length === 0) {
      throw new MedallionError("storage.upload requires org or bucket.", {
        code: "MEDALLION_MISSING_STORAGE_ORG",
      });
    }

    const response = await this.protocol.upload<Record<string, unknown>>(
      {
        org,
        repo: input.repo,
        path: input.path,
        content_type: input.contentType,
        data: await bodyInitToBase64(input.data),
        request_id: input.idempotencyKey,
      },
      {
        ...options,
        idempotencyKey: input.idempotencyKey,
      },
    );

    return {
      requestId: response.requestId,
      result: "uploaded",
      org,
      path: input.path,
      entry: recordValue(response.body.entry) ?? {},
    };
  }
}

async function bodyInitToBase64(data: BodyInit): Promise<string> {
  try {
    if (data instanceof ArrayBuffer) {
      return Buffer.from(data).toString("base64");
    }

    if (ArrayBuffer.isView(data)) {
      return Buffer.from(data.buffer, data.byteOffset, data.byteLength).toString(
        "base64",
      );
    }

    const buffer = await new Response(data).arrayBuffer();
    return Buffer.from(buffer).toString("base64");
  } catch (error) {
    throw new MedallionError(
      "storage.upload data must be a readable BodyInit value.",
      { code: "MEDALLION_INVALID_STORAGE_UPLOAD_BODY", cause: error },
    );
  }
}

function recordValue(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}
