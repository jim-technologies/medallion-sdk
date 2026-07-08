export interface MedallionErrorOptions {
  code?: string;
  requestId?: string;
  cause?: unknown;
}

export class MedallionError extends Error {
  readonly code?: string;
  readonly requestId?: string;

  constructor(message: string, options: MedallionErrorOptions = {}) {
    super(message, { cause: options.cause });
    this.name = "MedallionError";
    this.code = options.code;
    this.requestId = options.requestId;
  }
}

export interface MedallionApiErrorOptions {
  status: number;
  requestId?: string;
  responseBody?: unknown;
}

export class MedallionApiError extends MedallionError {
  readonly status: number;
  readonly responseBody?: unknown;

  constructor(message: string, options: MedallionApiErrorOptions) {
    super(message, {
      code: "MEDALLION_API_ERROR",
      requestId: options.requestId,
    });
    this.name = "MedallionApiError";
    this.status = options.status;
    this.responseBody = options.responseBody;
  }
}
