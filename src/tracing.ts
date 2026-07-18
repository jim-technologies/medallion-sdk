import {
  context,
  propagation,
  type Span,
  SpanKind,
  SpanStatusCode,
  type Tracer,
  trace,
} from "@opentelemetry/api";

export interface TracingOptions {
  enabled?: boolean;
  tracer?: Tracer;
  tracerName?: string;
  spanPrefix?: string;
}

export type TracingConfig = boolean | TracingOptions;

export interface NormalizedTracing {
  enabled: boolean;
  tracer: Tracer;
  spanPrefix: string;
}

export interface TraceRequestOptions<TResponse> {
  method: string;
  path: string;
  run: (span?: Span) => Promise<TResponse>;
  headers: Headers;
}

const DEFAULT_TRACER_NAME = "@jimtech/medallion";
const DEFAULT_SPAN_PREFIX = "medallion";

export function normalizeTracing(config?: TracingConfig): NormalizedTracing {
  if (config === undefined || config === false) {
    return {
      enabled: false,
      tracer: trace.getTracer(DEFAULT_TRACER_NAME),
      spanPrefix: DEFAULT_SPAN_PREFIX,
    };
  }

  if (config === true) {
    return {
      enabled: true,
      tracer: trace.getTracer(DEFAULT_TRACER_NAME),
      spanPrefix: DEFAULT_SPAN_PREFIX,
    };
  }

  const enabled = config.enabled ?? config.tracer !== undefined;
  return {
    enabled,
    tracer:
      config.tracer ??
      trace.getTracer(config.tracerName ?? DEFAULT_TRACER_NAME),
    spanPrefix: config.spanPrefix ?? DEFAULT_SPAN_PREFIX,
  };
}

export async function traceRequest<TResponse>(
  tracing: NormalizedTracing,
  options: TraceRequestOptions<TResponse>,
): Promise<TResponse> {
  if (!tracing.enabled) {
    return options.run(undefined);
  }

  const span = tracing.tracer.startSpan(
    `${tracing.spanPrefix} ${options.method} ${options.path}`,
    {
      kind: SpanKind.CLIENT,
      attributes: {
        "medallion.sdk.language": "typescript",
        "medallion.request.path": options.path,
        "http.request.method": options.method,
      },
    },
  );

  return context.with(trace.setSpan(context.active(), span), async () => {
    propagation.inject(context.active(), options.headers, {
      set: (carrier, key, value) => {
        carrier.set(key, value);
      },
    });

    try {
      const response = await options.run(span);
      span.setStatus({ code: SpanStatusCode.OK });
      return response;
    } catch (error) {
      recordSpanException(span, error);
      throw error;
    } finally {
      span.end();
    }
  });
}

export function setResponseSpanAttributes(
  span: Span | undefined,
  status: number,
  requestId?: string,
): void {
  if (span === undefined) {
    return;
  }

  span.setAttribute("http.response.status_code", status);
  if (requestId !== undefined) {
    span.setAttribute("medallion.request_id", requestId);
  }
}

function recordSpanException(span: Span, error: unknown): void {
  span.recordException(error instanceof Error ? error : String(error));
  span.setStatus({
    code: SpanStatusCode.ERROR,
    message: error instanceof Error ? error.message : undefined,
  });
}
