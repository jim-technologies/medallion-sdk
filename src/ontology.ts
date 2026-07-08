import type { ProtocolOntologyClient } from "./protocol.js";
import { idempotencyKey } from "./payload.js";
import type {
  ExecuteActionInput,
  ExecuteActionResponse,
  PlanActionInput,
  PlanActionResponse,
  QueryInput,
  QueryResponse,
  RequestOptions,
} from "./types.js";

export class OntologyClient {
  constructor(private readonly protocol: ProtocolOntologyClient) {}

  async query(
    input: QueryInput,
    options: RequestOptions = {},
  ): Promise<QueryResponse> {
    const response = await this.protocol.query<{
      answer?: string;
      resource_ids?: string[];
      explanations?: string[];
      events?: unknown[];
    }>(
      {
        question: input.question,
        include_inferred: input.includeInferred,
      },
      options,
    );

    return {
      requestId: response.requestId,
      answer: response.body.answer,
      resourceIds: response.body.resource_ids ?? [],
      explanations: response.body.explanations ?? [],
      events: (response.body.events ?? []).map((event) => ({
        ...(event as Record<string, unknown>),
      })),
    };
  }

  async planAction(
    input: PlanActionInput,
    options: RequestOptions = {},
  ): Promise<PlanActionResponse> {
    const response = await this.protocol.planAction<{
      plan?: Record<string, unknown>;
      required_approvals?: string[];
    }>(
      input.actionName,
      {
        input: input.input,
      },
      options,
    );

    return {
      requestId: response.requestId,
      plan: actionInvocationFromWire(response.body.plan),
      requiredApprovals: response.body.required_approvals ?? [],
    };
  }

  async executeAction(
    input: ExecuteActionInput,
    options: RequestOptions = {},
  ): Promise<ExecuteActionResponse> {
    const key = idempotencyKey(input.idempotencyKey);
    const response = await this.protocol.executeAction<{
      invocation?: Record<string, unknown>;
    }>(
      input.actionName,
      {
        idempotency_key: key,
        input: input.input,
      },
      options,
    );
    const invocation = actionInvocationFromWire(response.body.invocation);
    const status = invocation?.status ?? "";

    return {
      requestId: response.requestId,
      idempotencyKey: invocation?.idempotencyKey ?? key,
      duplicate: false,
      result: actionResult(status),
      invocation,
    };
  }
}

function actionInvocationFromWire(value: unknown) {
  if (value === undefined || value === null || typeof value !== "object") {
    return undefined;
  }

  const item = value as Record<string, unknown>;

  return {
    id: stringValue(item.id),
    tenantId: stringValue(item.tenant_id),
    actionName: stringValue(item.action_name),
    actorPrincipal: stringValue(item.actor_principal),
    idempotencyKey: stringValue(item.idempotency_key),
    request: item.request,
    response: item.response,
    status: stringValue(item.status),
    explanation: stringValue(item.explanation),
    errorMessage: stringValue(item.error_message),
    createdAt: stringValue(item.created_at),
    resourceId: stringValue(item.resource_id),
  };
}

function actionResult(
  status: string,
): ExecuteActionResponse["result"] {
  switch (status) {
    case "ACTION_INVOCATION_STATUS_SUCCEEDED":
      return "succeeded";
    case "ACTION_INVOCATION_STATUS_FAILED":
      return "failed";
    case "ACTION_INVOCATION_STATUS_REJECTED":
      return "rejected";
    default:
      return "accepted";
  }
}

function stringValue(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}
