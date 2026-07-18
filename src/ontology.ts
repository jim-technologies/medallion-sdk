import { MedallionError } from "./errors.js";
import { requiredIdempotencyKey } from "./payload.js";
import type { ProtocolOntologyClient } from "./protocol.js";
import type {
  ActionInvocation,
  AuditTrailEvent,
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
      events: (response.body.events ?? []).map(auditEventFromWire),
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

    const plan = requiredActionInvocation(
      response.body.plan,
      "plan",
      response.requestId,
    );

    return {
      requestId: response.requestId,
      plan,
      requiredApprovals: response.body.required_approvals ?? [],
    };
  }

  async executeAction(
    input: ExecuteActionInput,
    options: RequestOptions = {},
  ): Promise<ExecuteActionResponse> {
    const key = requiredIdempotencyKey(
      input.idempotencyKey,
      "ontology.executeAction.idempotencyKey",
    );
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
    const invocation = requiredActionInvocation(
      response.body.invocation,
      "execution",
      response.requestId,
    );
    if (
      invocation.actionName !== input.actionName ||
      invocation.idempotencyKey !== key
    ) {
      throw new MedallionError(
        "Medallion returned an action execution for a different action or idempotency key.",
        {
          code: "MEDALLION_INVALID_ACTION_RESPONSE",
          requestId: response.requestId,
        },
      );
    }
    const status = invocation.status;

    return {
      requestId: response.requestId,
      idempotencyKey: invocation.idempotencyKey,
      result: actionResult(status),
      invocation,
    };
  }
}

function requiredActionInvocation(
  value: unknown,
  kind: "plan" | "execution",
  requestId: string | undefined,
): ActionInvocation & {
  id: string;
  tenantId: string;
  actionName: string;
  actorPrincipal: string;
  status: string;
} {
  const invocation = actionInvocationFromWire(value);
  if (
    invocation === undefined ||
    !nonEmptyString(invocation.id) ||
    !nonEmptyString(invocation.tenantId) ||
    !nonEmptyString(invocation.actionName) ||
    !nonEmptyString(invocation.actorPrincipal) ||
    !nonEmptyString(invocation.status) ||
    invocation.status === "ACTION_INVOCATION_STATUS_UNSPECIFIED"
  ) {
    throw new MedallionError(
      `Medallion returned an incomplete action ${kind}.`,
      {
        code: "MEDALLION_INVALID_ACTION_RESPONSE",
        requestId,
      },
    );
  }
  return invocation as ActionInvocation & {
    id: string;
    tenantId: string;
    actionName: string;
    actorPrincipal: string;
    status: string;
  };
}

function actionInvocationFromWire(
  value: unknown,
): ActionInvocation | undefined {
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

function auditEventFromWire(value: unknown): AuditTrailEvent {
  const item = recordValue(value);
  const actorPrincipal = stringValue(item.actor_principal);
  const eventId = stringValue(item.id);

  return {
    id: eventId,
    eventId,
    tenantId: stringValue(item.tenant_id),
    actor: actorFromPrincipal(actorPrincipal),
    actorPrincipal,
    action: stringValue(item.action),
    targetType: stringValue(item.target_type),
    targetId: stringValue(item.target_id),
    entityType: stringValue(item.target_type),
    entityId: stringValue(item.target_id),
    requestId: stringValue(item.request_id),
    metadata: optionalRecordValue(item.metadata),
    createdAt: stringValue(item.created_at),
    before: item.before,
    after: item.after,
    evidenceUrl: stringValue(item.evidence_url),
    sourceEventId: stringValue(item.source_event_id),
  };
}

function actorFromPrincipal(
  value: string | undefined,
): AuditTrailEvent["actor"] {
  if (value === undefined || value.length === 0) {
    return undefined;
  }

  const parts = value.split(":");
  if (parts.length < 2) {
    return { id: value };
  }
  const type = parts.shift();
  if (parts.length === 1) {
    return { type, id: parts.join(":") };
  }
  const provider = parts.shift();
  return { type, provider, id: parts.join(":") };
}

function recordValue(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function optionalRecordValue(
  value: unknown,
): Record<string, unknown> | undefined {
  const record = recordValue(value);
  return Object.keys(record).length === 0 && value === undefined
    ? undefined
    : record;
}

function actionResult(status: string): ExecuteActionResponse["result"] {
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

function nonEmptyString(value: string | undefined): value is string {
  return value !== undefined && value.trim().length > 0;
}
