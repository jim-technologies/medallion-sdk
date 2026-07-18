import { MedallionError } from "./errors.js";
import { optionalId, requiredControlIdempotencyKey } from "./payload.js";
import type { ProtocolConnectClient } from "./protocol.js";
import type {
  Datasource,
  DatasourceRegistrationInput,
  RegisterDatasourceResponse,
  RequestOptions,
} from "./types.js";

export interface ConnectClientOptions {
  organizationId?: string;
}

export class ConnectClient {
  constructor(
    private readonly protocol: ProtocolConnectClient,
    private readonly options: ConnectClientOptions = {},
  ) {}

  async registerDatasource(
    input: DatasourceRegistrationInput,
    options: RequestOptions = {},
  ): Promise<RegisterDatasourceResponse> {
    const organizationId = input.organizationId ?? this.options.organizationId;
    if (organizationId === undefined || organizationId.trim().length === 0) {
      throw new MedallionError(
        "organizationId is required to register a datasource. Pass it to MedallionClient or connect.registerDatasource().",
        { code: "MEDALLION_MISSING_ORGANIZATION_ID" },
      );
    }
    const idempotencyKey = requiredControlIdempotencyKey(
      input.idempotencyKey,
      "datasource.idempotencyKey",
    );

    const response = await this.protocol.registerConnector(
      {
        organization_id: organizationId,
        kind: input.type,
        source_system: input.name,
        display_name: input.displayName ?? input.name,
        external_id: optionalId(input.externalId, "datasource.externalId"),
        idempotency_key: idempotencyKey,
      },
      options,
    );
    if (
      response.body.connector === undefined ||
      response.body.connector.id.trim().length === 0
    ) {
      throw new MedallionError(
        "Medallion returned a datasource registration without a connector ID.",
        {
          code: "MEDALLION_INVALID_DATASOURCE_RESPONSE",
          requestId: response.requestId,
        },
      );
    }

    return {
      requestId: response.requestId,
      datasource: datasourceFromConnector(
        response.body.connector,
        input.metadata,
      ),
    };
  }
}

export function datasourceFromConnector(
  connector: {
    id?: string;
    organization_id?: string;
    kind?: string;
    source_system?: string;
    display_name?: string;
    external_id?: string;
    status?: string;
    created_at?: string;
    updated_at?: string;
  },
  metadata?: Record<string, unknown>,
): Datasource {
  return {
    id: connector.id ?? "",
    organizationId: connector.organization_id,
    kind: connector.kind,
    type: connector.kind,
    sourceSystem: connector.source_system,
    name: connector.source_system,
    displayName: connector.display_name,
    externalId: connector.external_id,
    status: connector.status,
    createdAt: connector.created_at,
    updatedAt: connector.updated_at,
    metadata,
  };
}
