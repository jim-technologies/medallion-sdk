import type { ConnectClient } from "./connect.js";
import type {
  DatasourceRegistrationInput,
  RegisterDatasourceResponse,
  RequestOptions,
} from "./types.js";

export class DatasourcesClient {
  constructor(private readonly connect: ConnectClient) {}

  register(
    input: DatasourceRegistrationInput,
    options: RequestOptions = {},
  ): Promise<RegisterDatasourceResponse> {
    return this.connect.registerDatasource(input, options);
  }
}
