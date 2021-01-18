import {
  DynamoDBClient,
  UpdateTableCommand,
  UpdateTableInput,
} from "@aws-sdk/client-dynamodb";
import retry from "async-retry";

import {
  MARSHALL_REQUEST,
  LONG_MAX_LATENCY,
  RETRY_OPTIONS,
  TAKING_TOO_LONG_EXCEPTION,
} from "../../utils/constants";
import { isRetryableError } from "../../utils/misc-utils";
import { createShortCircuit } from "../../utils/short-circuit";

export class TableUpdate {
  constructor(
    protected readonly client: DynamoDBClient,
    protected parameters: UpdateTableInput,
  ) {}

  [MARSHALL_REQUEST](): UpdateTableInput {
    return { ...this.parameters };
  }

  $execute = async () => {
    const requestInput = this[MARSHALL_REQUEST]();
    return retry(async (bail, attempt) => {
      const shortCircuit = createShortCircuit({
        duration: attempt * LONG_MAX_LATENCY,
        error: new Error(TAKING_TOO_LONG_EXCEPTION),
      });
      try {
        const output = await Promise.race([
          this.client.send(new UpdateTableCommand(requestInput)),
          shortCircuit.launch(),
        ]);
        return output.TableDescription;
      } catch (error) {
        if (!isRetryableError(error)) {
          bail(error);
          return;
        }
        throw error;
      } finally {
        shortCircuit.halt();
      }
    }, RETRY_OPTIONS);
  };

  $ = this.$execute;
}