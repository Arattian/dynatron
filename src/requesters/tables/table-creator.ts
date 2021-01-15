import retry from "async-retry";
import DynamoDB, { CreateTableInput } from "aws-sdk/clients/dynamodb";

import {
  BUILD_PARAMS,
  LONG_MAX_LATENCY,
  RETRY_OPTIONS,
  TAKING_TOO_LONG_EXCEPTION,
} from "../../utils/constants";
import { isRetryableDBError, QuickFail } from "../../utils/misc-utils";

export class TableCreator {
  constructor(private DB: DynamoDB, private parameters: CreateTableInput) {}

  [BUILD_PARAMS]() {
    return { ...this.parameters };
  }

  $execute = async () => {
    return retry(async (bail, attempt) => {
      const qf = new QuickFail(
        attempt * LONG_MAX_LATENCY,
        new Error(TAKING_TOO_LONG_EXCEPTION),
      );
      try {
        const response = await Promise.race([
          this.DB.createTable(
            this[BUILD_PARAMS]() as CreateTableInput,
          ).promise(),
          qf.wait(),
        ]);
        return response.TableDescription;
      } catch (error) {
        if (!isRetryableDBError(error)) {
          bail(error);
          return;
        }
        throw error;
      } finally {
        qf.cancel();
      }
    }, RETRY_OPTIONS);
  };

  $ = this.$execute;
}
