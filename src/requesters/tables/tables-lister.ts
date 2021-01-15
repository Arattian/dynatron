import retry from "async-retry";
import DynamoDB, {
  ListTablesInput,
  ListTablesInputLimit,
  ListTablesOutput,
  TableName,
} from "aws-sdk/clients/dynamodb";

import {
  BUILD_PARAMS,
  LONG_MAX_LATENCY,
  RETRY_OPTIONS,
  TAKING_TOO_LONG_EXCEPTION,
} from "../../utils/constants";
import { isRetryableDBError, QuickFail } from "../../utils/misc-utils";

export class TablesLister {
  #Limit?: ListTablesInputLimit;
  #ExclusiveStartTableName?: TableName;

  constructor(private DB: DynamoDB) {}

  limit = (
    limit: ListTablesInputLimit,
    exclusiveStartTableName?: TableName,
  ) => {
    if (limit > 100) {
      throw new Error("The limit must be less than or equal to 100");
    }
    this.#Limit = limit;
    this.#ExclusiveStartTableName = exclusiveStartTableName;
    return this;
  };

  [BUILD_PARAMS]() {
    return {
      ...(this.#Limit ? { Limit: this.#Limit } : {}),
      ...(this.#ExclusiveStartTableName
        ? { ExclusiveStartTableName: this.#ExclusiveStartTableName }
        : {}),
    };
  }

  $execute = async () => {
    const parameters = { ...(this[BUILD_PARAMS]() as ListTablesInput) };

    let operationCompleted = false;

    const response: ListTablesOutput = {};

    return retry(async (bail, attempt) => {
      while (!operationCompleted) {
        const qf = new QuickFail(
          attempt * LONG_MAX_LATENCY,
          new Error(TAKING_TOO_LONG_EXCEPTION),
        );
        try {
          const result = await Promise.race([
            this.DB.listTables(parameters).promise(),
            qf.wait(),
          ]);
          if (result.LastEvaluatedTableName == undefined) {
            operationCompleted = true;
          } else {
            parameters.ExclusiveStartTableName = result.LastEvaluatedTableName;
          }
          if (result.TableNames) {
            response.TableNames = [
              ...(response.TableNames || []),
              ...result.TableNames,
            ];
          }

          if (
            parameters.Limit &&
            (response.TableNames?.length || 0) >= parameters.Limit
          ) {
            response.TableNames = response.TableNames?.slice(
              0,
              parameters.Limit,
            );
            operationCompleted = true;
          }
        } catch (error) {
          if (!isRetryableDBError(error)) {
            bail(error);
            return;
          }
          throw error;
        } finally {
          qf.cancel();
        }
      }
      return response.TableNames;
    }, RETRY_OPTIONS);
  };

  $ = this.$execute;
}
