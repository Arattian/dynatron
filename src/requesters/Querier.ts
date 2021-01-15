import retry from "async-retry";
import {
  BooleanObject,
  DocumentClient,
  ItemList,
  QueryInput,
  QueryOutput,
} from "aws-sdk/clients/dynamodb";

import { KeyCondition } from "../../types/conditions";
import {
  BUILD,
  BUILD_PARAMS,
  LONG_MAX_LATENCY,
  RETRY_OPTIONS,
  TAKING_TOO_LONG_EXCEPTION,
} from "../utils/constants";
import { optimizeRequestParameters as optimizeRequestParameters } from "../utils/expression-optimization-utils";
import {
  isRetryableDBError,
  QuickFail,
  validateKey,
} from "../utils/misc-utils";
import { MultiGetter } from "./_multi-getter";

export class Querier extends MultiGetter {
  #KeyConditionExpression?: KeyCondition;
  #ScanIndexForward?: BooleanObject;

  constructor(
    DB: DocumentClient,
    table: string,
    private key: DocumentClient.Key,
  ) {
    super(DB, table);
    validateKey(key);
  }

  having = (keyCondition: KeyCondition | undefined | null) => {
    if (keyCondition != undefined) {
      this.#KeyConditionExpression = keyCondition;
    }
    return this;
  };

  sort = (sort: "ASC" | "DSC") => {
    if (sort === "DSC") {
      this.#ScanIndexForward = false;
    }
    return this;
  };

  [BUILD]() {
    return {
      ...super[BUILD](),
      ...(this.#KeyConditionExpression
        ? { RawKeyConditionExpression: [this.#KeyConditionExpression] }
        : {}),
      ...(this.#ScanIndexForward != undefined
        ? { ScanIndexForward: this.#ScanIndexForward }
        : {}),
    };
  }

  [BUILD_PARAMS]() {
    const requestParameters = super[BUILD_PARAMS](this.key);

    return {
      TableName: this.table,
      ...optimizeRequestParameters(requestParameters),
    };
  }

  $execute = async <T = ItemList | undefined | null, U extends boolean = false>(
    returnRawResponse?: U,
    disableRecursion = false,
  ): Promise<U extends true ? QueryOutput : T | undefined | null> => {
    const parameters = { ...(this[BUILD_PARAMS]() as QueryInput) };

    if (parameters.IndexName) {
      delete parameters.ConsistentRead;
    }
    let operationCompleted = false;
    const response: QueryOutput = {};
    return retry(async (bail, attempt) => {
      while (!operationCompleted) {
        const qf = new QuickFail(
          attempt * LONG_MAX_LATENCY * (this.patienceRatio || 1),
          new Error(TAKING_TOO_LONG_EXCEPTION),
        );
        try {
          const result = await Promise.race([
            this.DB.query(parameters).promise(),
            qf.wait(),
          ]);
          if (result.LastEvaluatedKey == undefined || disableRecursion) {
            operationCompleted = true;
          } else {
            parameters.ExclusiveStartKey = result.LastEvaluatedKey;
          }
          if (result.Items) {
            response.Items = [...(response.Items || []), ...result.Items];
          }
          if (result.Count) {
            response.Count = (response.Count || 0) + result.Count;
          }
          if (result.ScannedCount) {
            response.ScannedCount =
              (response.ScannedCount || 0) + result.ScannedCount;
          }
          if (result.ConsumedCapacity) {
            if (!response.ConsumedCapacity) {
              response.ConsumedCapacity = result.ConsumedCapacity;
            } else {
              response.ConsumedCapacity.CapacityUnits =
                (response.ConsumedCapacity.CapacityUnits || 0) +
                (result.ConsumedCapacity?.CapacityUnits || 0);
            }
          }
          if (
            parameters.Limit &&
            (response.Items?.length || 0) >= parameters.Limit
          ) {
            response.Items = response.Items?.slice(0, parameters.Limit);
            response.Count = response.Items?.length || 0;
            operationCompleted = true;
          }
          if (disableRecursion && result.LastEvaluatedKey != undefined) {
            response.LastEvaluatedKey = result.LastEvaluatedKey;
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
      return (returnRawResponse ? response : response.Items) as any;
    }, RETRY_OPTIONS);
  };

  $ = this.$execute;
}
