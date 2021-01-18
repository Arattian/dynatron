import {
  QueryCommand,
  QueryCommandInput,
  QueryOutput,
} from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import retry from "async-retry";

import { KeyCondition } from "../../types/conditions";
import { NativeValue } from "../../types/native-types";
import {
  BUILD,
  LONG_MAX_LATENCY,
  MARSHALL_REQUEST,
  RETRY_OPTIONS,
  TAKING_TOO_LONG_EXCEPTION,
} from "../utils/constants";
import { isRetryableError } from "../utils/misc-utils";
import { createShortCircuit } from "../utils/short-circuit";
import { ListFetch } from "./1.3-list-fetch";

export class Query extends ListFetch {
  #KeyConditionExpression?: KeyCondition;
  #ScanIndexForward?: boolean;

  having = (keyCondition: KeyCondition | undefined) => {
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
      ...(this.#KeyConditionExpression && {
        _KeyConditionExpression: [this.#KeyConditionExpression],
      }),
      ...(this.#ScanIndexForward != undefined && {
        ScanIndexForward: this.#ScanIndexForward,
      }),
    };
  }

  $execute = async <T = NativeValue[] | undefined, U extends boolean = false>(
    returnRawResponse?: U,
    disableRecursion = false,
  ): Promise<U extends true ? QueryOutput : T | undefined> => {
    const requestInput = super[MARSHALL_REQUEST]<QueryCommandInput>(
      this[BUILD](),
    );

    if (requestInput.IndexName) {
      delete requestInput.ConsistentRead;
    }

    let operationCompleted = false;
    const aggregatedOutput: QueryOutput = {};

    return retry(async (bail, attempt) => {
      while (!operationCompleted) {
        const shortCircuit = createShortCircuit({
          duration: attempt * LONG_MAX_LATENCY,
          error: new Error(TAKING_TOO_LONG_EXCEPTION),
        });
        try {
          const { $metadata, ...output } = await Promise.race([
            this.databaseClient.send(new QueryCommand(requestInput)),
            shortCircuit.launch(),
          ]);

          if (output.LastEvaluatedKey == undefined || disableRecursion) {
            operationCompleted = true;
          } else {
            requestInput.ExclusiveStartKey = output.LastEvaluatedKey;
          }
          if (output.Items) {
            aggregatedOutput.Items = [
              ...(aggregatedOutput.Items || []),
              ...output.Items,
            ];
          }
          if (output.Count) {
            aggregatedOutput.Count =
              (aggregatedOutput.Count || 0) + output.Count;
          }
          if (output.ScannedCount) {
            aggregatedOutput.ScannedCount =
              (aggregatedOutput.ScannedCount || 0) + output.ScannedCount;
          }
          if (output.ConsumedCapacity) {
            if (!aggregatedOutput.ConsumedCapacity) {
              aggregatedOutput.ConsumedCapacity = output.ConsumedCapacity;
            } else {
              aggregatedOutput.ConsumedCapacity.CapacityUnits =
                (aggregatedOutput.ConsumedCapacity.CapacityUnits || 0) +
                (output.ConsumedCapacity?.CapacityUnits || 0);
            }
          }
          if (
            requestInput.Limit &&
            (aggregatedOutput.Items?.length || 0) >= requestInput.Limit
          ) {
            aggregatedOutput.Items = aggregatedOutput.Items?.slice(
              0,
              requestInput.Limit,
            );
            aggregatedOutput.Count = aggregatedOutput.Items?.length || 0;
            operationCompleted = true;
          }
          if (disableRecursion && output.LastEvaluatedKey != undefined) {
            aggregatedOutput.LastEvaluatedKey = output.LastEvaluatedKey;
          }
        } catch (error) {
          if (!isRetryableError(error)) {
            bail(error);
            return;
          }
          throw error;
        } finally {
          shortCircuit.halt();
        }
      }
      return (returnRawResponse
        ? aggregatedOutput
        : aggregatedOutput.Items?.map((item) => unmarshall(item))) as any;
    }, RETRY_OPTIONS);
  };

  $ = this.$execute;
}