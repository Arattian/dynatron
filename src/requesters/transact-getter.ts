import retry from "async-retry";
import DynamoDB, {
  ItemList,
  TransactGetItem,
  TransactGetItemsInput,
  TransactGetItemsOutput,
} from "aws-sdk/clients/dynamodb";

import {
  BUILD_PARAMS,
  LONG_MAX_LATENCY,
  RETRY_OPTIONS,
  TAKING_TOO_LONG_EXCEPTION,
} from "../utils/constants";
import { optimizeRequestParameters } from "../utils/expression-optimization-utils";
import { isRetryableDBError, QuickFail } from "../utils/misc-utils";
import { Requester } from "./_requester";
import { Getter } from "./getter";

export class TransactGetter extends Requester {
  constructor(DB: DynamoDB, table: string, private items: Getter[]) {
    super(DB, table);
  }

  [BUILD_PARAMS]() {
    let requestParameters = super[BUILD_PARAMS]();

    if (this.items.length === 0) {
      throw new Error("At least one transaction must be provided");
    }
    if (this.items.length > 25) {
      throw new Error("No more than 25 transactions can be provided");
    }
    const supportedParameters = new Set([
      "Key",
      "TableName",
      "ExpressionAttributeNames",
      "ProjectionExpression",
    ]);
    requestParameters = {
      TransactItems: this.items.map((item) => {
        const transactItem = item[BUILD_PARAMS]();
        Object.keys(transactItem).forEach((k) => {
          if (!supportedParameters.has(k)) {
            delete transactItem[k];
          }
        });
        return { Get: transactItem } as TransactGetItem;
      }),
      ...(requestParameters.ReturnConsumedCapacity
        ? {
            ReturnConsumedCapacity: requestParameters.ReturnConsumedCapacity,
          }
        : {}),
    };

    return { ...optimizeRequestParameters(requestParameters) };
  }

  $execute = async <T = ItemList | undefined | null, U extends boolean = false>(
    returnRawResponse?: U,
  ): Promise<
    U extends true ? TransactGetItemsOutput : T | undefined | null
  > => {
    return retry(async (bail, attempt) => {
      const qf = new QuickFail(
        attempt * LONG_MAX_LATENCY * (this.patienceRatio || 1),
        new Error(TAKING_TOO_LONG_EXCEPTION),
      );
      try {
        const response = await Promise.race([
          this.DB.transactGetItems(
            this[BUILD_PARAMS]() as TransactGetItemsInput,
          ).promise(),
          qf.wait(),
        ]);
        return (returnRawResponse
          ? response
          : response.Responses?.map((r) => r.Item)) as any;
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
