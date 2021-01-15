import retry from "async-retry";
import DynamoDB, {
  AttributeMap,
  BatchWriteItemInput,
  BatchWriteItemOutput,
  ItemList,
} from "aws-sdk/clients/dynamodb";

import {
  IBatchPutItemRequestItem,
  RequestParameters,
  ReturnItemCollectionMetrics,
} from "../../types/request";
import {
  BATCH_OPTIONS,
  BUILD,
  BUILD_PARAMS,
  LONG_MAX_LATENCY,
  RETRY_OPTIONS,
  TAKING_TOO_LONG_EXCEPTION,
} from "../utils/constants";
import { optimizeRequestParameters } from "../utils/expression-optimization-utils";
import { isRetryableDBError, QuickFail } from "../utils/misc-utils";
import { Requester } from "./_requester";

export class BatchPutter extends Requester {
  #ReturnItemCollectionMetrics?: ReturnItemCollectionMetrics;

  constructor(DB: DynamoDB, table: string, private items: AttributeMap[]) {
    super(DB, table);
  }

  returnItemCollectionMetrics = (
    returnItemCollectionMetrics: ReturnItemCollectionMetrics = "SIZE",
  ) => {
    this.#ReturnItemCollectionMetrics = returnItemCollectionMetrics;
    return this;
  };

  [BUILD]() {
    return {
      ...super[BUILD](),
      ...(this.#ReturnItemCollectionMetrics
        ? {
            ReturnItemCollectionMetrics: this.#ReturnItemCollectionMetrics,
          }
        : {}),
    };
  }

  [BUILD_PARAMS]() {
    let requestParameters = super[BUILD_PARAMS]();

    if (this.table == undefined) {
      throw new Error("Table name must be provided");
    }

    if (this.items.length === 0) {
      throw new Error("At least one item must be provided");
    }

    const requestItems: IBatchPutItemRequestItem[] = this.items.map((item) => ({
      PutRequest: { Item: item },
    }));
    const batchParameters: RequestParameters = {
      RequestItems: { [this.table]: requestItems },
    };
    requestParameters = {
      ...batchParameters,
      ...(requestParameters.ReturnConsumedCapacity
        ? {
            ReturnConsumedCapacity: requestParameters.ReturnConsumedCapacity,
          }
        : {}),
      ...(requestParameters.ReturnItemCollectionMetrics
        ? {
            ReturnItemCollectionMetrics:
              requestParameters.ReturnItemCollectionMetrics,
          }
        : {}),
    };

    return { ...optimizeRequestParameters(requestParameters) };
  }

  private batchWriteSegment = async (parameters: BatchWriteItemInput) => {
    const response: BatchWriteItemOutput = {};

    const table = Object.keys(parameters.RequestItems)[0];

    let operationCompleted = false;

    return retry(async (bail, attempt) => {
      while (!operationCompleted) {
        const qf = new QuickFail(
          attempt * LONG_MAX_LATENCY * (this.patienceRatio || 1),
          new Error(TAKING_TOO_LONG_EXCEPTION),
        );
        try {
          const result = await Promise.race([
            this.DB.batchWriteItem(parameters).promise(),
            qf.wait(),
          ]);
          if (result.UnprocessedItems?.[table]) {
            parameters.RequestItems = result.UnprocessedItems;
          } else {
            operationCompleted = true;
          }

          if (result.ConsumedCapacity) {
            if (!response.ConsumedCapacity) {
              response.ConsumedCapacity = result.ConsumedCapacity;
            } else {
              response.ConsumedCapacity[0].CapacityUnits =
                (response.ConsumedCapacity[0].CapacityUnits || 0) +
                (result.ConsumedCapacity[0].CapacityUnits || 0);
            }
          }

          if (result.ItemCollectionMetrics) {
            if (!response.ItemCollectionMetrics) {
              response.ItemCollectionMetrics = result.ItemCollectionMetrics;
            } else {
              response.ItemCollectionMetrics[0] = [
                ...response.ItemCollectionMetrics[0],
                ...result.ItemCollectionMetrics[0],
              ];
            }
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
      return response;
    }, RETRY_OPTIONS);
  };

  $execute = async <T = ItemList | undefined | null, U extends boolean = false>(
    returnRawResponse?: U,
  ): Promise<U extends true ? BatchWriteItemOutput : T | undefined | null> => {
    const parameters = { ...(this[BUILD_PARAMS]() as BatchWriteItemInput) };
    const table = Object.keys(parameters.RequestItems)[0];
    const items = [...parameters.RequestItems[table]];
    const parametersGroups: BatchWriteItemInput[] = [];
    const lighterParameters: BatchWriteItemInput = JSON.parse(
      JSON.stringify(parameters),
    );
    for (
      let index = 0;
      index < items.length;
      index += BATCH_OPTIONS.WRITE_LIMIT
    ) {
      parametersGroups.push({
        ...lighterParameters,
        RequestItems: {
          [table]: items.slice(index, index + BATCH_OPTIONS.WRITE_LIMIT),
        },
      });
    }
    const allResults = await Promise.all(
      parametersGroups.map((parametersGroup) =>
        this.batchWriteSegment(parametersGroup),
      ),
    );

    const results = allResults.reduce((p, c) => {
      if (p == undefined) {
        return c;
      }

      if (c?.ConsumedCapacity) {
        if (!p.ConsumedCapacity) {
          p.ConsumedCapacity = c.ConsumedCapacity;
        } else {
          p.ConsumedCapacity[0].CapacityUnits =
            (p.ConsumedCapacity[0].CapacityUnits || 0) +
            (c.ConsumedCapacity[0].CapacityUnits || 0);
        }
      }

      if (c?.ItemCollectionMetrics) {
        if (!p.ItemCollectionMetrics) {
          p.ItemCollectionMetrics = c.ItemCollectionMetrics;
        } else {
          p.ItemCollectionMetrics[0] = [
            ...p.ItemCollectionMetrics[0],
            ...c.ItemCollectionMetrics[0],
          ];
        }
      }
      return p;
    });
    return (returnRawResponse
      ? results
      : parameters.RequestItems[table].map(
          (input) => input.PutRequest?.Item,
        )) as any;
  };

  $ = this.$execute;
}
