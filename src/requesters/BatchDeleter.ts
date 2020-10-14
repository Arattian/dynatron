import retry from "async-retry";
import {
  BatchWriteItemInput,
  BatchWriteItemOutput,
  DocumentClient,
  ItemList,
} from "aws-sdk/clients/dynamodb";

import {
  IBatchDeleteItemRequestItem,
  RequestParams,
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
import { optimizeRequestParams } from "../utils/expression-optimization-utils";
import {
  isRetryableDBError,
  QuickFail,
  validateKey,
} from "../utils/misc-utils";
import { Requester } from "./_Requester";

export class BatchDeleter extends Requester {
  #ReturnItemCollectionMetrics?: ReturnItemCollectionMetrics;

  constructor(
    DB: DocumentClient,
    table: string,
    private keys: DocumentClient.Key[],
  ) {
    super(DB, table);
    keys.forEach((key) => validateKey(key));
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
        ? { ReturnItemCollectionMetrics: this.#ReturnItemCollectionMetrics }
        : {}),
    };
  }

  [BUILD_PARAMS]() {
    let requestParams = super[BUILD_PARAMS]();

    if (this.table == null) {
      throw new Error("Table name must be provided");
    }

    if (this.keys.length == 0) {
      throw new Error("At least one key must be provided");
    }

    const requestItems: IBatchDeleteItemRequestItem[] = this.keys.map(
      (key) => ({ DeleteRequest: { Key: key } }),
    );
    const batchParams: RequestParams = {
      RequestItems: { [this.table]: requestItems },
    };
    requestParams = {
      ...batchParams,
      ...(requestParams.ReturnConsumedCapacity
        ? { ReturnConsumedCapacity: requestParams.ReturnConsumedCapacity }
        : {}),
      ...(requestParams.ReturnItemCollectionMetrics
        ? {
            ReturnItemCollectionMetrics:
              requestParams.ReturnItemCollectionMetrics,
          }
        : {}),
    };

    return { ...optimizeRequestParams(requestParams) };
  }

  private batchWriteSegment = async (params: BatchWriteItemInput) => {
    const response: BatchWriteItemOutput = {};

    const table = Object.keys(params.RequestItems)[0];

    let operationCompleted = false;

    return retry(async (bail, attempt) => {
      while (!operationCompleted) {
        const qf = new QuickFail(
          attempt * LONG_MAX_LATENCY * (this.patienceRatio || 1),
          new Error(TAKING_TOO_LONG_EXCEPTION),
        );
        try {
          const result = await Promise.race([
            this.DB.batchWrite(params).promise(),
            qf.wait(),
          ]);
          if (result.UnprocessedItems?.[table]) {
            params.RequestItems = result.UnprocessedItems;
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
        } catch (ex) {
          if (!isRetryableDBError(ex)) {
            bail(ex);
            return;
          }
          throw ex;
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
    const params = { ...(this[BUILD_PARAMS]() as BatchWriteItemInput) };
    const table = Object.keys(params.RequestItems)[0];
    const items = [...params.RequestItems[table]];
    const paramsGroups: BatchWriteItemInput[] = [];
    const lighterParams: BatchWriteItemInput = JSON.parse(
      JSON.stringify(params),
    );
    for (let i = 0; i < items.length; i += BATCH_OPTIONS.WRITE_LIMIT) {
      paramsGroups.push({
        ...lighterParams,
        RequestItems: {
          [table]: items.slice(i, i + BATCH_OPTIONS.WRITE_LIMIT),
        },
      });
    }
    const allResults = await Promise.all(
      paramsGroups.map((paramsGroup) => this.batchWriteSegment(paramsGroup)),
    );

    const results = allResults.reduce((p, c) => {
      if (p == null) {
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
    return (returnRawResponse ? results : undefined) as any;
  };

  $ = this.$execute;
}
