import retry from "async-retry";
import {
  BatchGetItemInput,
  BatchGetItemOutput,
  ConsistentRead,
  DocumentClient,
  ItemList,
} from "aws-sdk/clients/dynamodb";

import { IBatchGetItemRequestItem, RequestParams } from "../../types/request";
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
  quickFail,
  validateKey,
} from "../utils/misc-utils";
import { Requester } from "./_Requester";

export class BatchGetter extends Requester {
  #ConsistentRead?: ConsistentRead;
  #ProjectionExpression?: string[];

  constructor(
    DB: DocumentClient,
    table: string,
    private keys: DocumentClient.Key[],
  ) {
    super(DB, table);
    keys.forEach((key) => validateKey(key));
  }

  consistentRead = (consistentRead: ConsistentRead = true) => {
    this.#ConsistentRead = consistentRead;
    return this;
  };

  select = (...args: (string | string[] | undefined | null)[]) => {
    if (args.every((arg) => arg == null) || args.flat().length === 0) {
      return this;
    }

    args.forEach((projection) => {
      if (typeof projection === "string") {
        projection = [projection];
      }
      this.#ProjectionExpression = [
        ...new Set([
          ...(this.#ProjectionExpression || []),
          ...(projection || []),
        ]),
      ];
    });
    return this;
  };

  [BUILD]() {
    return {
      ...super[BUILD](),
      ...(this.#ConsistentRead ? { ConsistentRead: this.#ConsistentRead } : {}),
      ...(this.#ProjectionExpression
        ? { RawProjectionExpression: this.#ProjectionExpression }
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

    const requestItems: IBatchGetItemRequestItem = {
      Keys: this.keys,
      ...(requestParams.ConsistentRead != null
        ? { ConsistentRead: requestParams.ConsistentRead }
        : {}),
      ...(requestParams.ProjectionExpression != null &&
      requestParams.ExpressionAttributeNames != null
        ? {
            ProjectionExpression: requestParams.ProjectionExpression,
            ExpressionAttributeNames: requestParams.ExpressionAttributeNames,
          }
        : {}),
    };
    const batchParams: RequestParams = {
      RequestItems: { [this.table]: requestItems },
    };
    requestParams = {
      ...batchParams,
      ...(requestParams.ReturnConsumedCapacity != null
        ? {
            ReturnConsumedCapacity: requestParams.ReturnConsumedCapacity,
          }
        : {}),
    };

    return { ...optimizeRequestParams(requestParams) };
  }

  private batchGetSegment = async (params: BatchGetItemInput) => {
    const response: BatchGetItemOutput = {};

    const table = Object.keys(params.RequestItems)[0];

    let scanCompleted = false;

    return retry(async (bail, attempt) => {
      try {
        while (!scanCompleted) {
          const result = await Promise.race([
            this.DB.batchGet(params).promise(),
            quickFail(
              attempt * LONG_MAX_LATENCY,
              new Error(TAKING_TOO_LONG_EXCEPTION),
            ),
          ]);
          if (result.UnprocessedKeys?.[table]) {
            params.RequestItems = result.UnprocessedKeys;
          } else {
            scanCompleted = true;
          }
          if (result.Responses?.[table]) {
            response.Responses = response.Responses || {};
            response.Responses[table] = [
              ...(response.Responses[table] || []),
              ...(result.Responses[table] || []),
            ];
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
        }
        return response;
      } catch (ex) {
        if (!isRetryableDBError(ex)) {
          bail(ex);
          return;
        }
        throw ex;
      }
    }, RETRY_OPTIONS);
  };

  $execute = async <T = ItemList | undefined | null, U extends boolean = false>(
    returnRawResponse?: U,
  ): Promise<U extends true ? BatchGetItemOutput : T | undefined | null> => {
    const params = {
      ...(this[BUILD_PARAMS]() as BatchGetItemInput),
    };
    const table = Object.keys(params.RequestItems)[0];
    const keys = [...params.RequestItems[table].Keys];
    const paramsGroups: BatchGetItemInput[] = [];
    while (keys.length > 0) {
      const lighterParams: BatchGetItemInput = JSON.parse(
        JSON.stringify(params),
      );
      lighterParams.RequestItems[table].Keys = [
        ...keys.splice(0, BATCH_OPTIONS.GET_LIMIT),
      ];
      paramsGroups.push(lighterParams);
    }
    const allResults = await Promise.all(
      paramsGroups.map((paramsGroup) => this.batchGetSegment(paramsGroup)),
    );
    const results = allResults.reduce((p, c) => {
      if (p == null) {
        return c;
      }
      p.Responses = p.Responses || {};
      p.Responses[table] = [
        ...(p.Responses[table] || []),
        ...(p.Responses[table] || []),
      ];
      if (c?.ConsumedCapacity) {
        if (!p.ConsumedCapacity) {
          p.ConsumedCapacity = c.ConsumedCapacity;
        } else {
          p.ConsumedCapacity[0].CapacityUnits =
            (p.ConsumedCapacity[0].CapacityUnits || 0) +
            (c.ConsumedCapacity[0].CapacityUnits || 0);
        }
      }
      return p;
    });
    return (returnRawResponse ? results : results?.Responses?.[table]) as any;
  };

  $ = this.$execute;
}
