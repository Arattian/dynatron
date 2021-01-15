import retry from "async-retry";
import {
  BatchGetItemInput,
  BatchGetItemOutput,
  ConsistentRead,
  DocumentClient,
  ItemList,
} from "aws-sdk/clients/dynamodb";

import {
  IBatchGetItemRequestItem,
  RequestParameters,
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
import {
  isRetryableDBError,
  QuickFail,
  validateKey,
} from "../utils/misc-utils";
import { Requester } from "./_requester";

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

  select = (...arguments_: (string | string[] | undefined | null)[]) => {
    if (
      arguments_.every((argument) => argument == undefined) ||
      arguments_.flat().length === 0
    ) {
      return this;
    }

    arguments_.forEach((projection) => {
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
    let requestParameters = super[BUILD_PARAMS]();

    if (this.table == undefined) {
      throw new Error("Table name must be provided");
    }

    if (this.keys.length === 0) {
      throw new Error("At least one key must be provided");
    }

    const requestItems: IBatchGetItemRequestItem = {
      Keys: this.keys,
      ...(requestParameters.ConsistentRead
        ? { ConsistentRead: requestParameters.ConsistentRead }
        : {}),
      ...(requestParameters.ProjectionExpression != undefined &&
      requestParameters.ExpressionAttributeNames != undefined
        ? {
            ProjectionExpression: requestParameters.ProjectionExpression,
            ExpressionAttributeNames:
              requestParameters.ExpressionAttributeNames,
          }
        : {}),
    };
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
    };

    return { ...optimizeRequestParameters(requestParameters) };
  }

  private batchGetSegment = async (parameters: BatchGetItemInput) => {
    const response: BatchGetItemOutput = {};

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
            this.DB.batchGet(parameters).promise(),
            qf.wait(),
          ]);
          if (result.UnprocessedKeys?.[table]) {
            parameters.RequestItems = result.UnprocessedKeys;
          } else {
            operationCompleted = true;
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
  ): Promise<U extends true ? BatchGetItemOutput : T | undefined | null> => {
    const parameters = {
      ...(this[BUILD_PARAMS]() as BatchGetItemInput),
    };
    const table = Object.keys(parameters.RequestItems)[0];
    const keys = [...parameters.RequestItems[table].Keys];
    const parametersGroups: BatchGetItemInput[] = [];
    const lighterParameters: BatchGetItemInput = JSON.parse(
      JSON.stringify(parameters),
    );
    for (let index = 0; index < keys.length; index += BATCH_OPTIONS.GET_LIMIT) {
      parametersGroups.push({
        RequestItems: {
          [table]: {
            ...lighterParameters.RequestItems[table],
            Keys: keys.slice(index, index + BATCH_OPTIONS.GET_LIMIT),
          },
        },
      });
    }
    const allResults = await Promise.all(
      parametersGroups.map((parametersGroup) =>
        this.batchGetSegment(parametersGroup),
      ),
    );
    const results = allResults.reduce((p, c) => {
      if (p == undefined) {
        return c;
      }
      p.Responses = p.Responses || {};
      p.Responses[table] = [
        ...(p.Responses[table] || []),
        ...(c?.Responses?.[table] || []),
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
