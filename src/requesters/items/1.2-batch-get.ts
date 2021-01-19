import {
  BatchGetItemCommand,
  BatchGetItemCommandInput,
  BatchGetItemOutput,
  DynamoDBClient,
} from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import AsyncRetry from "async-retry";

import { NativeKey, NativeValue } from "../../../types/native-types";
import {
  BUILD,
  createShortCircuit,
  isRetryableError,
  LONG_MAX_LATENCY,
  RETRY_OPTIONS,
  TAKING_TOO_LONG_EXCEPTION,
} from "../../utils/misc-utils";
import { marshallRequestParameters } from "../../utils/request-marshaller";
import { Fetch } from "./1-fetch";

const BATCH_GET_LIMIT = 100;

export class BatchGet extends Fetch {
  constructor(
    databaseClient: DynamoDBClient,
    tableName: string,
    private keys: NativeKey[],
  ) {
    super(databaseClient, tableName);
  }

  [BUILD]() {
    return {
      ...super[BUILD](),
      _Keys: this.keys,
    };
  }

  private batchGetSegment = async (requestInput: BatchGetItemCommandInput) => {
    let operationCompleted = false;
    const response: BatchGetItemOutput = {};
    return AsyncRetry(async (bail, attempt) => {
      while (!operationCompleted) {
        const shortCircuit = createShortCircuit({
          duration: attempt * LONG_MAX_LATENCY * (this.patienceRatio || 1),
          error: new Error(TAKING_TOO_LONG_EXCEPTION),
        });
        try {
          // eslint-disable-next-line @typescript-eslint/no-unused-vars
          const { $metadata, ...output } = await Promise.race([
            this.databaseClient.send(new BatchGetItemCommand(requestInput)),
            shortCircuit.launch(),
          ]);
          if (output?.UnprocessedKeys?.[this.tableName] == undefined) {
            operationCompleted = true;
          } else {
            requestInput.RequestItems = output.UnprocessedKeys;
          }

          if (output?.Responses?.[this.tableName]) {
            response.Responses = response.Responses || {};
            response.Responses[this.tableName] = [
              ...(response.Responses[this.tableName] || []),
              ...(output.Responses[this.tableName] || []),
            ];
          }

          if (output.ConsumedCapacity) {
            if (!response.ConsumedCapacity) {
              response.ConsumedCapacity = output.ConsumedCapacity;
            } else {
              response.ConsumedCapacity[0].CapacityUnits =
                (response.ConsumedCapacity[0].CapacityUnits || 0) +
                (output.ConsumedCapacity[0].CapacityUnits || 0);
            }
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
      return response;
    }, RETRY_OPTIONS);
  };

  $ = async <T = NativeValue[] | undefined, U extends boolean = false>(
    returnRawResponse?: U,
  ): Promise<U extends true ? BatchGetItemOutput : T | undefined> => {
    const {
      ReturnConsumedCapacity,
      TableName,
      ...marshalledParameters
    } = marshallRequestParameters(this[BUILD]());

    const requestInputs: BatchGetItemCommandInput[] = [];

    for (let index = 0; index < this.keys.length; index += BATCH_GET_LIMIT) {
      const requestInput: BatchGetItemCommandInput = {
        RequestItems: {
          [TableName]: {
            ...marshalledParameters,
            Keys: marshalledParameters.Keys.slice(
              index,
              index + BATCH_GET_LIMIT,
            ),
          },
        },
        ...(ReturnConsumedCapacity && { ReturnConsumedCapacity }),
      };
      requestInputs.push(requestInput);
    }

    const outputs = await Promise.all(
      requestInputs.map((requestInput) => this.batchGetSegment(requestInput)),
    );

    const aggregatedOutput: BatchGetItemOutput = {};

    for (const output of outputs) {
      if (output == undefined) {
        continue;
      }

      aggregatedOutput.Responses = aggregatedOutput.Responses || {};
      aggregatedOutput.Responses[TableName] = [
        ...(aggregatedOutput.Responses[TableName] || []),
        ...(output.Responses?.[TableName] || []),
      ];

      if (output.ConsumedCapacity) {
        if (!aggregatedOutput.ConsumedCapacity) {
          aggregatedOutput.ConsumedCapacity = output.ConsumedCapacity;
        } else {
          aggregatedOutput.ConsumedCapacity[0].CapacityUnits =
            (aggregatedOutput.ConsumedCapacity[0].CapacityUnits || 0) +
            (output.ConsumedCapacity[0].CapacityUnits || 0);
        }
      }
    }

    return (returnRawResponse
      ? aggregatedOutput
      : aggregatedOutput.Responses?.[TableName]?.map((item) =>
          unmarshall(item),
        )) as any;
  };
}