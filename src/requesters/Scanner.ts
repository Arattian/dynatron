import retry from "async-retry";
import {
  ItemList,
  ScanInput,
  ScanOutput,
  ScanSegment,
  ScanTotalSegments,
} from "aws-sdk/clients/dynamodb";

import {
  BUILD,
  BUILD_PARAMS,
  LONG_MAX_LATENCY,
  RETRY_OPTIONS,
  TAKING_TOO_LONG_EXCEPTION,
} from "../utils/constants";
import { optimizeRequestParameters as optimizeRequestParameters } from "../utils/expression-optimization-utils";
import { isRetryableDBError, QuickFail } from "../utils/misc-utils";
import { MultiGetter } from "./_multi-getter";

const MIN_TOTAL_SEGMENTS = 1;
const MAX_TOTAL_SEGMENTS = 1_000_000;

export class Scanner extends MultiGetter {
  readonly #INITIAL_MAX_TOTAL_SEGMENTS = 10;
  #TotalSegments?: ScanTotalSegments = this.#INITIAL_MAX_TOTAL_SEGMENTS;
  #Segment?: ScanSegment;

  totalSegments = (
    totalSegments: ScanTotalSegments = this.#INITIAL_MAX_TOTAL_SEGMENTS,
  ) => {
    this.#TotalSegments = totalSegments;
    return this;
  };

  segment = (segment: ScanSegment) => {
    this.#Segment = segment;
    return this;
  };

  disableSegments = () => {
    this.#TotalSegments = undefined;
    return this;
  };

  [BUILD]() {
    return {
      ...super[BUILD](),
      ...(this.#TotalSegments ? { TotalSegments: this.#TotalSegments } : {}),
      ...(this.#Segment != undefined ? { Segment: this.#Segment } : {}),
    };
  }

  private scanSegment = async (
    parameters: ScanInput,
    disableRecursion = false,
  ) => {
    let operationCompleted = false;
    if (parameters.Segment != undefined && parameters.TotalSegments) {
      parameters.Segment = Math.min(
        Math.max(parameters.Segment, MIN_TOTAL_SEGMENTS - 1),
        parameters.TotalSegments - 1,
      );
    }
    const response: ScanOutput = {};
    return retry(async (bail, attempt) => {
      while (!operationCompleted) {
        const qf = new QuickFail(
          attempt * LONG_MAX_LATENCY * (this.patienceRatio || 1),
          new Error(TAKING_TOO_LONG_EXCEPTION),
        );
        try {
          const result = await Promise.race([
            this.DB.scan(parameters).promise(),
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
      return response;
    }, RETRY_OPTIONS);
  };

  [BUILD_PARAMS]() {
    const requestParameters = super[BUILD_PARAMS]();

    return {
      TableName: this.table,
      ...optimizeRequestParameters(requestParameters),
    };
  }

  $execute = async <T = ItemList | undefined | null, U extends boolean = false>(
    returnRawResponse?: U,
    disableRecursion = false,
  ): Promise<U extends true ? ScanOutput : T | undefined | null> => {
    const parameters = { ...(this[BUILD_PARAMS]() as ScanInput) };
    if (parameters.IndexName) {
      delete parameters.ConsistentRead;
    }
    let initialLimit: number | undefined;
    if (parameters.ExclusiveStartKey && !disableRecursion) {
      delete parameters.TotalSegments;
      delete parameters.Segment;
    }
    if (parameters.TotalSegments) {
      parameters.TotalSegments = Math.max(
        Math.min(parameters.TotalSegments, MAX_TOTAL_SEGMENTS),
        MIN_TOTAL_SEGMENTS,
      );
      if (parameters.Limit) {
        const totalSegmentsBasedOnLimit = Math.ceil(parameters.Limit * 0.2);
        parameters.TotalSegments = Math.min(
          parameters.TotalSegments,
          totalSegmentsBasedOnLimit,
        );
        initialLimit = parameters.Limit;
        parameters.Limit = Math.ceil(
          parameters.Limit / parameters.TotalSegments,
        );
      }
    }
    let responses: (ScanOutput | undefined)[] = [];
    if (parameters.Segment != undefined) {
      const segmentParameters = { ...parameters };
      if (!segmentParameters.TotalSegments) {
        segmentParameters.TotalSegments = 1;
      }
      responses = [await this.scanSegment(parameters, disableRecursion)];
    } else {
      responses = await Promise.all(
        [...Array.from({ length: parameters.TotalSegments || 1 }).keys()].map(
          async (segment) => {
            const segmentParameters = { ...parameters };
            if (segmentParameters.TotalSegments) {
              segmentParameters.Segment = segment;
            }
            return this.scanSegment(segmentParameters, disableRecursion);
          },
        ),
      );
    }
    const consolidatedResponse = responses.reduce((p: ScanOutput, c) => {
      const o: ScanOutput = {
        Items: [...(p.Items || []), ...(c?.Items || [])],
        Count: (p.Count || 0) + (c?.Count || 0),
        ScannedCount: (p.ScannedCount || 0) + (c?.ScannedCount || 0),
        ...(disableRecursion
          ? { LastEvaluatedKey: p.LastEvaluatedKey || c?.LastEvaluatedKey }
          : {}),
      };
      if (c?.ConsumedCapacity) {
        if (!p.ConsumedCapacity) {
          o.ConsumedCapacity = c.ConsumedCapacity;
        } else {
          o.ConsumedCapacity = o.ConsumedCapacity || {};
          o.ConsumedCapacity.CapacityUnits =
            (p.ConsumedCapacity.CapacityUnits || 0) +
            (c.ConsumedCapacity?.CapacityUnits || 0);
        }
      }
      return o;
    }, {});
    if (
      initialLimit &&
      (consolidatedResponse.Items?.length || 0) >= initialLimit
    ) {
      consolidatedResponse.Items = consolidatedResponse.Items?.slice(
        0,
        initialLimit,
      );
      consolidatedResponse.Count = consolidatedResponse.Items?.length || 0;
    }
    return (returnRawResponse
      ? consolidatedResponse
      : consolidatedResponse.Items) as any;
  };

  $ = this.$execute;
}
