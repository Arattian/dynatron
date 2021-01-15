import retry from "async-retry";
import {
  AttributeMap,
  DeleteItemInput,
  DeleteItemOutput,
} from "aws-sdk/clients/dynamodb";

import {
  BUILD_PARAMS,
  RETRY_OPTIONS,
  SHORT_MAX_LATENCY,
  TAKING_TOO_LONG_EXCEPTION,
} from "../utils/constants";
import { isRetryableDBError, QuickFail } from "../utils/misc-utils";
import { Checker } from "./checker";

export class Deleter extends Checker {
  $execute = async <
    T = AttributeMap | undefined | null,
    U extends boolean = false
  >(
    returnRawResponse?: U,
  ): Promise<U extends true ? DeleteItemOutput : T | undefined | null> => {
    return retry(async (bail, attempt) => {
      const qf = new QuickFail(
        attempt * SHORT_MAX_LATENCY * (this.patienceRatio || 1),
        new Error(TAKING_TOO_LONG_EXCEPTION),
      );
      try {
        const response = await Promise.race([
          this.DB.deleteItem(this[BUILD_PARAMS]() as DeleteItemInput).promise(),
          qf.wait(),
        ]);
        return (returnRawResponse ? response : response.Attributes) as any;
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
