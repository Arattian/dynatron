import retry from "async-retry";
import DynamoDB, { UpdateTimeToLiveInput } from "aws-sdk/clients/dynamodb";

import {
  BUILD_PARAMS,
  RETRY_OPTIONS,
  SHORT_MAX_LATENCY,
  TAKING_TOO_LONG_EXCEPTION,
} from "../../utils/constants";
import { isRetryableDBError, QuickFail } from "../../utils/misc-utils";

export class TableTTLUpdater {
  constructor(
    private DB: DynamoDB,
    private parameters: UpdateTimeToLiveInput,
  ) {}

  [BUILD_PARAMS]() {
    return { ...this.parameters };
  }

  $execute = async () => {
    return retry(async (bail, attempt) => {
      const qf = new QuickFail(
        attempt * SHORT_MAX_LATENCY,
        new Error(TAKING_TOO_LONG_EXCEPTION),
      );
      try {
        const response = await Promise.race([
          this.DB.updateTimeToLive(
            this[BUILD_PARAMS]() as UpdateTimeToLiveInput,
          ).promise(),
          qf.wait(),
        ]);
        return response.TimeToLiveSpecification;
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
