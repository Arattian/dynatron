import retry from "async-retry";
import DynamoDB, {
  AttributeMap,
  DocumentClient,
  PutItemInput,
  PutItemOutput,
} from "aws-sdk/clients/dynamodb";

import { Condition } from "../../types/conditions";
import { FullReturnValues, ReturnValues } from "../../types/request";
import { isConditionEmptyDeep } from "../utils/condition-expression-utils";
import {
  BUILD,
  BUILD_PARAMS,
  RETRY_OPTIONS,
  SHORT_MAX_LATENCY,
  TAKING_TOO_LONG_EXCEPTION,
} from "../utils/constants";
import { optimizeRequestParameters } from "../utils/expression-optimization-utils";
import { isRetryableDBError, QuickFail } from "../utils/misc-utils";
import { Mutator } from "./_mutator";

export class Putter extends Mutator {
  #ConditionExpression?: Condition[];
  #ReturnValues?: ReturnValues;

  constructor(
    DB: DynamoDB,
    table: string,
    private item: DocumentClient.PutItemInputAttributeMap,
  ) {
    super(DB, table);
  }

  returnValues = (returnValues: ReturnValues = "ALL_OLD") => {
    this.#ReturnValues = returnValues;
    return this;
  };

  if = (...arguments_: (Condition | Condition[] | undefined | null)[]) => {
    if (isConditionEmptyDeep(arguments_)) {
      return this;
    }
    this.#ConditionExpression = arguments_.reduce((p: Condition[], c) => {
      if (c == undefined) {
        return p;
      }
      return [...p, ...(Array.isArray(c) ? c : [c])];
    }, this.#ConditionExpression || []);
    return this;
  };

  [BUILD]() {
    return {
      ...super[BUILD](),
      ...(this.#ConditionExpression
        ? { RawConditionExpression: this.#ConditionExpression }
        : {}),
      ...(this.#ReturnValues
        ? { ReturnValues: this.#ReturnValues as FullReturnValues }
        : {}),
    };
  }

  [BUILD_PARAMS]() {
    const requestParameters = super[BUILD_PARAMS]();

    return {
      Item: this.item,
      TableName: this.table,
      ...optimizeRequestParameters(requestParameters),
    };
  }

  $execute = async <
    T = AttributeMap | undefined | null,
    U extends boolean = false
  >(
    returnRawResponse?: U,
  ): Promise<U extends true ? PutItemOutput : T | undefined | null> => {
    const requestParameters = this[BUILD_PARAMS]() as PutItemInput;
    return retry(async (bail, attempt) => {
      const qf = new QuickFail(
        attempt * SHORT_MAX_LATENCY * (this.patienceRatio || 1),
        new Error(TAKING_TOO_LONG_EXCEPTION),
      );
      try {
        const response = await Promise.race([
          this.DB.putItem(requestParameters).promise(),
          qf.wait(),
        ]);
        return (returnRawResponse ? response : requestParameters.Item) as any;
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
