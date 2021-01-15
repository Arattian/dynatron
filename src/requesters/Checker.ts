import { DocumentClient } from "aws-sdk/clients/dynamodb";

import { Condition } from "../../types/conditions";
import { FullReturnValues, ReturnValues } from "../../types/request";
import { isConditionEmptyDeep } from "../utils/condition-expression-utils";
import { BUILD, BUILD_PARAMS } from "../utils/constants";
import { optimizeRequestParameters } from "../utils/expression-optimization-utils";
import { validateKey } from "../utils/misc-utils";
import { Mutator } from "./_mutator";

export class Checker extends Mutator {
  #ConditionExpression?: Condition[];
  #ReturnValues?: ReturnValues;

  constructor(
    DB: DocumentClient,
    table: string,
    protected key: DocumentClient.Key,
  ) {
    super(DB, table);
    validateKey(key);
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
      Key: this.key,
      TableName: this.table,
      ...optimizeRequestParameters(requestParameters),
    };
  }
}
