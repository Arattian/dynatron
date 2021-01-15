import { DocumentClient } from "aws-sdk/clients/dynamodb";

import { RequestParameters, ReturnConsumedCapacity } from "../../types/request";
import { BUILD, BUILD_PARAMS } from "../utils/constants";
import {
  cleanupEmptyExpressions,
  convertRawConditionExpressions,
  convertRawProjectionExpression,
  convertRawUpdateExpression,
} from "../utils/request-parameters-utils";

export class Requester {
  #ReturnConsumedCapacity?: ReturnConsumedCapacity;
  protected patienceRatio = 1;

  constructor(protected readonly DB: DocumentClient, protected table: string) {}

  returnConsumedCapacity = (
    returnConsumedCapacity: ReturnConsumedCapacity = "TOTAL",
  ) => {
    this.#ReturnConsumedCapacity = returnConsumedCapacity;
    return this;
  };

  relaxLatencies = (patienceRatio = 1) => {
    if (patienceRatio <= 0) {
      throw new Error("The ratio must be positive");
    }
    this.patienceRatio = Math.abs(patienceRatio);
    return this;
  };

  [BUILD]() {
    return {
      ...(this.#ReturnConsumedCapacity
        ? { ReturnConsumedCapacity: this.#ReturnConsumedCapacity }
        : {}),
    };
  }

  [BUILD_PARAMS](queryKey?: DocumentClient.Key) {
    const requestParameters: RequestParameters = this[BUILD]();

    return cleanupEmptyExpressions(
      convertRawUpdateExpression(
        convertRawConditionExpressions(
          convertRawProjectionExpression(
            Object.keys(requestParameters).reduce(
              (p: RequestParameters, c) => ({
                ...p,
                ...(requestParameters[c] != undefined
                  ? { [c]: requestParameters[c] }
                  : {}),
              }),
              {},
            ),
          ),
          queryKey,
        ),
      ),
    );
  }
}
