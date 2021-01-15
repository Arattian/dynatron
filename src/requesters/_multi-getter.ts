import {
  ConsistentRead,
  DocumentClient,
  IndexName,
  Key,
  PositiveIntegerObject,
} from "aws-sdk/clients/dynamodb";

import { Condition } from "../../types/conditions";
import { isConditionEmptyDeep } from "../utils/condition-expression-utils";
import { BUILD } from "../utils/constants";
import { Requester } from "./_requester";

export class MultiGetter extends Requester {
  #ConsistentRead?: ConsistentRead;
  #ExclusiveStartKey?: DocumentClient.Key;
  #FilterExpression?: Condition[];
  #IndexName?: IndexName;
  #Limit?: PositiveIntegerObject;
  #ProjectionExpression?: string[];

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

  where = (...arguments_: (Condition | Condition[] | undefined | null)[]) => {
    if (isConditionEmptyDeep(arguments_)) {
      return this;
    }
    this.#FilterExpression = arguments_.reduce((p: Condition[], c) => {
      if (c == undefined) {
        return p;
      }
      return [...p, ...(Array.isArray(c) ? c : [c])];
    }, this.#FilterExpression || []);
    return this;
  };

  indexName = (indexName: IndexName) => {
    this.#IndexName = indexName;
    return this;
  };

  limit = (limit: PositiveIntegerObject) => {
    this.#Limit = limit;
    return this;
  };

  start = (exclusiveStartKey: Key | null) => {
    if (exclusiveStartKey != undefined) {
      this.#ExclusiveStartKey = exclusiveStartKey;
    }
    return this;
  };

  [BUILD]() {
    return {
      ...super[BUILD](),
      ...(this.#ConsistentRead ? { ConsistentRead: this.#ConsistentRead } : {}),
      ...(this.#ProjectionExpression
        ? { RawProjectionExpression: this.#ProjectionExpression }
        : {}),
      ...(this.#FilterExpression
        ? { RawFilterExpression: this.#FilterExpression }
        : {}),
      ...(this.#IndexName ? { IndexName: this.#IndexName } : {}),
      ...(this.#Limit ? { Limit: this.#Limit } : {}),
      ...(this.#ExclusiveStartKey
        ? { ExclusiveStartKey: this.#ExclusiveStartKey }
        : {}),
    };
  }
}
