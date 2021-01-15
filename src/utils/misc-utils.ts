import { AWSError, Credentials, SharedIniFileCredentials } from "aws-sdk";
import DynamoDB, {
  AttributeMap,
  AttributeValue,
  BinaryAttributeValue,
  ClientConfiguration,
  DocumentClient,
  Key,
} from "aws-sdk/clients/dynamodb";
import { ServiceConfigurationOptions } from "aws-sdk/lib/service";
import https from "https";
import { nanoid } from "nanoid";

import {
  DirectConnectionWithCredentials,
  DirectConnectionWithProfile,
  DynatronConnectionParameters,
} from "../../types/request";
import { LONG_MAX_LATENCY, TAKING_TOO_LONG_EXCEPTION } from "./constants";

export const serializeExpressionValue = (value: AttributeValue) => ({
  name: `:${nanoid()}`,
  value,
});

export const validateKey = (key: Key, singlePropertyKey = false) => {
  if (Object.keys(key).length === 0) {
    throw new Error("At least 1 property must be present in the key");
  }
  const maxKeys = singlePropertyKey ? 1 : 2;
  if (Object.keys(key).length > maxKeys) {
    throw new Error(
      `At most ${maxKeys} ${
        maxKeys === 1 ? "property" : "properties"
      } must be present in the key`,
    );
  }
};

export const assertNever = (object: never): never => {
  throw new Error(`Unexpected value: ${JSON.stringify(object)}`);
};

export const isRetryableDBError = (error: Error) =>
  error.message === TAKING_TOO_LONG_EXCEPTION ||
  (Object.prototype.hasOwnProperty.call(error, "retryable") &&
    (error as AWSError).retryable) ||
  error.toString().toUpperCase().includes("ECONN") ||
  error.toString().toUpperCase().includes("NetworkingError") ||
  error.toString().toUpperCase().includes("InternalServerError") ||
  (Object.prototype.hasOwnProperty.call(error, "code") &&
    ["ProvisionedThroughputExceededException", "ThrottlingException"].includes(
      (error as AWSError).code,
    ));

export const setOfValues = (
  values:
    | string
    | number
    | BinaryAttributeValue
    | (string | number | BinaryAttributeValue)[],
) =>
  new DocumentClient().createSet(Array.isArray(values) ? values : [values], {
    validate: true,
  });

export const preStringify = (attributeMap: AttributeMap) => {
  Object.entries(attributeMap).forEach(([key, value]) => {
    if (value == undefined || value.constructor.name !== "Set") {
      return;
    }

    attributeMap[key] = {
      wrapperName: "Set",
      values: (value as DocumentClient.DynamoDbSet).values,
      type: (value as DocumentClient.DynamoDbSet).type,
    } as AttributeValue;
  });

  return attributeMap;
};

const bootstrapDynamoDBOptions = (
  parameters?: DynatronConnectionParameters,
) => {
  const options: ClientConfiguration & ServiceConfigurationOptions = {
    maxRetries: 3,
  };

  if (parameters == undefined || parameters?.mode === "direct") {
    // Experiments have shown that this is the optimal number for sockets
    const MAX_SOCKETS = 256;

    const dynamoDBHttpsAgent = new https.Agent({
      keepAlive: true,
      rejectUnauthorized: true,
      maxSockets: MAX_SOCKETS,
      maxFreeSockets: MAX_SOCKETS / 8,
      secureProtocol: "TLSv1_method",
      ciphers: "ALL",
    });

    options.httpOptions = {
      agent: dynamoDBHttpsAgent,
      timeout: parameters?.timeout || LONG_MAX_LATENCY + 1000,
    };
  }

  if (parameters?.mode === "direct") {
    if ((parameters as DirectConnectionWithProfile).profile) {
      options.credentials = new SharedIniFileCredentials({
        profile: (parameters as DirectConnectionWithProfile).profile,
      });
    } else {
      options.credentials = new Credentials({
        accessKeyId: (parameters as DirectConnectionWithCredentials)
          .accessKeyId,
        secretAccessKey: (parameters as DirectConnectionWithCredentials)
          .secretAccessKey,
      });
    }

    options.region = (parameters as DirectConnectionWithProfile).region;
  }

  if (parameters?.mode === "local") {
    options.endpoint = `http://${parameters?.host || "localhost"}:${
      parameters?.port || 8000
    }`;
    options.region = "localhost";
    options.credentials = parameters?.profile
      ? new SharedIniFileCredentials({
          profile: parameters?.profile,
        })
      : new Credentials({
          accessKeyId: parameters?.accessKeyId || "localAwsAccessKeyId",
          secretAccessKey:
            parameters?.secretAccessKey || "localAwsSecretAccessKey",
        });
  }

  return options;
};

export const initDB = (parameters?: DynatronConnectionParameters) =>
  new DynamoDB(bootstrapDynamoDBOptions(parameters));

export class QuickFail {
  #timeoutReference?: NodeJS.Timeout;
  constructor(private duration: number, private error: Error) {}

  wait = async (): Promise<never> => {
    return new Promise((_, reject) => {
      this.#timeoutReference = setTimeout(() => {
        reject(this.error);
      }, this.duration);
    });
  };

  cancel = () => {
    if (this.#timeoutReference == undefined) {
      return;
    }
    clearTimeout(this.#timeoutReference);
  };
}
