import {
  ExpressionAttributeNameMap,
  ExpressionAttributeValueMap,
} from "aws-sdk/clients/dynamodb";
import fastEquals from "fast-deep-equal";

import { RequestParameters as RequestParameters } from "../../types/request";

export const optimizeExpression = (
  expression: string,
  attributeNames: ExpressionAttributeNameMap,
  attributeValues?: ExpressionAttributeValueMap,
): {
  Expression: string;
  ExpressionAttributeNames: ExpressionAttributeNameMap;
  ExpressionAttributeValues?: ExpressionAttributeValueMap;
} => {
  let optimizedExpression = expression;
  const optimizedNames: Record<string, string> = {};
  const optimizedValues: Record<string, any> = {};

  Object.keys(attributeNames).forEach((key) => {
    if (optimizedNames[attributeNames[key]] == undefined) {
      optimizedNames[attributeNames[key]] = key;
    } else {
      optimizedExpression = optimizedExpression
        .split(key)
        .join(optimizedNames[attributeNames[key]]);
    }
  });

  if (attributeValues != undefined) {
    Object.keys(attributeValues).forEach((key) => {
      const value = attributeValues[key];

      const optimizedKey = Object.keys(optimizedValues).find((k) =>
        fastEquals(optimizedValues[k], value),
      );

      if (optimizedKey) {
        optimizedExpression = optimizedExpression.split(key).join(optimizedKey);
      } else {
        optimizedValues[key] = value;
      }
    });
  }

  return {
    Expression: optimizedExpression,
    ExpressionAttributeNames: Object.keys(optimizedNames).reduce(
      (p, c) => ({ ...p, [optimizedNames[c]]: c }),
      {},
    ),
    ...(attributeValues != undefined
      ? { ExpressionAttributeValues: optimizedValues }
      : {}),
  };
};

export const optimizeRequestParameters = (
  requestParameters: RequestParameters,
) => {
  const expressions = {
    ConditionExpression: requestParameters.ConditionExpression,
    FilterExpression: requestParameters.FilterExpression,
    KeyConditionExpression: requestParameters.KeyConditionExpression,
    ProjectionExpression: requestParameters.ProjectionExpression,
    UpdateExpression: requestParameters.UpdateExpression,
  };

  const optimizedNames: Record<string, string> = {};
  const optimizedValues: Record<string, any> = {};

  if (
    requestParameters.ExpressionAttributeNames &&
    Object.keys(requestParameters.ExpressionAttributeNames).length > 0
  ) {
    const attributeNames = requestParameters.ExpressionAttributeNames;
    Object.keys(requestParameters.ExpressionAttributeNames).forEach((key) => {
      if (optimizedNames[attributeNames[key]] != undefined) {
        Object.keys(expressions).forEach((expressionType) => {
          expressions[expressionType] = (expressions[expressionType] || "")
            .split(key)
            .join(optimizedNames[attributeNames[key]]);
        });
      } else {
        optimizedNames[attributeNames[key]] = key;
      }
    });

    requestParameters.ExpressionAttributeNames = optimizedNames;
  }

  if (
    requestParameters.ExpressionAttributeValues &&
    Object.keys(requestParameters.ExpressionAttributeValues).length > 0
  ) {
    const attributeValues = requestParameters.ExpressionAttributeValues;
    Object.keys(attributeValues).forEach((key) => {
      const value = attributeValues[key];

      const optimizedKey = Object.keys(optimizedValues).find((k) =>
        fastEquals(optimizedValues[k], value),
      );

      if (optimizedKey) {
        Object.keys(expressions).forEach((expressionType) => {
          expressions[expressionType] = (expressions[expressionType] || "")
            .split(key)
            .join(optimizedKey);
        });
      } else {
        optimizedValues[key] = value;
      }
    });
  }

  if (Object.keys(optimizedNames).length > 0) {
    requestParameters.ExpressionAttributeNames = Object.keys(
      optimizedNames,
    ).reduce((p, c) => ({ ...p, [optimizedNames[c]]: c }), {});
  }

  if (Object.keys(optimizedValues).length > 0) {
    requestParameters.ExpressionAttributeValues = optimizedValues;
  }

  Object.keys(expressions).forEach((expressionType) => {
    if (expressions[expressionType]) {
      requestParameters[expressionType] = expressions[expressionType];
    }
  });

  // Shorter keys
  if (
    requestParameters.ExpressionAttributeNames &&
    Object.keys(requestParameters.ExpressionAttributeNames).length > 0
  ) {
    const attributeNames = requestParameters.ExpressionAttributeNames;
    const optimizedAttributeNames = {};
    Object.keys(attributeNames).forEach((key, index) => {
      const prefixedKey = `#n${index}`;
      Object.keys(expressions).forEach((expressionType) => {
        expressions[expressionType] = (expressions[expressionType] || "")
          .split(key)
          .join(prefixedKey);
      });

      optimizedAttributeNames[prefixedKey] = attributeNames[key];
    });
    requestParameters.ExpressionAttributeNames = optimizedAttributeNames;
  }

  if (
    requestParameters.ExpressionAttributeValues &&
    Object.keys(requestParameters.ExpressionAttributeValues).length > 0
  ) {
    const attributeValues = requestParameters.ExpressionAttributeValues;
    const optimizedAttributeValues = {};
    Object.keys(attributeValues).forEach((key, index) => {
      const prefixedKey = `:v${index}`;
      Object.keys(expressions).forEach((expressionType) => {
        expressions[expressionType] = (expressions[expressionType] || "")
          .split(key)
          .join(prefixedKey);
      });

      optimizedAttributeValues[prefixedKey] = attributeValues[key];
    });
    requestParameters.ExpressionAttributeValues = optimizedAttributeValues;
  }

  Object.keys(expressions).forEach((expressionType) => {
    if (!expressions[expressionType]) {
      return;
    }
    requestParameters[expressionType] = expressions[expressionType];
  });

  return requestParameters;
};
