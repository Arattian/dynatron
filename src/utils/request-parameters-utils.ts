import { DocumentClient } from "aws-sdk/clients/dynamodb";

import { EqualsCondition } from "../../types/conditions";
import { RequestParameters } from "../../types/request";
import { RawUpdate, RawUpdateType } from "../../types/update";
import { serializeAttributePath } from "./attribute-path-utils";
import {
  and,
  serializeConditionExpression,
} from "./condition-expression-utils";
import { optimizeExpression } from "./expression-optimization-utils";
import { serializeUpdateExpression } from "./update-expression-utils";

export const convertRawProjectionExpression = (
  requestParameters: RequestParameters,
) => {
  const parameters = { ...requestParameters };

  if (!parameters.RawProjectionExpression) {
    return parameters;
  }

  const projectionObject = [
    ...new Set(parameters.RawProjectionExpression || []),
  ]
    .map((projection) => serializeAttributePath(projection))
    .reduce(
      (
        p: {
          expressions: string[];
          expressionAttributeNames: Record<string, string>;
        },
        c,
      ) => {
        return {
          expressions: [...new Set([...p.expressions, c.expression])],
          expressionAttributeNames: {
            ...p.expressionAttributeNames,
            ...c.expressionAttributeNames,
          },
        };
      },
      { expressions: [], expressionAttributeNames: {} },
    );
  const projectionExpression = projectionObject?.expressions
    .filter((t) => t.trim() !== "")
    .join(", ");
  const projectionExpressionAttributeNames =
    projectionObject?.expressionAttributeNames;
  if (
    projectionExpression == undefined ||
    projectionExpressionAttributeNames == undefined
  ) {
    return parameters;
  }
  const { Expression, ExpressionAttributeNames } = optimizeExpression(
    projectionExpression,
    projectionExpressionAttributeNames,
  );

  parameters.ProjectionExpression = Expression;
  delete parameters.RawProjectionExpression;

  parameters.ExpressionAttributeNames =
    parameters.ExpressionAttributeNames || {};
  parameters.ExpressionAttributeNames = {
    ...parameters.ExpressionAttributeNames,
    ...ExpressionAttributeNames,
  };

  return parameters;
};

export const convertRawConditionExpressions = (
  requestParameters: RequestParameters,
  queryKey?: DocumentClient.Key,
) => {
  const parameters = { ...requestParameters };

  if (queryKey != undefined) {
    parameters.RawKeyConditionExpression =
      parameters.RawKeyConditionExpression || [];

    const partitionKeyCondition: EqualsCondition = {
      kind: "=",
      path: Object.keys(queryKey)[0],
      value: queryKey[Object.keys(queryKey)[0]],
    };

    parameters.RawKeyConditionExpression.unshift(partitionKeyCondition);
  }

  const expressionTypes = [
    "KeyConditionExpression",
    "ConditionExpression",
    "FilterExpression",
  ];

  expressionTypes.forEach((expressionType) => {
    if (parameters[`Raw${expressionType}`]) {
      const {
        Expression: ConditionExpression,
        ExpressionAttributeNames,
        ExpressionAttributeValues,
      } = serializeConditionExpression(and(parameters[`Raw${expressionType}`]));

      const {
        Expression,
        ExpressionAttributeNames: OptimizedExpressionAttributeNames,
        ExpressionAttributeValues: OptimizedExpressionAttributeValues,
      } = optimizeExpression(
        ConditionExpression,
        ExpressionAttributeNames,
        ExpressionAttributeValues,
      );

      parameters[expressionType] = Expression;
      delete parameters[`Raw${expressionType}`];

      parameters.ExpressionAttributeNames =
        parameters.ExpressionAttributeNames || {};
      parameters.ExpressionAttributeNames = {
        ...parameters.ExpressionAttributeNames,
        ...OptimizedExpressionAttributeNames,
      };
      parameters.ExpressionAttributeValues =
        parameters.ExpressionAttributeValues || {};
      parameters.ExpressionAttributeValues = {
        ...parameters.ExpressionAttributeValues,
        ...OptimizedExpressionAttributeValues,
      };
    }
  });
  return parameters;
};

export const convertRawUpdateExpression = (
  requestParameters: RequestParameters,
) => {
  const parameters = { ...requestParameters };

  if (!parameters.RawUpdateExpression) {
    return parameters;
  }

  const updateMap: { [group in RawUpdateType]?: RawUpdate[] } = {};

  const updateObject = {
    expression: "",
    expressionAttributeNames: {},
    expressionAttributeValues: {},
  };

  parameters.RawUpdateExpression.forEach((expression) => {
    const { Type, ...updateExpression } = serializeUpdateExpression(expression);
    updateMap[Type] = updateMap[Type] || [];
    (updateMap[Type] || []).push(updateExpression);
  });

  Object.keys(updateMap).forEach((updateGroup) => {
    const group: RawUpdate[] = updateMap[updateGroup];
    const flatGroup = group.reduce((p, c) => {
      return {
        Expression: p.Expression
          ? `${p.Expression}, ${c.Expression}`
          : c.Expression,
        ExpressionAttributeNames: {
          ...p.ExpressionAttributeNames,
          ...c.ExpressionAttributeNames,
        },
        ExpressionAttributeValues: {
          ...p.ExpressionAttributeValues,
          ...c.ExpressionAttributeValues,
        },
      };
    });

    if (!flatGroup.Expression) {
      return;
    }

    updateObject.expression =
      updateObject.expression +
      ` ${updateGroup.toUpperCase()} ${flatGroup.Expression}`;

    updateObject.expressionAttributeNames = {
      ...updateObject.expressionAttributeNames,
      ...flatGroup.ExpressionAttributeNames,
    };

    updateObject.expressionAttributeValues = {
      ...updateObject.expressionAttributeValues,
      ...flatGroup.ExpressionAttributeValues,
    };
  });

  const {
    Expression,
    ExpressionAttributeNames,
    ExpressionAttributeValues,
  } = optimizeExpression(
    updateObject.expression.trim(),
    updateObject.expressionAttributeNames,
    updateObject.expressionAttributeValues,
  );
  parameters.UpdateExpression = Expression;
  delete parameters.RawUpdateExpression;
  parameters.ExpressionAttributeNames =
    parameters.ExpressionAttributeNames || {};
  parameters.ExpressionAttributeNames = {
    ...parameters.ExpressionAttributeNames,
    ...ExpressionAttributeNames,
  };
  parameters.ExpressionAttributeValues = {
    ...parameters.ExpressionAttributeValues,
    ...ExpressionAttributeValues,
  };

  return parameters;
};

export const cleanupEmptyExpressions = (
  requestParameters: RequestParameters,
) => {
  const parameters = { ...requestParameters };
  if (Object.keys(parameters.ExpressionAttributeNames || {}).length === 0) {
    delete parameters.ExpressionAttributeNames;
  }
  if (Object.keys(parameters.ExpressionAttributeValues || {}).length === 0) {
    delete parameters.ExpressionAttributeValues;
  }
  return parameters;
};
