import { marshall } from "@aws-sdk/util-dynamodb";
import { and, eq } from "../../src/condition-expression-builders";
import { nextAlpha } from "../../src/utils/next-alpha-char-generator";
import { marshallRequestParameters } from "../../src/utils/request-marshaller";

describe("Request Marshaller", () => {
  test("should return correct object", () => {
    expect(marshallRequestParameters({})).toEqual({});

    expect(marshallRequestParameters({ ClientRequestToken: "TOKEN" })).toEqual({
      ClientRequestToken: "TOKEN",
    });

    expect(marshallRequestParameters({ ConsistentRead: false })).toEqual({});
    expect(marshallRequestParameters({ ConsistentRead: true })).toEqual({
      ConsistentRead: true,
    });

    expect(marshallRequestParameters({ IndexName: "INDEX" })).toEqual({
      IndexName: "INDEX",
    });

    expect(marshallRequestParameters({ Limit: 0 })).toEqual({});
    expect(marshallRequestParameters({ Limit: 1 })).toEqual({ Limit: 1 });

    expect(marshallRequestParameters({ TotalSegments: 0 })).toEqual({});
    expect(marshallRequestParameters({ TotalSegments: 1 })).toEqual({
      TotalSegments: 1,
    });
    expect(marshallRequestParameters({ Segment: 0 })).toEqual({ Segment: 0 });
    expect(marshallRequestParameters({ Segment: 1 })).toEqual({ Segment: 1 });

    expect(marshallRequestParameters({ TableName: "TABLE" })).toEqual({
      TableName: "TABLE",
    });

    const key = { id: "id" };
    expect(marshallRequestParameters({ _Key: key })).toEqual({
      Key: marshall(key),
    });

    expect(marshallRequestParameters({ _Keys: [key] })).toEqual({
      Keys: [marshall(key)],
    });

    expect(marshallRequestParameters({ _ExclusiveStartKey: key })).toEqual({
      ExclusiveStartKey: marshall(key),
    });

    expect(marshallRequestParameters({ _Item: key })).toEqual({
      Item: marshall(key),
    });

    expect(marshallRequestParameters({ _Items: [key] })).toEqual({
      Items: [marshall(key)],
    });

    expect(
      marshallRequestParameters({ ReturnConsumedCapacity: "INDEXES" }),
    ).toEqual({ ReturnConsumedCapacity: "INDEXES" });

    expect(
      marshallRequestParameters({ ReturnItemCollectionMetrics: "SIZE" }),
    ).toEqual({ ReturnItemCollectionMetrics: "SIZE" });

    expect(marshallRequestParameters({ ReturnValues: "ALL_NEW" })).toEqual({
      ReturnValues: "ALL_NEW",
    });

    expect(marshallRequestParameters({ ScanIndexForward: false })).toEqual({
      ScanIndexForward: false,
    });
    expect(marshallRequestParameters({ ScanIndexForward: true })).toEqual({
      ScanIndexForward: true,
    });

    nextAlpha.reset();
    expect(
      marshallRequestParameters({ _ProjectionExpressions: ["id"] }),
    ).toEqual({
      ProjectionExpression: "#projection_a",
      ExpressionAttributeNames: {
        "#projection_a": "id",
      },
    });

    nextAlpha.reset();
    expect(
      marshallRequestParameters({ _FilterExpressions: [eq("id", "value")] }),
    ).toEqual({
      FilterExpression: "#filter_a=:filter_b",
      ExpressionAttributeNames: {
        "#filter_a": "id",
      },
      ExpressionAttributeValues: marshall({
        ":filter_b": "value",
      }),
    });

    nextAlpha.reset();
    expect(
      marshallRequestParameters({
        _KeyConditionExpression: and([eq("id", "value")]),
      }),
    ).toEqual({
      KeyConditionExpression: "#key_a=:key_b",
      ExpressionAttributeNames: {
        "#key_a": "id",
      },
      ExpressionAttributeValues: marshall({
        ":key_b": "value",
      }),
    });

    nextAlpha.reset();
    expect(
      marshallRequestParameters({
        _ConditionExpressions: [eq("id", "value")],
      }),
    ).toEqual({
      ConditionExpression: "#condition_a=:condition_b",
      ExpressionAttributeNames: {
        "#condition_a": "id",
      },
      ExpressionAttributeValues: marshall({
        ":condition_b": "value",
      }),
    });

    nextAlpha.reset();
    expect(
      marshallRequestParameters({
        _UpdateExpressions: [
          { kind: "add", attributePath: "id", value: new Set(["value"]) },
        ],
      }),
    ).toEqual({
      UpdateExpression: "ADD #update_a :update_b",
      ExpressionAttributeNames: {
        "#update_a": "id",
      },
      ExpressionAttributeValues: marshall({
        ":update_b": new Set(["value"]),
      }),
    });
  });
});
