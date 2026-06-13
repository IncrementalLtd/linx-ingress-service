import type {
  APIGatewayProxyEventV2,
  APIGatewayProxyResultV2,
} from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, QueryCommand } from '@aws-sdk/lib-dynamodb';

const TABLE_NAME = process.env.TABLE_NAME!;
const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));

const MAX_LIMIT = 1000;
const DEFAULT_LIMIT = 100;

/**
 * Query API: GET /messages
 *   ?messageType=TMCSTC   (required)
 *   &runDate=2026-06-02   (required, the train service run date)
 *   &from=2026-06-02T05:00:00Z   (optional, MessageDateTime lower bound)
 *   &to=2026-06-02T09:00:00Z     (optional, MessageDateTime upper bound)
 *   &limit=100            (optional, max 1000)
 *   &next=<token>         (optional, pagination cursor from a previous response)
 */
export const handler = async (
  event: APIGatewayProxyEventV2,
): Promise<APIGatewayProxyResultV2> => {
  const q = event.queryStringParameters ?? {};
  const messageType = q.messageType?.trim();
  const runDate = q.runDate?.trim();

  if (!messageType || !runDate) {
    return json(400, {
      error: 'Query parameters "messageType" and "runDate" are required.',
    });
  }

  const from = normaliseIso(q.from);
  const to = normaliseIso(q.to);
  const limit = clampLimit(q.limit);

  // SK is `${runDate}#${messageDateTime}#${messageId}`. A run date with no time
  // bounds uses begins_with; with bounds we range over the messageDateTime part.
  const names = { '#pk': 'messageType', '#sk': 'sk' };
  const values: Record<string, unknown> = { ':pk': messageType };
  let keyExpr = '#pk = :pk AND ';

  if (from || to) {
    values[':lo'] = `${runDate}#${from ?? ''}`;
    values[':hi'] = `${runDate}#${to ?? ''}￿`;
    keyExpr += '#sk BETWEEN :lo AND :hi';
  } else {
    values[':prefix'] = `${runDate}#`;
    keyExpr += 'begins_with(#sk, :prefix)';
  }

  try {
    const res = await ddb.send(
      new QueryCommand({
        TableName: TABLE_NAME,
        KeyConditionExpression: keyExpr,
        ExpressionAttributeNames: names,
        ExpressionAttributeValues: values,
        Limit: limit,
        ExclusiveStartKey: decodeCursor(q.next),
        ScanIndexForward: true, // chronological order
      }),
    );

    return json(200, {
      messageType,
      runDate,
      count: res.Items?.length ?? 0,
      items: res.Items ?? [],
      next: encodeCursor(res.LastEvaluatedKey),
    });
  } catch (err) {
    console.error('Query failed:', err);
    return json(500, { error: 'Query failed.' });
  }
};

function clampLimit(raw: string | undefined): number {
  const n = raw ? parseInt(raw, 10) : NaN;
  if (isNaN(n) || n <= 0) return DEFAULT_LIMIT;
  return Math.min(n, MAX_LIMIT);
}

function normaliseIso(value: string | undefined): string | undefined {
  if (!value) return undefined;
  const d = new Date(value);
  return isNaN(d.getTime()) ? value : d.toISOString();
}

function encodeCursor(key: Record<string, unknown> | undefined): string | undefined {
  return key ? Buffer.from(JSON.stringify(key)).toString('base64url') : undefined;
}

function decodeCursor(token: string | undefined): Record<string, unknown> | undefined {
  if (!token) return undefined;
  try {
    return JSON.parse(Buffer.from(token, 'base64url').toString('utf8'));
  } catch {
    return undefined;
  }
}

function json(statusCode: number, body: unknown): APIGatewayProxyResultV2 {
  return {
    statusCode,
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  };
}
