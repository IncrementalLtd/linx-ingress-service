import type {
  KinesisStreamEvent,
  KinesisStreamBatchResponse,
} from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, BatchWriteCommand } from '@aws-sdk/lib-dynamodb';
import { XMLParser } from 'fast-xml-parser';

const TABLE_NAME = process.env.TABLE_NAME!;
const MESSAGE_TYPE = process.env.MESSAGE_TYPE!;

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}), {
  marshallOptions: { removeUndefinedValues: true },
});

// Parse to the same shape as the JSON examples (same attribute/text key names),
// and keep every value as a string so rail codes like "0070"/"01" aren't coerced
// to numbers (which would drop leading zeros).
const xmlParser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '_',
  textNodeName: '__text',
  parseTagValue: false,
  parseAttributeValue: false,
  trimValues: true,
  // Drop the <?xml ...?> declaration so the message element is the only root key.
  ignoreDeclaration: true,
});

/** A parsed record paired with its Kinesis sequence number (for failure reporting). */
interface Parsed {
  sequenceNumber: string;
  item: Record<string, unknown>;
}

/**
 * Kinesis consumer: decodes each record (the raw JSON message body the producer
 * published), extracts the key fields, and writes to DynamoDB. Returns partial
 * batch failures so only records that fail to persist are retried — parse
 * failures are logged and dropped (retrying them would never succeed).
 */
export const handler = async (
  event: KinesisStreamEvent,
): Promise<KinesisStreamBatchResponse> => {
  const parsed: Parsed[] = [];

  for (const record of event.Records) {
    const seq = record.kinesis.sequenceNumber;
    try {
      const text = Buffer.from(record.kinesis.data, 'base64').toString('utf8');
      parsed.push({ sequenceNumber: seq, item: buildItem(text) });
    } catch (err) {
      console.error(`Skipping unparseable record ${seq}:`, err);
    }
  }

  const failures: { itemIdentifier: string }[] = [];

  // BatchWriteItem allows max 25 items per call.
  for (let i = 0; i < parsed.length; i += 25) {
    const chunk = parsed.slice(i, i + 25);
    try {
      await writeChunk(chunk.map((p) => p.item));
    } catch (err) {
      console.error(`Batch write failed for ${chunk.length} record(s):`, err);
      for (const p of chunk) failures.push({ itemIdentifier: p.sequenceNumber });
    }
  }

  console.log(
    `${MESSAGE_TYPE}: stored ${parsed.length - failures.length}/${event.Records.length} message(s)` +
      (failures.length ? `, ${failures.length} to retry` : ''),
  );

  return { batchItemFailures: failures };
};

/** Write a chunk (<=25), retrying any UnprocessedItems with a short backoff. */
async function writeChunk(items: Record<string, unknown>[]): Promise<void> {
  let request: Record<string, unknown>[] | undefined = items.map((Item) => ({
    PutRequest: { Item },
  }));

  for (let attempt = 0; attempt < 5 && request && request.length; attempt++) {
    const res = await ddb.send(
      new BatchWriteCommand({ RequestItems: { [TABLE_NAME]: request } }),
    );
    const unprocessed = res.UnprocessedItems?.[TABLE_NAME];
    request = unprocessed && unprocessed.length ? (unprocessed as typeof request) : undefined;
    if (request) await sleep(100 * (attempt + 1));
  }

  if (request && request.length) {
    throw new Error(`${request.length} item(s) still unprocessed after retries`);
  }
}

/** Build the DynamoDB item from the raw message body (XML or JSON). */
function buildItem(raw: string): Record<string, unknown> {
  const parsed = parseMessage(raw);
  // Each message has a single root object (TrainJourneyModificationMessage,
  // PathDetailsMessage, ...).
  const root = parsed[Object.keys(parsed)[0]] ?? {};
  const ref = root?.MessageHeader?.MessageReference ?? {};

  const messageIdentifier: string = ref.MessageIdentifier ?? 'unknown';
  const messageDateTime = normaliseIso(ref.MessageDateTime);
  const runDate = extractStartDate(root) ?? messageDateTime?.slice(0, 10) ?? 'unknown';

  return {
    messageType: MESSAGE_TYPE,
    sk: `${runDate}#${messageDateTime ?? ''}#${messageIdentifier}`,
    runDate,
    messageDateTime,
    messageIdentifier,
    internalMessageType: ref.MessageType, // e.g. "9004", "2003"
    payload: parsed,
    ingestedAt: new Date().toISOString(),
  };
}

/** Parse the message body: XML (the live feed) or JSON, into the same shape. */
function parseMessage(raw: string): Record<string, any> {
  const text = raw.trimStart();
  return text.startsWith('<')
    ? (xmlParser.parse(text) as Record<string, any>)
    : (JSON.parse(text) as Record<string, any>);
}

/**
 * The operational run date = the StartDate of the train ('TR') identifier, taken
 * from whichever identifier collection the message carries.
 */
function extractStartDate(root: any): string | undefined {
  const collections = [
    root?.TrainOperationalIdentification?.TransportOperationalIdentifiers,
    root?.Identifiers?.PlannedTransportIdentifiers,
  ];
  for (const collection of collections) {
    // XML yields a single object (not an array) when only one identifier is present.
    const arr = toArray(collection);
    const train = arr.find((x) => x?.ObjectType === 'TR' && x?.StartDate);
    if (train) return String(train.StartDate);
    const any = arr.find((x) => x?.StartDate);
    if (any) return String(any.StartDate);
  }
  return undefined;
}

function toArray(value: any): any[] {
  if (value === undefined || value === null) return [];
  return Array.isArray(value) ? value : [value];
}

/** Normalise to sortable UTC ISO (e.g. 2026-06-02T07:46:43.000Z); raw on failure. */
function normaliseIso(value: unknown): string | undefined {
  if (typeof value !== 'string' || !value) return undefined;
  const d = new Date(value);
  return isNaN(d.getTime()) ? value : d.toISOString();
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
