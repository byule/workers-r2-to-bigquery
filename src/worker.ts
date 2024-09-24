import { generateJWT } from './jwt';
import { ndjsonStream, StreamItem, ErrorItem, JsonObject } from './ndjsonstream';

interface BatchItem {
  json: JsonObject;
  lineNumber: number;
}

export interface Env {
  BQTOKEN: string;
  BQ_PROJECT_ID: string;
  BQ_DATASET_ID: string;
  BQ_TABLE_ID: string;
  R2_FATPIPE: R2Bucket;
}

interface BigQueryResponse {
  insertErrors?: Array<{ index: number; errors: Array<{ message: string }> }>;
}

async function sendBatchToBigQuery(url: string, jwt: string, batch: BatchItem[]): Promise<BigQueryResponse> {
  const payload = {
    kind: 'bigquery#tableDataInsertAllRequest',
    rows: batch.map(({ json }) => ({ json }))
  };

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${jwt}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(payload)
  });

  if (!response.ok) {
    const errorBody = await response.text();
    throw new Error(`BigQuery API error: ${response.status} ${response.statusText}\nResponse body: ${errorBody}`);
  }

  return await response.json();
}

async function insertIntoBq(dataStream: AsyncGenerator<StreamItem | ErrorItem, void, unknown>, env: Env): Promise<void> {
  const token = JSON.parse(env.BQTOKEN);
  const projectId = env.BQ_PROJECT_ID;
  const datasetId = env.BQ_DATASET_ID;
  const tableId = env.BQ_TABLE_ID;
  const url = `https://bigquery.googleapis.com/bigquery/v2/projects/${projectId}/datasets/${datasetId}/tables/${tableId}/insertAll`;
  const jwt = await generateJWT(token);

  const maxBatchSizeBytes = 1024 * 1024;
  let batch: BatchItem[] = [];
  let batchSizeBytes = 0;
  let totalBytes = 0;
  let totalRecords = 0;
  let successfulInserts = 0;
  let failedInserts = 0;
  let skippedRecords = 0;

  for await (const item of dataStream) {
    if ('error' in item) {
      console.error(`Skipping malformed JSON at line ${item.lineNumber}: ${item.error}`);
      skippedRecords++;
      continue;
    }

    const { json, bytes, lineNumber } = item;
    totalBytes += bytes;
    totalRecords++;
    
    const recordSizeBytes = JSON.stringify(json).length;
    
    if (batchSizeBytes + recordSizeBytes > maxBatchSizeBytes && batch.length > 0) {
      const result = await sendBatchToBigQuery(url, jwt, batch);
      const { batchSuccessful, batchFailed } = handleBigQueryResponse(result, batch);
      successfulInserts += batchSuccessful;
      failedInserts += batchFailed;
      batch = [];
      batchSizeBytes = 0;
    }
    
    batch.push({ json, lineNumber });
    batchSizeBytes += recordSizeBytes;
  }

  if (batch.length > 0) {
    const result = await sendBatchToBigQuery(url, jwt, batch);
    const { batchSuccessful, batchFailed } = handleBigQueryResponse(result, batch);
    successfulInserts += batchSuccessful;
    failedInserts += batchFailed;
  }

  console.log(`Operation summary: Processed ${totalRecords} records (${(totalBytes/1000000).toFixed(2)} MB), Successfully inserted: ${successfulInserts}, Failed: ${failedInserts}, Skipped: ${skippedRecords}`);

  if (failedInserts > 0) {
    throw new Error(`Failed to insert ${failedInserts} records`);
  }
}

function handleBigQueryResponse(response: BigQueryResponse, batch: BatchItem[]): { batchSuccessful: number, batchFailed: number } {
  const failedCount = response.insertErrors?.length || 0;
  const successCount = batch.length - failedCount;

  if (failedCount > 0) {
    const firstError = response.insertErrors![0];
    const lineNumber = batch[firstError.index].lineNumber;
    const errorMessage = firstError.errors[0]?.message || 'Unknown error';
    console.error(`Batch insert partial success: ${successCount} inserted, ${failedCount} failed`);
    console.error(`First error at line ${lineNumber}: ${errorMessage}`);
    console.error('Problematic object:', JSON.stringify(batch[firstError.index].json, null, 2));
  }

  return { batchSuccessful: successCount, batchFailed: failedCount };
}

async function processR2Object(key: string, r2Binding: R2Bucket): Promise<AsyncGenerator<StreamItem | ErrorItem, void, unknown>> {
  const obj = await r2Binding.get(key);
  if (!obj) {
    throw new Error(`Object not found: ${key}`);
  }

  const compressedStream = obj.body;
  if (!compressedStream) {
    throw new Error('Object body is null');
  }
  const decompressedStream = compressedStream.pipeThrough(new DecompressionStream('gzip'));
  return ndjsonStream(decompressedStream);
}

interface R2Event {
  object: {
    key: string;
  };
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    try {
      const key = 'event_date=2024-09-19/hr=18/cc2f8de2-efe7-466f-be8f-c53b5be6a2b2-18.json.gz';
      const dataStream = await processR2Object(key, env.R2_FATPIPE);
      await insertIntoBq(dataStream, env);
      return new Response('Data processed and inserted successfully', { status: 200 });
    } catch (error: any) {
      console.error("Error in fetch handler:", error);
      return new Response(`Error: ${error.stack}`, { status: 500 });
    }
  },

  async queue(batch: MessageBatch<R2Event>, env: Env): Promise<void> {
    for (const message of batch.messages) {
      try {
        const event = message.body;
        if (event.object.key.startsWith('__temporary')) {
          continue;
        }

        const dataStream = await processR2Object(event.object.key, env.R2_FATPIPE);
        await insertIntoBq(dataStream, env);
        message.ack();
      } catch (error) {
        console.error("Error processing R2 event:", (error as Error).stack);
        throw error; // Re-throw the error to prevent acknowledging the message
      }
    }
  }
};