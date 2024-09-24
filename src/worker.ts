/// <reference types="@cloudflare/workers-types" />

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

async function sendBatchToBigQuery(url: string, jwt: string, batch: BatchItem[]): Promise<void> {
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

  const responseBody: { insertErrors?: Array<{ index: number }> } = await response.json();

  if (!response.ok) {
    throw new Error(`BigQuery API error: ${response.status} ${response.statusText} ${batch.length}`);
  }

  if (responseBody.insertErrors && responseBody.insertErrors.length > 0) {
    const firstError = responseBody.insertErrors[0];
    const lineNumber = batch[firstError.index].lineNumber;
    const errorMessage = `BigQuery insert error at line ${lineNumber}: ${JSON.stringify(firstError, null, 2)}`;
    console.error(errorMessage);
    console.error('Problematic object:', JSON.stringify(batch[firstError.index].json, null, 2));
    throw new Error(errorMessage);
  }
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
  let errors: Error[] = [];

  for await (const item of dataStream) {
    if ('error' in item) {
      console.error(`Skipping malformed JSON at line ${item.lineNumber}`);
      continue;
    }

    const { json, bytes, lineNumber } = item;
    totalBytes += bytes;
    totalRecords++;
    
    const recordSizeBytes = JSON.stringify(json).length;
    
    if (batchSizeBytes + recordSizeBytes > maxBatchSizeBytes && batch.length > 0) {
      try {
        await sendBatchToBigQuery(url, jwt, batch);
      } catch (error) {
        errors.push(error as Error);
      }
      batch = [];
      batchSizeBytes = 0;
    }
    
    batch.push({ json, lineNumber });
    batchSizeBytes += recordSizeBytes;
  }

  if (batch.length > 0) {
    try {
      await sendBatchToBigQuery(url, jwt, batch);
    } catch (error) {
      errors.push(error as Error);
    }
  }

  console.log(`Processed ${totalRecords} records, ${(totalBytes/1000000).toFixed(2)} MB`);

  if (errors.length > 0) {
    const errorMessage = `${errors.length} insertion errors:\n${JSON.stringify(errors.slice(0, 10))}`;
    console.error(errorMessage);
    throw new Error(errorMessage);
  }
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
      // Retrieve object key from the request or environment
      const key = 'event_date=2024-09-19/hr=18/cc2f8de2-efe7-466f-be8f-c53b5be6a2b2-18.json.gz';
      const dataStream = await processR2Object(key, env.R2_FATPIPE);
      await insertIntoBq(dataStream, env);
      return new Response('Data processed and inserted successfully', { status: 200 });
    } catch (error: any) {
      console.error("Error in fetch handler:", error);
      return new Response(`Error: ${error.stack}`, { status: 500 });
    }
  },

  async queue(batch: MessageBatch<R2Event>, env: Env, ctx: ExecutionContext): Promise<void> {
    for (const message of batch.messages) {
      try {
        const event = message.body;
        if (event.object.key.startsWith('__temporary')) {
          continue;
        }

        const dataStream = await processR2Object(event.object.key, env.R2_FATPIPE);
        await insertIntoBq(dataStream, env);
      } catch (error) {
        console.error("Error processing R2 event:", (error as Error).stack);
      }
    }
  }
};