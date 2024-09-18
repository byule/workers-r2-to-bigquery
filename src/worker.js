import { generateJWT } from "./jwt.js";

async function* streamNdjsonObjects(stream) {
  const decoder = new TextDecoder();
  const reader = stream.getReader();
  let buffer = '';
  let lineNumber = 0;
  let isFirstRow = true;

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    
    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split('\n');
    buffer = lines.pop(); // Keep the last potentially incomplete line in the buffer

    for (const line of lines) {
      lineNumber++;
      if (line.trim()) {
        try {
          let json = JSON.parse(line);
          
          // Print the first row
          if (isFirstRow) {
            console.log("First row:", JSON.stringify(json, null, 2));
            isFirstRow = false;
          }

          // Add 'generated_at' if it doesn't exist
          if (!json.hasOwnProperty('generated_at')) {
            json.generated_at = new Date().toISOString();
          }

          yield { json, bytes: JSON.stringify(json).length, lineNumber };
        } catch (error) {
          console.error(`Malformed JSON at line ${lineNumber}:`, line);
          console.error('Parse error:', error.message);
          yield { error: true, line, lineNumber, errorMessage: error.message };
        }
      }
    }
  }

  if (buffer.trim()) {
    lineNumber++;
    try {
      let json = JSON.parse(buffer);
      
      // Print the first row if it hasn't been printed yet
      if (isFirstRow) {
        console.log("First row:", JSON.stringify(json, null, 2));
      }

      // Add 'generated_at' if it doesn't exist
      if (!json.hasOwnProperty('generated_at')) {
        json.generated_at = new Date().toISOString();
      }

      yield { json, bytes: JSON.stringify(json).length, lineNumber };
    } catch (error) {
      console.error(`Malformed JSON at line ${lineNumber}:`, buffer);
      console.error('Parse error:', error.message);
      yield { error: true, line: buffer, lineNumber, errorMessage: error.message };
    }
  }
}

async function sendBatchToBigQuery(url, jwt, batch) {
  const payload = {
    kind: 'bigquery#tableDataInsertAllRequest',
    rows: batch.map(({ json }) => ({ json }))
  };

  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${jwt}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(payload)
    });

    const responseBody = await response.json();

    if (!response.ok) {
      throw new Error(`BigQuery API error: ${response.status} ${response.statusText} ${batch.length}`);
    }

    // Check for insert errors and throw an exception if any are found
    if (responseBody.insertErrors && responseBody.insertErrors.length > 0) {
      const firstError = responseBody.insertErrors[0];
      const lineNumber = batch[firstError.index].lineNumber;
      const errorMessage = `BigQuery insert error at line ${lineNumber}: ${JSON.stringify(firstError, null, 2)}`;
      console.error(errorMessage);
      console.error('Problematic object:', JSON.stringify(batch[firstError.index].json, null, 2));
      throw new Error(errorMessage);
    }
  } catch (error) {
    console.error('Error sending batch to BigQuery:', error.message);
    console.error('First problematic object:', JSON.stringify(batch[0].json, null, 2));
    throw error;  // Re-throw the error to be caught in insertIntoBq
  }
}

async function insertIntoBq(dataStream, env) {
  const token = JSON.parse(env.BQTOKEN);
  const url = `https://bigquery.googleapis.com/bigquery/v2/projects/cloudflare-workers-perf/datasets/fatpipe/tables/fatpipe/insertAll`;
  const jwt = await generateJWT(token);

  const maxBatchSizeBytes = 1024*1024;
  let batch = [];
  let batchSizeBytes = 0;
  let totalBytes = 0;
  let totalRecords = 0;
  let errors = [];

  for await (const item of dataStream) {
    if (item.error) {
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
        errors.push(error);
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
      errors.push(error);
    }
  }

  console.log(`Processed ${totalRecords} records, ${(totalBytes/1000000).toFixed(2)} MB`);

  if (errors.length > 0) {
    const errorMessage = `Encountered ${errors.length} errors during batch insertion`;
    console.error(errorMessage);
    throw new Error(errorMessage);
  } else {
    console.log('All batches inserted successfully');
  }
}

// wtf
async function processR2Object(key, r2Binding) {
  const obj = await r2Binding.get(key);
  if (!obj) {
    throw new Error(`Object not found: ${key}`);
  }

  console.log(`File Length: ${obj.range.length/1000000}MB`);

  const compressedStream = obj.body;
  const decompressedStream = compressedStream.pipeThrough(new DecompressionStream('gzip'));
  return streamNdjsonObjects(decompressedStream);
}

export default {
  async fetch(request, env, ctx) {
    try {
      //const key = 'event_date=2024-09-12/hr=16/e4f7f444-6167-4ce7-b264-de487f9e7ed2.json.gz';
      const dataStream = await processR2Object(key, env.R2_FATPIPE);
      await insertIntoBq(dataStream, env);
      return new Response('Data processed and inserted successfully', { status: 200 });
    } catch (error) {
      console.error("Error in fetch handler:", error);
      return new Response(`Error: ${error.stack}`, { status: 500 });
    }
  },

  async queue(batch, env) {
    for (const message of batch.messages) {
      try {
        const event = message.body;
        console.log("Received R2 event:", JSON.stringify(event, null, 2));

        // Bail early if the object key starts with '__temporary'
        if (event.object.key.startsWith('__temporary')) {
          console.log(`Skipping temporary file: ${event.object.key}`);
          continue;
        }

        const dataStream = await processR2Object(event.object.key, env.R2_FATPIPE);
        await insertIntoBq(dataStream, env);

        console.log(`Processed and inserted data for key: ${event.object.key}`);
      } catch (error) {
        console.error("Error processing R2 event:", error.stack);
      }
    }
  }
};