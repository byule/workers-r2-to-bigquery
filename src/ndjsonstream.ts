export interface JsonObject {
    [key: string]: any;
  }
  
  export interface StreamItem {
    json: JsonObject;
    bytes: number;
    lineNumber: number;
  }
  
  export interface ErrorItem {
    error: true;
    line: string;
    lineNumber: number;
    errorMessage: string;
  }
  
  export async function* ndjsonStream(stream: ReadableStream<Uint8Array>): AsyncGenerator<StreamItem | ErrorItem, void, unknown> {
    const decoder = new TextDecoder();
    const reader = stream.getReader();
    let buffer = '';
    let lineNumber = 0;
  
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop() || '';
  
      for (const line of lines) {
        lineNumber++;
        if (line.trim()) {
          try {
            let json: JsonObject = JSON.parse(line);
            yield { json, bytes: JSON.stringify(json).length, lineNumber };
          } catch (error) {
            yield { error: true, line, lineNumber, errorMessage: (error as Error).message };
          }
        }
      }
    }
  
    if (buffer.trim()) {
      lineNumber++;
      try {
        let json: JsonObject = JSON.parse(buffer);
        yield { json, bytes: JSON.stringify(json).length, lineNumber };
      } catch (error) {
        yield { error: true, line: buffer, lineNumber, errorMessage: (error as Error).message };
      }
    }
  }