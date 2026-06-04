export type SseMessage = {
  event: string;
  data: string;
};

export async function* readSse(
  body: ReadableStream<Uint8Array>,
  signal?: AbortSignal,
): AsyncGenerator<SseMessage> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  const onAbort = () => {
    reader.cancel().catch(() => undefined);
  };
  signal?.addEventListener("abort", onAbort, { once: true });

  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) {
        if (buffer.trim()) {
          const message = parseEvent(buffer);
          if (message) yield message;
        }
        return;
      }
      buffer += decoder.decode(value, { stream: true });
      let separator = findEventSeparator(buffer);
      while (separator !== -1) {
        const separatorLength = buffer.startsWith("\r\n\r\n", separator)
          ? 4
          : 2;
        const rawEvent = buffer.slice(0, separator);
        buffer = buffer.slice(separator + separatorLength);
        const message = parseEvent(rawEvent);
        if (message) yield message;
        separator = findEventSeparator(buffer);
      }
    }
  } finally {
    signal?.removeEventListener("abort", onAbort);
    reader.releaseLock();
  }
}

function parseEvent(rawEvent: string): SseMessage | null {
  let eventName = "message";
  const dataLines: string[] = [];
  for (const line of rawEvent.replaceAll("\r\n", "\n").split("\n")) {
    if (line.startsWith("event: ")) {
      eventName = line.slice(7).trim();
    } else if (line.startsWith("data: ")) {
      dataLines.push(line.slice(6));
    }
  }
  if (dataLines.length === 0) return null;
  return { event: eventName, data: dataLines.join("\n") };
}

function findEventSeparator(buffer: string) {
  const lf = buffer.indexOf("\n\n");
  const crlf = buffer.indexOf("\r\n\r\n");
  if (lf === -1) return crlf;
  if (crlf === -1) return lf;
  return Math.min(lf, crlf);
}
