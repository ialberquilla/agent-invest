import { readSse } from "@/lib/sse";
import { describe, expect, it } from "vitest";

describe("readSse", () => {
  it("parses streamed events with CRLF separators", async () => {
    const encoder = new TextEncoder();
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(
          encoder.encode('event: chat.delta\r\ndata: {"content":"hel'),
        );
        controller.enqueue(encoder.encode('lo"}\r\n\r\n'));
        controller.enqueue(
          encoder.encode(
            'event: chat.completed\r\ndata: {"content":"hello"}\r\n\r\n',
          ),
        );
        controller.close();
      },
    });

    const messages = [];
    for await (const message of readSse(body)) {
      messages.push(message);
    }

    expect(messages).toEqual([
      { event: "chat.delta", data: '{"content":"hello"}' },
      { event: "chat.completed", data: '{"content":"hello"}' },
    ]);
  });
});
