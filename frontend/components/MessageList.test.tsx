import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { MessageList } from "@/components/MessageList";

describe("MessageList", () => {
  beforeEach(() => {
    Element.prototype.scrollIntoView = vi.fn();
  });

  it("renders an empty streaming assistant turn as thinking, not metadata", () => {
    render(
      <MessageList
        isThinking
        messages={[{ role: "agent", text: "", status: "streaming" }]}
      />,
    );

    expect(screen.getByText("thinking...")).toBeInTheDocument();
    expect(screen.queryByText("streaming")).not.toBeInTheDocument();
  });

  it("renders streamed assistant text without showing streaming metadata", () => {
    render(
      <MessageList
        isThinking
        messages={[
          { role: "agent", text: "Partial streamed answer", status: "streaming" },
        ]}
      />,
    );

    expect(screen.getByText("Partial streamed answer")).toBeInTheDocument();
    expect(screen.queryByText("streaming")).not.toBeInTheDocument();
    expect(screen.queryByText("thinking...")).not.toBeInTheDocument();
  });
});
