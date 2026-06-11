import { fireEvent, render, screen } from "@testing-library/react";
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

  it("shows the live tool activity label while a tool runs with no text yet", () => {
    render(
      <MessageList
        isThinking
        liveActivity="Screening markets…"
        messages={[{ role: "agent", text: "", status: "running" }]}
      />,
    );

    expect(screen.getByText("Screening markets…")).toBeInTheDocument();
    expect(screen.queryByText("thinking...")).not.toBeInTheDocument();
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

  it("launches prompts from the progressive empty state", () => {
    const onSelectPrompt = vi.fn();
    render(
      <MessageList
        isThinking={false}
        messages={[]}
        onSelectPrompt={onSelectPrompt}
        onOpenWizard={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: /Screen markets/i }));
    fireEvent.click(screen.getByRole("button", { name: /Momentum leaders/i }));

    expect(onSelectPrompt).toHaveBeenCalledWith(
      expect.stringContaining("Screen GMX-tradeable markets"),
    );
  });

  it("keeps the guided setup entry point in the empty state", () => {
    const onOpenWizard = vi.fn();
    render(
      <MessageList
        isThinking={false}
        messages={[]}
        onSelectPrompt={vi.fn()}
        onOpenWizard={onOpenWizard}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: /Try the guided setup/i }));

    expect(onOpenWizard).toHaveBeenCalledOnce();
  });
});
