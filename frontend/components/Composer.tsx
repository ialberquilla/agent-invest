"use client";

import { FormEvent, KeyboardEvent, useState } from "react";
import type { ReactNode } from "react";

import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";

type ComposerProps = {
  disabled?: boolean;
  action?: ReactNode;
  onSubmit: (text: string) => void | Promise<void>;
};

export function Composer({
  disabled = false,
  action,
  onSubmit,
}: ComposerProps) {
  const [text, setText] = useState("");

  async function submit(event?: FormEvent<HTMLFormElement>) {
    event?.preventDefault();

    const nextText = text.trim();
    if (!nextText || disabled) {
      return;
    }

    setText("");
    await onSubmit(nextText);
  }

  function handleKeyDown(event: KeyboardEvent<HTMLTextAreaElement>) {
    if (event.key === "Enter" && (event.metaKey || event.ctrlKey)) {
      event.preventDefault();
      void submit();
    }
  }

  return (
    <form
      className="border-t bg-background px-4 py-4 sm:px-5"
      onSubmit={(event) => void submit(event)}
    >
      <div className="flex flex-col gap-3">
        <Textarea
          value={text}
          onChange={(event) => setText(event.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Ask the agent to build or refine a strategy..."
          disabled={disabled}
          rows={4}
        />

        <div className="flex items-center justify-between gap-3">
          <p className="text-xs text-muted-foreground">
            Cmd/Ctrl+Enter to send
          </p>
          <div className="flex items-center gap-2">
            {action}
            <Button type="submit" disabled={disabled || text.trim().length === 0}>
              Send
            </Button>
          </div>
        </div>
      </div>
    </form>
  );
}
