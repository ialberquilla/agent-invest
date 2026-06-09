"use client";

import { FormEvent, KeyboardEvent, useState } from "react";
import type { ReactNode } from "react";
import { ArrowUp } from "lucide-react";

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
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      void submit();
    }
  }

  const canSend = !disabled && text.trim().length > 0;

  return (
    <div className="shrink-0 px-4 pb-4 pt-4">
      <form
        className="mx-auto w-full max-w-5xl"
        onSubmit={(event) => void submit(event)}
      >
        <div className="flex flex-col gap-2 rounded-3xl border border-border/70 bg-card px-3 py-2.5 shadow-sm transition-colors focus-within:border-border">
          <Textarea
            value={text}
            onChange={(event) => setText(event.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Ask the agent to build or refine a strategy..."
            disabled={disabled}
            rows={1}
            className="max-h-48 min-h-9 resize-none border-0 bg-transparent px-1.5 py-1 shadow-none focus-visible:border-0 focus-visible:ring-0 disabled:bg-transparent dark:bg-transparent dark:disabled:bg-transparent"
          />

          <div className="flex items-center justify-between gap-2">
            <div className="flex items-center gap-1">{action}</div>
            <Button
              type="submit"
              size="icon"
              className="size-8 rounded-full"
              disabled={!canSend}
              aria-label="Send"
            >
              <ArrowUp />
            </Button>
          </div>
        </div>
      </form>
    </div>
  );
}
