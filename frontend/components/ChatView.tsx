"use client";

import { useEffect, useState } from "react";

import { Composer } from "@/components/Composer";
import { IdentityBar } from "@/components/IdentityBar";
import { MessageList } from "@/components/MessageList";
import { Card } from "@/components/ui/card";
import {
  ChatMessage,
  getMessages,
  setMessages as persistMessages,
  setStrategyId as persistStrategyId,
} from "@/lib/local-store";
import { Run, StrategyCreateResponse } from "@/lib/types";

type ChatViewProps = {
  initialStrategyId: string;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

async function readJson(response: Response) {
  try {
    return (await response.json()) as unknown;
  } catch {
    return null;
  }
}

function getErrorMessage(payload: unknown) {
  if (
    isRecord(payload) &&
    typeof payload.message === "string" &&
    payload.message.trim()
  ) {
    return payload.message.trim();
  }

  return "Request failed";
}

async function createStrategy() {
  const response = await fetch("/api/strategies", {
    method: "POST",
    cache: "no-store",
  });
  const payload = await readJson(response);

  if (!response.ok) {
    throw new Error(getErrorMessage(payload));
  }

  if (!isRecord(payload) || typeof payload.strategy_id !== "string") {
    throw new Error("Strategy creation returned an invalid response");
  }

  return payload as StrategyCreateResponse;
}

export function ChatView({ initialStrategyId }: ChatViewProps) {
  const [strategyId, setStrategyId] = useState(initialStrategyId);
  const [messages, setMessages] = useState<ChatMessage[]>(() =>
    getMessages(initialStrategyId),
  );
  const [isSending, setIsSending] = useState(false);
  const [strategyError, setStrategyError] = useState<string | null>(null);

  useEffect(() => {
    persistStrategyId(strategyId);
    persistMessages(strategyId, messages);
  }, [messages, strategyId]);

  async function handleSend(text: string) {
    if (isSending) {
      return;
    }

    setStrategyError(null);
    setMessages((current) => [...current, { role: "user", text }]);
    setIsSending(true);

    try {
      const response = await fetch("/api/messages", {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        cache: "no-store",
        body: JSON.stringify({ strategy_id: strategyId, text }),
      });

      const payload = await readJson(response);

      if (!response.ok) {
        setMessages((current) => [
          ...current,
          {
            role: "agent",
            text: "",
            status: String(response.status),
            error: getErrorMessage(payload),
          },
        ]);
        return;
      }

      const run = payload as Run;

      setMessages((current) => [
        ...current,
        {
          role: "agent",
          text: run.reply ?? "",
          run_id: run.run_id,
          status: run.status,
          error: run.error ?? undefined,
        },
      ]);
    } catch {
      setMessages((current) => [
        ...current,
        {
          role: "agent",
          text: "",
          status: "error",
          error: "Unable to reach the chat service",
        },
      ]);
    } finally {
      setIsSending(false);
    }
  }

  async function handleNewStrategy() {
    if (isSending) {
      return;
    }

    setStrategyError(null);

    try {
      const next = await createStrategy();

      persistMessages(strategyId, []);
      persistStrategyId(next.strategy_id);
      setStrategyId(next.strategy_id);
      setMessages([]);
    } catch (error) {
      setStrategyError(
        error instanceof Error
          ? error.message
          : "Unable to create a new strategy",
      );
    }
  }

  return (
    <Card className="flex h-[calc(100vh-2rem)] w-full flex-col overflow-hidden border-border/70 bg-background shadow-sm sm:h-[calc(100vh-3rem)]">
      <IdentityBar
        strategyId={strategyId}
        disabled={isSending}
        onNewStrategy={handleNewStrategy}
      />

      {strategyError ? (
        <div className="border-b bg-destructive/10 px-4 py-3 text-sm text-destructive sm:px-5">
          {strategyError}
        </div>
      ) : null}

      <div className="min-h-0 flex-1 bg-muted/10">
        <MessageList messages={messages} isThinking={isSending} />
      </div>

      <Composer disabled={isSending} onSubmit={handleSend} />
    </Card>
  );
}
