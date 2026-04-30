"use client";

import { useEffect, useEffectEvent, useState } from "react";

import { Composer } from "@/components/Composer";
import { IdentityBar } from "@/components/IdentityBar";
import { MessageList } from "@/components/MessageList";
import { Card } from "@/components/ui/card";
import {
  ChatMessage,
  deriveStrategyLabel,
  ensureKnownStrategy,
  getMessages,
  setMessages as persistMessages,
  setStrategyId as persistStrategyId,
  upsertKnownStrategy,
} from "@/lib/local-store";
import { Run } from "@/lib/types";

type ChatViewProps = {
  strategyId: string;
  disabled?: boolean;
  strategyError?: string | null;
  onBusyChange?: (isBusy: boolean) => void;
  onKnownStrategiesChange?: () => void;
  onNewStrategy: () => void | Promise<void>;
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

export function ChatView({
  strategyId,
  disabled = false,
  strategyError = null,
  onBusyChange,
  onKnownStrategiesChange,
  onNewStrategy,
}: ChatViewProps) {
  const [messages, setMessages] = useState<ChatMessage[]>(() =>
    getMessages(strategyId),
  );
  const [isSending, setIsSending] = useState(false);

  const reportBusyChange = useEffectEvent((isBusy: boolean) => {
    onBusyChange?.(isBusy);
  });

  const notifyKnownStrategiesChange = useEffectEvent(() => {
    onKnownStrategiesChange?.();
  });

  useEffect(() => {
    reportBusyChange(isSending);
  }, [isSending]);

  useEffect(() => {
    persistStrategyId(strategyId);
    persistMessages(strategyId, messages);

    const firstUserMessage = messages.find(
      (message) => message.role === "user",
    );
    if (firstUserMessage) {
      upsertKnownStrategy({
        strategy_id: strategyId,
        label: deriveStrategyLabel(firstUserMessage.text),
      });
    } else {
      ensureKnownStrategy(strategyId);
    }

    notifyKnownStrategiesChange();
  }, [messages, strategyId]);

  async function handleSend(text: string) {
    if (disabled || isSending) {
      return;
    }

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

  const isDisabled = disabled || isSending;

  return (
    <Card className="flex h-[calc(100vh-22rem)] min-h-[30rem] w-full flex-col overflow-hidden border-border/70 bg-background shadow-sm lg:h-[calc(100vh-3rem)]">
      <IdentityBar
        strategyId={strategyId}
        disabled={isDisabled}
        onNewStrategy={onNewStrategy}
      />

      {strategyError ? (
        <div className="border-b bg-destructive/10 px-4 py-3 text-sm text-destructive sm:px-5">
          {strategyError}
        </div>
      ) : null}

      <div className="min-h-0 flex-1 bg-muted/10">
        <MessageList messages={messages} isThinking={isSending} />
      </div>

      <Composer disabled={isDisabled} onSubmit={handleSend} />
    </Card>
  );
}
