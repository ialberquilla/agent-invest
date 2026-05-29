// Narrow LLM client interface for workflow steps. Steps depend on this,
// not on opencode directly, so they remain testable with a fake.

import {
  disabledOpencodeBuiltinsTools,
  getOrCreateManagedOpencode,
  parseOpencodeModel,
  resolveOpencodeModel,
  type ManagedOpencode,
} from "../session";

export type LLMRequest = {
  system: string;
  user: string;
  model?: string;
};

export type LLMResponse = {
  text: string;
  tokens_in: number | null;
  tokens_out: number | null;
};

export interface LLMClient {
  complete(request: LLMRequest): Promise<LLMResponse>;
}

export type OpencodeLLMClientOptions = {
  env?: NodeJS.ProcessEnv;
  getManagedOpencode?: typeof getOrCreateManagedOpencode;
  sessionTitle?: string;
};

// Opencode-backed LLM client. Creates a fresh session per call so steps
// are stateless and independent. All built-in tools are disabled — the
// step is a pure text-in, text-out interaction.
export function createOpencodeLLMClient(
  options: OpencodeLLMClientOptions = {},
): LLMClient {
  const env = options.env ?? process.env;
  const getManaged = options.getManagedOpencode ?? getOrCreateManagedOpencode;
  const sessionTitle = options.sessionTitle ?? "workflow-step";

  return {
    async complete(request) {
      const managed = await getManaged(env);
      const modelString = request.model ?? resolveOpencodeModel(env);
      const model = parseOpencodeModel(modelString);
      const sessionId = await createSession(managed, sessionTitle);

      const response = await managed.client.session.prompt({
        path: { id: sessionId },
        body: {
          model,
          tools: disabledOpencodeBuiltinsTools(),
          system: request.system,
          parts: [{ type: "text", text: request.user }],
        },
        throwOnError: true,
      });

      return {
        text: extractText(response.data.parts ?? []),
        tokens_in: tokenCount(response.data.info, "input"),
        tokens_out: tokenCount(response.data.info, "output"),
      };
    },
  };
}

async function createSession(managed: ManagedOpencode, title: string) {
  const session = await managed.client.session.create({
    body: { title },
    throwOnError: true,
  });
  return session.data.id as string;
}

function extractText(parts: unknown[]): string {
  return parts
    .map((part) => {
      if (!part || typeof part !== "object") return "";
      const record = part as { type?: unknown; text?: unknown };
      if (record.type !== "text") return "";
      return typeof record.text === "string" ? record.text : "";
    })
    .filter(Boolean)
    .join("\n");
}

function tokenCount(info: unknown, key: "input" | "output"): number | null {
  if (!info || typeof info !== "object") return null;
  const usage = (info as { usage?: Record<string, unknown> }).usage;
  const candidate = usage?.[`${key}Tokens`] ?? usage?.[`${key}_tokens`];
  return typeof candidate === "number" ? candidate : null;
}
