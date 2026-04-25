import Fastify, { type FastifyReply, type FastifyRequest } from "fastify";
import pino from "pino";
import { fileURLToPath } from "node:url";

const NOT_IMPLEMENTED_RESPONSE = {
  error: {
    code: "not_implemented",
    message: "Not implemented",
  },
} as const;

function getPort() {
  const rawPort = process.env.PORT ?? "3000";
  const port = Number.parseInt(rawPort, 10);

  if (!Number.isInteger(port) || port <= 0) {
    throw new Error(`Invalid PORT value: ${rawPort}`);
  }

  return port;
}

async function notImplemented(_request: FastifyRequest, reply: FastifyReply) {
  return reply.code(501).send(NOT_IMPLEMENTED_RESPONSE);
}

export function buildServer() {
  const app = Fastify({
    loggerInstance: pino(),
  });

  app.get("/health", async () => ({ ok: true }));

  app.post("/users/:user_id/strategies", notImplemented);
  app.get("/users/:user_id/strategies", notImplemented);
  app.get("/users/:user_id/strategies/:strategy_id", notImplemented);
  app.post("/users/:user_id/strategies/:strategy_id/messages", notImplemented);
  app.get(
    "/users/:user_id/strategies/:strategy_id/runs/:run_id",
    notImplemented,
  );
  app.get(
    "/users/:user_id/strategies/:strategy_id/runs/:run_id/events",
    notImplemented,
  );

  return app;
}

export async function startServer() {
  const app = buildServer();

  try {
    await app.listen({
      host: process.env.HOST ?? "0.0.0.0",
      port: getPort(),
    });
  } catch (error) {
    app.log.error(error);
    process.exitCode = 1;
  }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  void startServer();
}
