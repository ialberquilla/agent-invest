import { PrivyClient } from "@privy-io/server-auth";

type UserIdentity = {
  userId: string;
  authenticated: boolean;
};

let privyClient: PrivyClient | null = null;

function getPrivyClient() {
  const appId = process.env.PRIVY_APP_ID ?? process.env.NEXT_PUBLIC_PRIVY_APP_ID;
  const appSecret = process.env.PRIVY_APP_SECRET;
  if (!appId || !appSecret) return null;
  privyClient ??= new PrivyClient(appId, appSecret);
  return privyClient;
}

function bearerToken(request: Request) {
  const authorization = request.headers.get("authorization");
  if (!authorization?.startsWith("Bearer ")) return null;
  return authorization.slice("Bearer ".length).trim() || null;
}

function anonUserId(value: unknown) {
  return typeof value === "string" && value.startsWith("anon:") ? value : null;
}

export async function resolveUserIdentity(
  request: Request,
  body: { user_id?: unknown },
): Promise<UserIdentity> {
  const token = bearerToken(request);
  const client = getPrivyClient();

  if (token && client) {
    try {
      const claims = await client.verifyAuthToken(token);
      return { userId: `privy:${claims.userId}`, authenticated: true };
    } catch {
      // Invalid tokens fall back to the anonymous browser identity so public
      // chat flows keep working if a stale client token is present.
    }
  }

  return { userId: anonUserId(body.user_id) ?? "anon:unknown", authenticated: false };
}
