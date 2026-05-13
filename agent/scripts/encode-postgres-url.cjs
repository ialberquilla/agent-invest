#!/usr/bin/env node

if (!process.argv[2]) {
  try {
    process.loadEnvFile(".env");
  } catch {
    // .env is optional when the URL is passed as an argument.
  }
}

const input = process.argv[2] || process.env.DATABASE_URL;

if (!input) {
  console.error("Usage: node scripts/encode-postgres-url.cjs '<postgres-url>'");
  console.error(
    "Or set DATABASE_URL and run: node scripts/encode-postgres-url.cjs",
  );
  process.exit(1);
}

const schemeMatch = input.match(/^(postgres(?:ql)?:\/\/)/i);

if (!schemeMatch) {
  console.error("Expected a URL starting with postgres:// or postgresql://");
  process.exit(1);
}

const scheme = schemeMatch[1];
const rest = input.slice(scheme.length);
const atIndex = rest.lastIndexOf("@");

if (atIndex === -1) {
  console.error("Expected credentials before @ in the connection string");
  process.exit(1);
}

const credentials = rest.slice(0, atIndex);
const hostAndDatabase = rest.slice(atIndex + 1);
const colonIndex = credentials.indexOf(":");

if (colonIndex === -1) {
  console.error("Expected credentials in user:password format");
  process.exit(1);
}

const username = credentials.slice(0, colonIndex);
const password = credentials.slice(colonIndex + 1);
const encodedUsername = encodeURIComponent(decodeURIComponent(username));
const encodedPassword = encodeURIComponent(decodeURIComponent(password));

console.log(
  `${scheme}${encodedUsername}:${encodedPassword}@${hostAndDatabase}`,
);
