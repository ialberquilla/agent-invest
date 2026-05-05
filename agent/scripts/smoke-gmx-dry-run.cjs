const { spawnSync } = require("node:child_process");

const command = "pnpm";
const args = [
  "--filter",
  "@agent-invest/agent",
  "ingest:gmx",
  "--",
  "--dry-run",
  "--symbols",
  "BTC",
];

const result = spawnSync(command, args, {
  cwd: new URL("../..", `file://${__filename}`).pathname,
  encoding: "utf8",
  shell: false,
});

if (result.stdout) process.stdout.write(result.stdout);
if (result.stderr) process.stderr.write(result.stderr);

if (result.error) {
  process.stderr.write(
    `Failed to run GMX dry-run smoke command: ${result.error.message}\n`,
  );
  process.exit(1);
}

if (result.status !== 0) {
  process.stderr.write(
    `GMX dry-run smoke command exited with ${result.status}.\n`,
  );
  process.exit(result.status ?? 1);
}

const jsonLines = result.stdout
  .split(/\r?\n/)
  .map((line) => line.trim())
  .filter((line) => line.startsWith("{"))
  .map((line) => JSON.parse(line));

const summary = jsonLines.find((line) => line.event === "gmx_history_summary");

if (!summary) {
  process.stderr.write(
    "GMX dry-run smoke did not print a gmx_history_summary event.\n",
  );
  process.exit(1);
}

const btc = summary.symbols?.find((symbol) => symbol.symbol === "BTC");

if (summary.dryRun !== true || btc?.dryRun !== true) {
  process.stderr.write(
    "GMX dry-run smoke summary did not confirm dryRun=true.\n",
  );
  process.exit(1);
}

if (summary.failureCount !== 0 || summary.successCount !== 1 || !btc) {
  process.stderr.write(
    "GMX dry-run smoke did not complete exactly one BTC symbol successfully.\n",
  );
  process.exit(1);
}

if (btc.assetId !== "BTC") {
  process.stderr.write(
    "GMX dry-run smoke did not use the expected BTC asset id.\n",
  );
  process.exit(1);
}

if (!Number.isInteger(btc.rowCount) || btc.rowCount <= 0) {
  process.stderr.write(
    "GMX dry-run smoke did not report a positive intended row count.\n",
  );
  process.exit(1);
}

if (
  typeof btc.startTimestamp !== "string" ||
  typeof btc.endTimestamp !== "string"
) {
  process.stderr.write(
    "GMX dry-run smoke did not report a fetched timestamp range.\n",
  );
  process.exit(1);
}

process.stdout.write(
  `GMX BTC dry-run smoke passed: fetched ${btc.rowCount} rows from ${btc.startTimestamp} to ${btc.endTimestamp}; dryRun=true so Postgres writes are skipped.\n`,
);
