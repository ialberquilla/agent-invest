import { defineConfig } from "tsup";

export default defineConfig({
  entry: {
    server: "src/api/server.ts",
    migrate: "src/db/migrate.ts",
  },
  format: ["esm"],
  target: "node22",
  platform: "node",
  outDir: "dist",
  clean: true,
  sourcemap: true,
  splitting: false,
  bundle: true,
  skipNodeModulesBundle: true,
});
