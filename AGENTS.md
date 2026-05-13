# CRITICAL: Package Manager Policy

Use `pnpm` for all JavaScript dependency operations. Do not use `npm` or `npx`.

Use the root scripts for Python dependency operations so uv applies the 7-day package age gate:

```sh
pnpm py:lock
pnpm py:sync
pnpm py:sync:frozen
```

Do not run raw `uv lock` or `uv sync`; those commands can resolve packages that were uploaded less than 7 days ago.
