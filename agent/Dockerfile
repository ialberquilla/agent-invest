# syntax=docker/dockerfile:1

# Build from the repository root:
# docker build -f agent/Dockerfile .

FROM python:3.12-slim AS base

ARG NODE_VERSION=22.17.0
ARG PNPM_VERSION=10.19.0
ARG OPENCODE_VERSION=1.14.25
ARG UV_VERSION=0.7.21

ENV PNPM_HOME=/pnpm
ENV PATH=/pnpm:/usr/local/bin:${PATH}

RUN apt-get update \
  && apt-get install --yes --no-install-recommends ca-certificates curl git xz-utils \
  && rm -rf /var/lib/apt/lists/*

RUN arch="$(dpkg --print-architecture)" \
  && case "${arch}" in \
    amd64) node_arch="x64" ;; \
    arm64) node_arch="arm64" ;; \
    *) echo "Unsupported architecture: ${arch}" >&2; exit 1 ;; \
  esac \
  && curl --fail --silent --show-error --location \
    "https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-${node_arch}.tar.xz" \
    --output /tmp/node.tar.xz \
  && tar -xJf /tmp/node.tar.xz -C /usr/local --strip-components=1 --no-same-owner \
  && rm /tmp/node.tar.xz \
  && corepack enable \
  && corepack prepare "pnpm@${PNPM_VERSION}" --activate

RUN pip install --no-cache-dir "uv==${UV_VERSION}"
RUN pnpm add --global "opencode-ai@${OPENCODE_VERSION}"

FROM base AS deps
WORKDIR /app
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY agent/package.json ./agent/package.json
RUN pnpm install --frozen-lockfile --filter @agent-invest/agent

FROM deps AS builder
WORKDIR /app
COPY agent ./agent
RUN pnpm --filter @agent-invest/agent build

FROM base AS python-deps
WORKDIR /app/agent/scripts
COPY agent/scripts/pyproject.toml agent/scripts/uv.lock agent/scripts/uv-age-policy.sh ./
COPY agent/scripts/agent_invest_scripts ./agent_invest_scripts
RUN chmod +x ./uv-age-policy.sh \
  && ./uv-age-policy.sh sync --frozen

FROM base AS runner
WORKDIR /app

ENV NODE_ENV=production
ENV HOST=0.0.0.0
ENV PORT=8000
ENV OPENCODE_MODEL=azure/gpt-5.4
ENV OPENCODE_AUTH_PROVIDER=azure
ENV STORAGE_ROOT=/tmp/agent-invest-storage
ENV AGENT_API_KEY=""
ENV UV_CACHE_DIR=/tmp/agent-invest-storage/uv
ENV XDG_CACHE_HOME=/tmp/agent-invest-storage/xdg-cache
ENV OPENCODE_DIRECTORY=/app

COPY --from=deps /app/node_modules ./node_modules
COPY --from=deps /app/agent/node_modules ./agent/node_modules
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY --from=builder /app/agent/package.json ./agent/package.json
COPY --from=builder /app/agent/dist ./agent/dist
COPY --from=builder /app/agent/src ./agent/src
COPY --from=builder /app/agent/migrations ./agent/migrations
COPY --from=builder /app/agent/drizzle.config.ts ./agent/drizzle.config.ts
COPY --from=builder /app/agent/docker/entrypoint.sh ./agent/docker/entrypoint.sh
COPY --from=python-deps /app/agent/scripts ./agent/scripts

RUN test -d ./agent/scripts/agent_invest_scripts
RUN chmod +x ./agent/docker/entrypoint.sh

EXPOSE 8000

ENTRYPOINT ["./agent/docker/entrypoint.sh"]
CMD ["node", "agent/dist/server.js"]
