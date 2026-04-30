FROM python:3.12-slim

ARG NODE_VERSION=22.17.0
ARG PNPM_VERSION=10.19.0
ARG OPENCODE_VERSION=1.14.24
ARG UV_VERSION=0.7.21

ENV PNPM_HOME=/pnpm
ENV PATH=/pnpm:/usr/local/bin:${PATH}
ENV HOST=0.0.0.0
ENV PORT=8000
ENV NODE_ENV=production

WORKDIR /app

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
RUN npm install --global "opencode-ai@${OPENCODE_VERSION}"

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY agent/package.json agent/package.json

RUN pnpm install --frozen-lockfile

COPY agent agent

RUN pnpm build

ENTRYPOINT ["./agent/docker/entrypoint.sh"]
CMD ["node", "agent/dist/server.js"]
