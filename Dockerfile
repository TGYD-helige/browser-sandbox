# Upstream content is pinned; users must verify binary distribution rights.
ARG CLOAK_IMAGE=cloakhq/cloakbrowser@sha256:1cd05418c165e4bf8351a56c8a3403d736f7540a84ae365f27504453a1e05221
ARG BROWSERLESS_IMAGE=ghcr.io/browserless/chromium@sha256:6bac628b3d8293f8fb251d3a6b63030dfc5a1cb0c7e5c39abe5efd8f4b25360b
FROM ${BROWSERLESS_IMAGE} AS build
USER root
WORKDIR /build
RUN npm install --global pnpm@10.28.2
COPY package.json pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile
COPY tsconfig.json ./
COPY src ./src
COPY skills/browser-sandbox-auth/scripts/auth.mjs ./skills/browser-sandbox-auth/scripts/auth.mjs
COPY scripts/copy-assets.mjs ./scripts/copy-assets.mjs
RUN pnpm build && pnpm prune --prod

FROM ${CLOAK_IMAGE} AS cloak
COPY scripts/extract-cloak.py /tmp/extract-cloak.py
RUN python /tmp/extract-cloak.py && chmod -R go-w /cloak

FROM ${BROWSERLESS_IMAGE} AS browserless-runtime
USER root
RUN rm -rf "${PLAYWRIGHT_BROWSERS_PATH:?}"/chromium-* \
    "${PLAYWRIGHT_BROWSERS_PATH:?}"/chromium_headless_shell-*

# Repack the pinned upstream filesystem so removed browsers leave no retained layers.
FROM scratch
COPY --from=browserless-runtime / /
ENV PATH=/usr/src/.nvm/versions/node/v24.21.0/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    NODE_VERSION=v24.21.0 NVM_DIR=/usr/src/.nvm \
    NODE_PATH=/usr/src/.nvm/versions/node/v24.21.0/bin APP_DIR=/usr/src/app \
    DEBIAN_FRONTEND=noninteractive LANG=C.UTF-8 DEBUG_COLORS=true \
    PUPPETEER_SKIP_DOWNLOAD=true PLAYWRIGHT_BROWSERS_PATH=/usr/local/bin/playwright-browsers \
    NODE_ENV=production
USER root
WORKDIR /opt/browser-sandbox
# Ubuntu's existing Chrome AppArmor profile attaches to this root-owned path.
COPY --from=cloak --chown=0:0 /cloak /opt/google/chrome
COPY --from=build /build/dist ./dist
COPY --from=build /build/node_modules ./node_modules
COPY package.json ./
COPY examples ./examples
COPY .github/scripts/benchmark-*.mjs .github/scripts/image-smoke.mjs .github/scripts/image-layout.mjs ./.github/scripts/
COPY config/seccomp-source.json ./seccomp-source.json
RUN chmod -R go-w /opt/browser-sandbox \
  && chmod 0755 /opt/browser-sandbox/dist/chrome.js \
  && cd "${APP_DIR}" \
  && executable="$(node -p "require('playwright-core').chromium.executablePath()")" \
  && mkdir -p "$(dirname "$executable")" \
  && ln -sf /opt/browser-sandbox/dist/chrome.js "$executable" \
  && node /opt/browser-sandbox/dist/prepare-seccomp.js /opt/browser-sandbox/seccomp-source.json /opt/browser-sandbox/seccomp.json \
  && command -v flock \
  && node -e "if(typeof process.execve!=='function')throw Error('Node execve required'); const p=require(process.env.APP_DIR+'/package.json'); if(p.version!=='2.57.0')throw Error('Browserless version mismatch'); const tar=require('tar-stream'); if(typeof tar.pack!=='function'||typeof tar.extract!=='function')throw Error('Profile archive dependency missing')"
RUN apt-get update && apt-get install -y --no-install-recommends fonts-noto-cjk fonts-liberation fonts-noto-color-emoji util-linux iptables && fc-cache -f && rm -rf /var/lib/apt/lists/*
WORKDIR ${APP_DIR}
ENV HOST=127.0.0.1 PORT=3001 CONCURRENT=2 QUEUED=4 TIMEOUT=1800000 TZ=UTC \
    DEBUG=quiet ENABLE_DEBUGGER=false BROWSER_CONTROL_SOCKET=/run/browser-sandbox/control.sock
EXPOSE 3000 9222
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 \
  CMD node /opt/browser-sandbox/dist/healthcheck.js
ENTRYPOINT ["node", "/opt/browser-sandbox/dist/main.js"]
