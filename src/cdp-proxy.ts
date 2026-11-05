export function attachCdpProxy({
  server,
  WebSocket,
  WebSocketServer,
  executions,
  resolvePeerUid,
  authorize = undefined,
}) {
  const ws = new WebSocketServer({
    noServer: true,
    maxPayload: 32 * 1024 * 1024,
  });
  server.on("upgrade", async (req, socket, head) => {
    const match = /^\/execution\/([a-f0-9]{64})$/.exec(req.url ?? "");
    if (authorize && !match) return;
    const authorization = authorize ? await authorize(req).catch(() => null) : null;
    const entry = authorize
      ? authorization?.entry
      : match && executions.resolve(match[1]);
    let authorized = true;
    try {
      if (!authorize && entry?.uid >= 100000)
        authorized = (await resolvePeerUid(req.socket)) === entry.uid;
    } catch {
      authorized = false;
    }
    if (
      !entry ||
      !authorized ||
      (authorize && (!authorization?.signal || authorization.signal.aborted)) ||
      entry.revoked ||
      executions.closing ||
      entry.peers.size >= 16 ||
      (entry.takeover && entry.takeover.state !== "automation")
    ) {
      socket.end("HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n");
      return;
    }
    ws.handleUpgrade(req, socket, head, (client) => {
      entry.browserRequested = true;
      const pair: any = { client, upstream: null, inflight: new Set() };
      entry.peers.add(pair);
      const isCurrentConnection = () =>
        !entry.revoked &&
        !authorization?.signal.aborted &&
        !entry.abort?.signal.aborted &&
        entry.starting === pair.starting;
      let pending = [],
        pendingBytes = 0;
      clearTimeout(entry.idle);
      entry.idle = null;
      const cleanup = (pair.cleanup = () => {
        if (!entry.peers.delete(pair)) return;
        client.terminate();
        pair.upstream?.terminate();
        pending = [];
        pendingBytes = 0;
        executions.disconnected(entry);
      });
      client.on("error", cleanup);
      client.on("close", cleanup);
      client.on("message", (data, binary) => {
        if (!isCurrentConnection()) return;
        if (entry.takeover && entry.takeover.state !== "automation") return;
        try {
          const message = JSON.parse(String(data));
          if (message.id !== undefined && message.method) pair.inflight.add(message.id);
        } catch {}
        if (pair.inflight.size > 4096) return cleanup();
        if (entry.revoked) return cleanup();
        if ((pair.upstream?.bufferedAmount ?? 0) + data.length > 32 * 1024 * 1024)
          return cleanup();
        if (pair.upstream?.readyState === WebSocket.OPEN)
          pair.upstream.send(data, { binary });
        else {
          pendingBytes += data.length;
          if (pendingBytes > 32 * 1024 * 1024) return cleanup();
          pending.push([data, binary]);
        }
      });
      executions.ensureStarted(entry);
      const starting = (pair.starting = entry.starting);
      starting
        .then((browser) => {
          if (
            !isCurrentConnection() ||
            client.readyState !== WebSocket.OPEN ||
            !entry.peers.has(pair)
          )
            return cleanup();
          const upstream = (pair.upstream = new WebSocket(browser.endpoint, {
            maxPayload: 32 * 1024 * 1024,
          }));
          const lost = () => {
            if (!entry.peers.has(pair)) return; // Normal client disconnect already cleaned up.
            executions.retire(entry, starting);
            cleanup(); // Surface the failure; a later connection starts fresh. No automatic replay.
          };
          upstream.on("error", lost);
          upstream.on("close", lost);
          upstream.on("open", () => {
            if (!isCurrentConnection() || client.readyState !== WebSocket.OPEN)
              return cleanup();
            for (const [data, binary] of pending) upstream.send(data, { binary });
            pending = [];
            pendingBytes = 0;
          });
          upstream.on("message", (data, binary) => {
            try {
              pair.inflight.delete(JSON.parse(String(data)).id);
            } catch {}
            if (client.bufferedAmount + data.length > 32 * 1024 * 1024)
              return cleanup();
            if (isCurrentConnection() && client.readyState === WebSocket.OPEN)
              client.send(data, { binary });
          });
        })
        .catch((error) => {
          const code =
            error instanceof Error && /^browser_profile_[a-z_]+$/.test(error.message)
              ? error.message
              : null;
          if (code && client.readyState === WebSocket.OPEN) client.close(1011, code);
          else cleanup();
        });
    });
  });
  return ws;
}
