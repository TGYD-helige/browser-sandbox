import { type IncomingMessage, request, type ServerResponse } from "node:http";
import type { Duplex } from "node:stream";
import { pipeline } from "node:stream";
import WebSocket, { createWebSocketStream } from "ws";
import { type ControlPeer, connectControl } from "./network-control.js";

/** A separate listener, rather than a caller-controlled header, is the one-hop marker. */
export function forwardControllerRequest(
  req: IncomingMessage,
  destination: ServerResponse | Duplex,
  origin: string,
  head?: Buffer,
) {
  const target = new URL(req.url, origin);
  // URL normalization must never turn an agent execution route into trusted peer control.
  if (target.origin !== origin || target.pathname !== req.url.split("?")[0])
    throw Error("browser_access_denied");
  const auth = /^\/execution\/(?:[a-f0-9-]{36}\/)?[a-f0-9]{64}\/auth$/.test(
    target.pathname,
  );
  let responseStarted = false;
  const life = new AbortController();
  const deadline = setTimeout(() => life.abort(), auth ? 620000 : 180000);
  deadline.unref();
  const upstream = request(target, {
    method: req.method,
    headers: { ...req.headers, host: target.host },
    signal: life.signal,
  });
  const close = () => life.abort();
  const cleanup = () => {
    clearTimeout(deadline);
    destination.removeListener("close", close);
    req.removeListener("aborted", close);
  };
  destination.once("close", close);
  req.once("aborted", close);
  upstream.once("error", () => {
    cleanup();
    if (destination.destroyed) return;
    const body = JSON.stringify({ error: "browser_controller_owner_moved" });
    if ("writeHead" in destination) {
      if (destination.headersSent) destination.destroy();
      else destination.writeHead(503, { "content-type": "application/json" }).end(body);
    } else if (responseStarted) destination.destroy();
    else
      destination.end(
        `HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: ${Buffer.byteLength(body)}\r\n\r\n${body}`,
      );
  });
  const rawHeaders = (response: IncomingMessage, exclude: string[] = []) =>
    response.rawHeaders
      .flatMap((name, index, headers) =>
        index % 2 === 0 && !exclude.includes(name.toLowerCase())
          ? [`${name}: ${headers[index + 1]}`]
          : [],
      )
      .join("\r\n");
  upstream.once("response", (response) => {
    responseStarted = true;
    if ("writeHead" in destination)
      destination.writeHead(response.statusCode, response.headers);
    else
      destination.write(
        `HTTP/1.1 ${response.statusCode} ${response.statusMessage}\r\n${rawHeaders(response, ["transfer-encoding", "connection", "content-length", "trailer"])}\r\nConnection: close\r\n\r\n`,
      );
    pipeline(response, destination, cleanup);
  });
  upstream.once("upgrade", (response, socket, upstreamHead) => {
    // The request deadline covers the handshake; an established CDP session has its own lifetime.
    cleanup();
    destination.write(
      `HTTP/1.1 101 Switching Protocols\r\n${rawHeaders(response)}\r\n\r\n`,
    );
    if (upstreamHead.length) destination.write(upstreamHead);
    if (head?.length) socket.write(head);
    pipeline(socket, destination as Duplex, () => socket.destroy());
    pipeline(destination as Duplex, socket, () => destination.destroy());
  });
  if (head !== undefined) upstream.end();
  else
    pipeline(req, upstream, (error) => {
      if (error) life.abort();
    });
}

export function createHostForwarding() {
  const peers = new WeakMap<
    ControlPeer,
    Map<string, ReturnType<typeof connectControl>>
  >();
  const connections = new Set<ReturnType<typeof connectControl>>();
  return {
    async request(
      origin: string,
      request: any,
      peer: ControlPeer,
      signal?: AbortSignal,
    ) {
      if (!peer) throw Error("browser_host_authority_required");
      if (peer.signal.aborted) throw Error("browser_controller_owner_moved");
      let owners = peers.get(peer);
      if (!owners) peers.set(peer, (owners = new Map()));
      let connection = owners.get(origin);
      if (!connection || connection.signal.aborted) {
        connection = connectControl(
          origin,
          (input, signal) => peer.callback(input.op, input, signal),
          10000,
        );
        const current = connection;
        connections.add(current);
        owners.set(origin, current);
        peer.signal.addEventListener("abort", current.close, { once: true });
        current.signal.addEventListener(
          "abort",
          () => {
            connections.delete(current);
            if (owners.get(origin) === current) owners.delete(origin);
            peer.signal.removeEventListener("abort", current.close);
          },
          { once: true },
        );
      }
      try {
        return await connection.call(request, { signal });
      } catch (error) {
        if (/^browser_control_(unavailable|disconnected)$/.test(error.message))
          throw Error("browser_controller_owner_moved");
        throw error;
      }
    },
    close() {
      for (const connection of connections) connection.close();
    },
  };
}

/** Legacy viewers reveal their routing identity in the first authenticated frame. */
export function forwardControllerViewer(
  client: WebSocket,
  origin: string,
  executionId: string,
  token: string,
) {
  const upstream = new WebSocket(
    `${origin.replace(/^http/, "ws")}/execution/${executionId}/view/socket`,
    { maxPayload: 32 * 1024 * 1024, handshakeTimeout: 10000 },
  );
  const downstream = createWebSocketStream(client, {
    readableObjectMode: true,
    decodeStrings: false,
    highWaterMark: 1,
  });
  const incoming = createWebSocketStream(upstream, {
    readableObjectMode: true,
    decodeStrings: false,
    highWaterMark: 1,
  });
  const close = () => {
    downstream.destroy();
    incoming.destroy();
    client.terminate();
    upstream.terminate();
  };
  client.once("error", close);
  client.once("close", close);
  upstream.once("error", close);
  upstream.once("close", close);
  upstream.once("open", () => {
    upstream.send(JSON.stringify({ type: "authenticate", token }));
    pipeline(downstream, incoming, close);
    pipeline(incoming, downstream, close);
    client.resume();
  });
}
