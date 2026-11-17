import { type IncomingMessage, request, type ServerResponse } from "node:http";
import type { Duplex } from "node:stream";
import { pipeline } from "node:stream";
import WebSocket, { createWebSocketStream } from "ws";
import { type ControlPeer, connectPeer } from "./network-control.js";

/** A separate listener, rather than a caller-controlled header, is the one-hop marker. */
export function forwardControllerRequest(
  req: IncomingMessage,
  destination: ServerResponse | Duplex,
  origin: string,
  head?: Buffer,
) {
  const upstream = request(new URL(req.url, origin), {
    method: req.method,
    headers: { ...req.headers, host: new URL(origin).host },
    timeout: req.url?.endsWith("/auth") ? 620000 : 180000,
  });
  const close = () => upstream.destroy();
  destination.once("close", close);
  upstream.once("error", () => {
    if ("writeHead" in destination && !destination.headersSent)
      destination
        .writeHead(503)
        .end(JSON.stringify({ error: "browser_controller_owner_moved" }));
    else destination.destroy();
  });
  upstream.once("timeout", () => upstream.destroy());
  upstream.once("response", (response) => {
    if ("writeHead" in destination) {
      destination.writeHead(response.statusCode, response.headers);
      pipeline(response, destination, () => {});
    } else {
      destination.end("HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n");
      response.destroy();
    }
  });
  upstream.once("upgrade", (response, socket, upstreamHead) => {
    destination.write(
      `HTTP/1.1 101 Switching Protocols\r\n${Object.entries(response.headers)
        .map(([key, value]) => `${key}: ${value}`)
        .join("\r\n")}\r\n\r\n`,
    );
    if (upstreamHead.length) destination.write(upstreamHead);
    if (head?.length) socket.write(head);
    pipeline(socket, destination as Duplex, () => socket.destroy());
    pipeline(destination as Duplex, socket, () => destination.destroy());
  });
  if (head !== undefined) upstream.end();
  else pipeline(req, upstream, () => {});
}

export function createHostForwarding() {
  const peers = new WeakMap<
    ControlPeer,
    Map<string, { rpc: ReturnType<typeof connectPeer>; ready: Promise<void> }>
  >();
  const sockets = new Set<WebSocket>();
  return {
    async request(
      origin: string,
      request: any,
      peer: ControlPeer,
      signal?: AbortSignal,
    ) {
      if (!peer) throw Error("browser_host_authority_required");
      let owners = peers.get(peer);
      if (!owners) {
        owners = new Map();
        peers.set(peer, owners);
      }
      let connection = owners.get(origin);
      if (!connection || connection.rpc.signal.aborted) {
        const url = new URL("/host", origin);
        url.protocol = "ws:";
        const socket = new WebSocket(url, {
          maxPayload: 32 * 1024 * 1024,
          handshakeTimeout: 10000,
        });
        sockets.add(socket);
        const close = () => socket.terminate();
        peer.signal.addEventListener("abort", close, { once: true });
        socket.once("close", () => {
          sockets.delete(socket);
          if (owners.get(origin)?.rpc === rpc) owners.delete(origin);
          peer.signal.removeEventListener("abort", close);
        });
        const rpc = connectPeer(socket, (input, callbackSignal) =>
          peer.callback(input.op, input, callbackSignal),
        );
        const ready = new Promise<void>((resolve, reject) => {
          socket.once("open", resolve);
          socket.once("error", () => reject(Error("browser_controller_owner_moved")));
          socket.once("close", () => reject(Error("browser_controller_owner_moved")));
        });
        ready.catch(() => {});
        connection = { rpc, ready };
        owners.set(origin, connection);
      }
      await connection.ready;
      return connection.rpc.call(request, { signal });
    },
    close() {
      for (const socket of sockets) socket.terminate();
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
