import { type IncomingMessage, request, type ServerResponse } from "node:http";
import type { Duplex } from "node:stream";
import { pipeline } from "node:stream";
import WebSocket from "ws";
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
    timeout: 180000,
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
  const peers = new WeakMap<ControlPeer, Map<string, ReturnType<typeof connectPeer>>>();
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
      let rpc = owners.get(origin);
      if (!rpc || rpc.signal.aborted) {
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
          peer.signal.removeEventListener("abort", close);
        });
        rpc = connectPeer(socket, (input, callbackSignal) =>
          peer.callback(input.op, input, callbackSignal),
        );
        owners.set(origin, rpc);
        await new Promise<void>((resolve, reject) => {
          socket.once("open", resolve);
          socket.once("error", () => reject(Error("browser_controller_owner_moved")));
          socket.once("close", () => reject(Error("browser_controller_owner_moved")));
        });
      }
      return rpc.call(request, { signal });
    },
    close() {
      for (const socket of sockets) socket.terminate();
    },
  };
}
