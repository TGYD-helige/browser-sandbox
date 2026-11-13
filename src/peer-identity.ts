import fs from "node:fs/promises";

/** Execution UIDs the runtime's network policy fences from internal ports. */
export const validExecutorUid = (uid) =>
  Number.isInteger(uid) && uid >= 100000 && uid < 2000000000;

/** Socket ownership comes from the kernel, never HTTP headers or a claimed Run ID. */
export async function peerUid(socket) {
  if (process.platform !== "linux") throw Error("browser_peer_identity_unavailable");
  const matches = (await fs.readFile("/proc/net/tcp", "utf8"))
    .trim()
    .split("\n")
    .slice(1)
    .flatMap((line) => {
      const fields = line.trim().split(/\s+/),
        [local, localPort] = fields[1].split(":"),
        [remote, remotePort] = fields[2].split(":");
      return local === "0100007F" &&
        remote === "0100007F" &&
        parseInt(localPort, 16) === socket.remotePort &&
        parseInt(remotePort, 16) === socket.localPort &&
        fields[3] === "01"
        ? [Number(fields[7])]
        : [];
    });
  if (matches.length !== 1 || !Number.isSafeInteger(matches[0]))
    throw Error("browser_peer_identity_unavailable");
  return matches[0];
}
