import { execFile } from "node:child_process";
import { promisify } from "node:util";

const exec = promisify(execFile);

// execFile errors include argv, which can contain the execution's access reference.
export async function docker(args, execute = exec) {
  try {
    return (
      await execute("docker", args, { timeout: 180000, maxBuffer: 4 * 1024 * 1024 })
    ).stdout.trim();
  } catch (error) {
    const diagnostics = String(error.stderr ?? "")
      .replace(/(?:https?|wss?):\/\/[^\s"'<>]+/g, "[endpoint]")
      .replace(/[a-f0-9]{64}/gi, "[reference]");
    throw Error(
      `managed_docker_${args[0]}_failed${diagnostics ? `\n${diagnostics}` : ""}`,
    );
  }
}

export function diagnostics(logs, state = {}) {
  return {
    running: state.Running,
    oomKilled: state.OOMKilled,
    exitCode: state.ExitCode,
    codes: [
      ...new Set(
        String(logs).match(
          /\bbrowser_[a-z_]+\b|\b(?:EPERM|ENOENT|EACCES|ENOSPC|SIGSEGV|SIGKILL|FATAL|OOM)\b/g,
        ) ?? [],
      ),
    ],
  };
}
