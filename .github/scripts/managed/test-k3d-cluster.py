"""Check cluster bootstrap against controlled external command responses."""
import os
import pathlib
import subprocess
import tempfile
import unittest

SCRIPT = pathlib.Path(__file__).with_name("k3d-cluster.sh")


class ClusterTest(unittest.TestCase):
    def test_kubeconfig_uses_actual_docker_ephemeral_port(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            commands = {
                "k3d": "if [ \"$1\" = kubeconfig ]; then echo 'server: https://127.0.0.1:0'; fi",
                "docker": "echo 127.0.0.1:38451",
                "kubectl": 'printf "%s\\n" "$@" >> "$TEST_ARGUMENTS"',
            }
            for name, body in commands.items():
                command = root / name
                command.write_text("#!/bin/sh\nset -eu\n" + body + "\n")
                command.chmod(0o755)
            arguments = root / "arguments"
            subprocess.run(["bash", str(SCRIPT), "create", "probe", str(root / "kubeconfig")],
                           env={**os.environ, "PATH": f"{root}:{os.environ['PATH']}", "TEST_ARGUMENTS": str(arguments)},
                           check=True, capture_output=True, text=True)
            self.assertIn("--server=https://127.0.0.1:38451", arguments.read_text().splitlines())


if __name__ == "__main__":
    unittest.main()
