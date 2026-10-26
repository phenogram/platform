#!/usr/bin/env python3
"""Open the existing restricted API transport; never expose the Kubernetes API."""
import base64
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time


def cleanup(root):
    root = Path(root)
    parent = Path(os.environ["RUNNER_TEMP"]).resolve()
    if root.resolve().parent != parent or not root.name.startswith("phenogram-ci-"):
        raise ValueError("Unexpected credential directory")
    pid = root / "ssh.pid"
    if pid.exists():
        try:
            number = int(pid.read_text())
            command = Path(f"/proc/{number}/cmdline").read_bytes()
            if str(root / "key").encode() in command and b"ssh" in command:
                os.kill(number, signal.SIGTERM)
        except (ProcessLookupError, FileNotFoundError):
            pass
    shutil.rmtree(root)


def main():
    if len(sys.argv) == 2 and sys.argv[1] == "--cleanup":
        if os.environ.get("PHENOGRAM_CI_WORK"):
            cleanup(os.environ["PHENOGRAM_CI_WORK"])
        return
    config = json.loads(os.environ["PHENOGRAM_KUBECONFIG"])
    if not all(len(config[k]) == 1 for k in ("clusters", "users", "contexts")):
        raise ValueError("Exactly one restricted cluster identity is required")
    cluster = config["clusters"][0]["cluster"]
    context = config["contexts"][0]["context"]
    if cluster["server"] != "https://100.96.0.1:6443" or context["namespace"] != "phenogram":
        raise ValueError("Refusing the old cluster or another namespace")
    if cluster.get("insecure-skip-tls-verify") or not cluster.get("certificate-authority-data"):
        raise ValueError("Verified Kubernetes TLS is required")
    extension = next(x["extension"] for x in cluster.pop("extensions") if x["name"] == "infra-ssh-tunnel")
    if (extension["host"], extension["port"], extension["user"]) != ("89.35.119.225", 42242, "ci-unified-apps"):
        raise ValueError("Unexpected transport identity")
    if cluster["proxy-url"] != "socks5://127.0.0.1:1080":
        raise ValueError("The API tunnel must listen only on loopback")
    if set(config["users"][0]["user"]) != {"token"}:
        raise ValueError("Only the reviewed service-account identity is allowed")
    os.umask(0o077)
    root = Path(tempfile.mkdtemp(prefix="phenogram-ci-", dir=os.environ["RUNNER_TEMP"]))
    try:
        (root / "key").write_bytes(base64.b64decode(extension["private-key-data"], validate=True))
        (root / "known_hosts").write_text("[89.35.119.225]:42242 " + extension["host-key"] + "\n")
        (root / "config").write_text(json.dumps(config))
        log = (root / "ssh.log").open("wb")
        tunnel = subprocess.Popen([
            "ssh", "-F", "/dev/null", "-N", "-D", "127.0.0.1:1080", "-p", "42242",
            "-i", str(root / "key"), "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes",
            "-o", "StrictHostKeyChecking=yes", "-o", "UserKnownHostsFile=" + str(root / "known_hosts"),
            "-o", "ExitOnForwardFailure=yes", "-o", "ServerAliveInterval=15",
            "-o", "ServerAliveCountMax=4", "ci-unified-apps@89.35.119.225",
        ], stdout=subprocess.DEVNULL, stderr=log, start_new_session=True)
        (root / "ssh.pid").write_text(str(tunnel.pid))
        for _ in range(100):
            if tunnel.poll() is not None:
                raise RuntimeError("Restricted SSH transport failed")
            try:
                with socket.create_connection(("127.0.0.1", 1080), timeout=0.2):
                    break
            except OSError:
                time.sleep(0.1)
        else:
            raise RuntimeError("Restricted SSH transport did not become ready")
        with Path(os.environ["GITHUB_ENV"]).open("a") as output:
            output.write("PHENOGRAM_CI_WORK=" + str(root) + "\nKUBECONFIG=" + str(root / "config") + "\n")
        print("Verified-TLS API transport prepared for the phenogram namespace")
    except BaseException:
        cleanup(root)
        raise


if __name__ == "__main__":
    main()
