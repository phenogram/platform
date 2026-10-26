#!/usr/bin/env python3
"""Update immutable application images without replacing migrated data/configuration."""
import argparse
import copy
import hashlib
import json
import os
import re
import subprocess
from pathlib import Path

NAMESPACE = "phenogram"
WORKLOADS = [
    ("deployment", "phenogram"),
    ("statefulset", "phenogram-data-plane-gateway"),
    ("statefulset", "phenogram-telegram-standard"),
    ("statefulset", "phenogram-telegram-local"),
]
REPOSITORIES = {
    "phenogram-platform": ("APP_DIGEST", "ghcr.io/phenogram/platform"),
    "update-tap-collector": ("APP_DIGEST", "ghcr.io/phenogram/platform"),
    "gateway": ("GATEWAY_DIGEST", "ghcr.io/phenogram/data-plane-gateway"),
    "file-server": ("GATEWAY_DIGEST", "ghcr.io/phenogram/data-plane-gateway"),
    "telegram-bot-api": ("TELEGRAM_DIGEST", "ghcr.io/phenogram/telegram-bot-api"),
}


def kubectl(*args, data=None):
    result = subprocess.run(
        ["kubectl", "--request-timeout=30s", "-n", NAMESPACE, *args],
        input=None if data is None else json.dumps(data),
        text=True, capture_output=True, timeout=75,
    )
    if result.returncode:
        raise RuntimeError("Kubernetes request failed; inspect current state before continuing: " + result.stderr[-600:])
    return json.loads(result.stdout)


def protected_spec(obj):
    """Everything except image references and the release revision stays owned by operations."""
    spec = copy.deepcopy(obj["spec"])
    for container in spec["template"]["spec"]["containers"]:
        container.pop("image", None)
        for env in container.get("env", []):
            if env["name"] == "DEPLOYMENT_REVISION":
                env["value"] = "<release-revision>"
    return spec


def image_patch(obj, uid, *, preflight):
    if obj["metadata"]["uid"] != uid or obj["metadata"]["namespace"] != NAMESPACE:
        raise ValueError("Unexpected workload identity; never adopt another cluster or namespace")
    if obj["spec"].get("replicas") != 1:
        raise ValueError("Expected the existing single-instance application topology")
    patch = [
        {"op": "test", "path": "/metadata/uid", "value": uid},
        {"op": "test", "path": "/metadata/resourceVersion", "value": obj["metadata"]["resourceVersion"]},
    ]
    containers = obj["spec"]["template"]["spec"]["containers"]
    for index, container in enumerate(containers):
        key, repository = REPOSITORIES[container["name"]]
        if not container["image"].startswith(repository + "@sha256:"):
            raise ValueError("Unexpected image repository")
        digest = os.environ.get(key, "")
        if not preflight and not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
            raise ValueError("An immutable verified image digest is required")
        desired = container["image"] if preflight else repository + "@" + digest
        patch.extend([
            {"op": "test", "path": f"/spec/template/spec/containers/{index}/image", "value": container["image"]},
            {"op": "replace", "path": f"/spec/template/spec/containers/{index}/image", "value": desired},
        ])
        if not preflight:
            revision = os.environ.get("GITHUB_SHA", "")
            if not re.fullmatch(r"[0-9a-f]{40}", revision):
                raise ValueError("A complete release commit is required")
            for number, env in enumerate(container.get("env", [])):
                if env["name"] == "DEPLOYMENT_REVISION":
                    patch.append({"op": "replace", "path": f"/spec/template/spec/containers/{index}/env/{number}/value", "value": revision})
    return patch


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--preflight-only", action="store_true")
    args = parser.parse_args()
    pg = kubectl("get", "statefulset", "phenogram-postgresql", "-o", "json")
    if pg["metadata"]["namespace"] != NAMESPACE or pg.get("status", {}).get("readyReplicas") != 1:
        raise RuntimeError("The migrated PostgreSQL identity/readiness check failed")
    before = {key: kubectl("get", *key, "-o", "json") for key in WORKLOADS}
    patches = {key: image_patch(obj, obj["metadata"]["uid"], preflight=args.preflight_only) for key, obj in before.items()}
    # Admit every exact patch before making the first production change.
    for key, patch in patches.items():
        obj = kubectl("patch", *key, "--type=json", "--dry-run=server", "-p", json.dumps(patch), "-o", "json")
        if protected_spec(obj) != protected_spec(before[key]):
            raise RuntimeError("Admission changed migrated configuration, storage or placement")
    if args.preflight_only:
        print(json.dumps({"preflight": True, "workloads": 4, "postgresqlPreserved": True, "productionWrites": False}))
        return
    receipts = Path(os.environ["RUNNER_TEMP"]) / "phenogram-unified-release"
    receipts.mkdir(mode=0o700)
    for key, patch in patches.items():
        name = key[1]
        (receipts / (name + "-intent.json")).write_text(json.dumps(patch))
        print("Applying existing workload", name, "intent", hashlib.sha256(json.dumps(patch).encode()).hexdigest(), flush=True)
        obj = kubectl("patch", *key, "--type=json", "-p", json.dumps(patch), "-o", "json")
        if protected_spec(obj) != protected_spec(before[key]):
            raise RuntimeError("Unexpected configuration difference; stop and inspect, never restore old source manifests")
        result = subprocess.run(["kubectl", "--request-timeout=30s", "-n", NAMESPACE, "rollout", "status", "/".join(key), "--timeout=10m"], timeout=630)
        if result.returncode:
            raise RuntimeError("Rollout incomplete; preserve current data and inspect, no automatic rollback")
    after = kubectl("get", "statefulset", "phenogram-postgresql", "-o", "json")
    if after["metadata"]["uid"] != pg["metadata"]["uid"] or after["spec"] != pg["spec"]:
        raise RuntimeError("Protected PostgreSQL changed independently; inspect before continuing")
    print("Unified immutable application release complete; migrated database, claims and bindings retained")


if __name__ == "__main__":
    main()
