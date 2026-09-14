#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pi-safe-agents-package-XXXXXX")"
trap 'rm -rf "${TMP_DIR}"' EXIT

cd "${ROOT_DIR}"
npm pack --pack-destination "${TMP_DIR}" >/dev/null
TARBALL="$(find "${TMP_DIR}" -maxdepth 1 -name '*.tgz' -print -quit)"
if [[ -z "${TARBALL}" ]]; then
  echo "npm pack did not produce a tarball" >&2
  exit 1
fi

# Check the package surface explicitly: advertised benchmark and smoke scripts
# must be present in the published archive rather than only in the repository.
# Consume the complete tar listing before filtering: grep -q can exit early and
# turn tar's expected SIGPIPE into a false failure under bash pipefail.
tar -tzf "${TARBALL}" | grep -F -x 'package/bench/r10-production-soak.ts' >/dev/null
tar -tzf "${TARBALL}" | grep -F -x 'package/scripts/smoke-isolated-pi.sh' >/dev/null

INSTALL_DIR="${TMP_DIR}/install"
mkdir -p "${INSTALL_DIR}"
npm install --prefix "${INSTALL_DIR}" --ignore-scripts --no-audit --no-fund "${TARBALL}" >/dev/null

cd "${INSTALL_DIR}"
node --input-type=module <<'NODE'
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { BrokerClient, BrokerServer } from "pi-safe-agents-team";

const directory = await mkdtemp(join(tmpdir(), "pi-safe-agents-tarball-smoke-"));
const server = new BrokerServer({ directory, rootId: "tarball-smoke", rootAgentId: "root" });
const client = new BrokerClient({ endpoint: server.endpoint, agentId: "root" });
try {
  await server.start();
  await client.connect();
  const registration = await client.request("agent.register", {
    rootId: "tarball-smoke",
    route: { provider: "smoke", model: "small", thinking: "off" },
  });
  client.setIdentity("root", registration.token);
  const status = await client.request("fabric.status");
  if (status.rootId !== "tarball-smoke") throw new Error("installed broker did not return the expected fabric");
} finally {
  client.close();
  if (server.isStarted()) await server.stop();
  await rm(directory, { recursive: true, force: true });
}
NODE

echo "Package tarball smoke passed: ${TARBALL}"
