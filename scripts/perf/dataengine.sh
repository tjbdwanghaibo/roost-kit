#!/usr/bin/env bash
set -euo pipefail

readonly repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly go_cache="${ROOST_GO_CACHE:-$(cd /tmp && pwd -P)/roost-go-cache}"
readonly output_dir="${ROOST_PERF_OUTPUT:-${repo_dir}/artifacts/perf}"

mkdir -p "${output_dir}"
cd "${repo_dir}"

GOCACHE="${go_cache}" go test ./nestwal ./dataengine ./saga \
  -run '^$' -bench . -benchmem -count=5 \
  | tee "${output_dir}/dataengine-local.txt"

cat <<'EOF'
Local benchmark complete. This output covers codec, WAL, projector adapter,
Mongo adapter, and Saga reservation overhead only. Production release gates
must also run the replica-set + JetStream file-storage profile, including
primary/leader failover and a 100k-record recovery backlog.
EOF
