#!/usr/bin/env bash
# The suites gated on an S3 bucket, which return at once without one: the
# lease and ownership tests on an S3 API, failover with production timings,
# two nodes over the cluster listener, `cluster status` and deploy
# configuration against a bucket, and `cluster diagnose` (the bucket
# conformance check), plus files, static asset deploy/collection and the feed.
#
# With WASMKEEP_TEST_S3_BUCKET and the AWS_* variables already set, they run
# against that bucket (R2, OVHcloud, AWS: see docs/deploy). Otherwise this
# starts moto's S3 server on 127.0.0.1:5055, as the CI does: moto_server
# from PATH, or the pinned moto through uvx. It installs nothing else.
set -euo pipefail
cd "$(dirname "$0")/.."

MOTO_VERSION=5.2.5
moto_pid=
cleanup() { if [ -n "$moto_pid" ]; then kill "$moto_pid" 2>/dev/null || true; fi; }
trap cleanup EXIT

if [ -n "${WASMKEEP_TEST_S3_BUCKET:-}" ]; then
  echo "S3 suites against the configured bucket ${WASMKEEP_TEST_S3_BUCKET}"
else
  if command -v moto_server >/dev/null 2>&1; then
    moto=(moto_server)
  elif command -v uvx >/dev/null 2>&1; then
    moto=(uvx --quiet --from "moto[server]==$MOTO_VERSION" moto_server)
  else
    echo "no S3 server: install moto[server]==$MOTO_VERSION (or uv), or set WASMKEEP_TEST_S3_BUCKET and the AWS_* variables" >&2
    exit 1
  fi
  log=$(mktemp -t wasmkeep-moto.XXXXXX)
  "${moto[@]}" -p 5055 >"$log" 2>&1 &
  moto_pid=$!
  for _ in $(seq 1 60); do
    curl -sf -o /dev/null http://127.0.0.1:5055/moto-api/ && break
    if ! kill -0 "$moto_pid" 2>/dev/null; then cat "$log" >&2; exit 1; fi
    sleep 1
  done
  curl -sf -X PUT -H 'Content-Length: 0' http://127.0.0.1:5055/wasmkeep-test >/dev/null
  export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1
  export AWS_ENDPOINT=http://127.0.0.1:5055 AWS_ALLOW_HTTP=true
  export WASMKEEP_TEST_S3_BUCKET=wasmkeep-test
  echo "S3 suites against moto $MOTO_VERSION"
fi

cargo nextest run --build-jobs 4 --test-threads 4 -p wasmkeep-cluster --test s3 --locked
cargo nextest run --build-jobs 4 --test-threads 4 -p wasmkeep-cli --locked --no-capture \
  --test failover --test cluster_tls --test cluster_status --test deploy_config --test cluster_diagnose

cargo nextest run --build-jobs 4 --test-threads 4 -p wasmkeep-server --test cluster --locked -E 'test(::s3_)'

# The same drill used by the soak, with a bounded fixture writer here.
./scripts/drills/backup-restore.sh
