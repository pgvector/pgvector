#!/bin/sh
# Compare filtered exact search scored in the backend with the pipelined
# worker route. Usage:
#   ANN_PG_DSN='host=... dbname=...' benchmarks/ann/pipeline.sh [rows] [dimensions]
# The worker runs locally, so PostgreSQL must be on this host. Requires a
# superuser (ann.worker_socket is superuser-only), psql, and pgbench on PATH.
set -eu

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
ROWS=${1:-100000}
DIM=${2:-256}
CLIENTS=${ANN_BENCH_CLIENTS:-"1 4 8 16"}
SECONDS_PER_RUN=${ANN_BENCH_SECONDS:-10}
BACKEND=${ANN_BENCH_BACKEND:-mock-gpu}
: "${ANN_PG_DSN:?set ANN_PG_DSN}"

WORK=$(mktemp -d)
SOCKET=$WORK/ann.sock
trap 'kill $WORKER 2>/dev/null || true; rm -rf "$WORK"' EXIT INT TERM

make -s -C "$ROOT/ann" ann-worker
"$ROOT/ann/ann-worker" --backend "$BACKEND" --listen "$SOCKET" 2>"$WORK/worker.log" &
WORKER=$!
while [ ! -S "$SOCKET" ]; do sleep 0.1; done

psql "$ANN_PG_DSN" -X -q -v ON_ERROR_STOP=1 <<EOF
CREATE EXTENSION IF NOT EXISTS vector;
DROP TABLE IF EXISTS ann_pipeline_bench, ann_pipeline_queries;
CREATE TABLE ann_pipeline_bench (id int, tenant int, emb vector($DIM));
INSERT INTO ann_pipeline_bench SELECT i, i % 4,
    (SELECT array_agg(random())::real[] FROM generate_series(1, $DIM) WHERE i > 0)::vector
    FROM generate_series(1, $ROWS) i;
CREATE INDEX ON ann_pipeline_bench USING ann (emb vector_l2_ops);
CREATE INDEX ON ann_pipeline_bench (tenant);
CREATE TABLE ann_pipeline_queries AS SELECT id, emb FROM ann_pipeline_bench WHERE id <= 200;
ANALYZE ann_pipeline_bench, ann_pipeline_queries;
EOF

cat > "$WORK/query.sql" <<'EOF'
\set q random(1, 200)
SELECT emb::text AS v FROM ann_pipeline_queries WHERE id = :q \gset
SELECT id FROM ann_pipeline_bench WHERE tenant = 1 ORDER BY emb <-> :v::vector, id LIMIT 10;
EOF

BASE="-c ann.enable_filtered_search=on -c max_parallel_workers_per_gather=0 -c session_preload_libraries=vector"
for route in local worker; do
    OPTIONS=$BASE
    [ "$route" = worker ] && OPTIONS="$BASE -c ann.worker_socket=$SOCKET"
    for clients in $CLIENTS; do
        result=$(PGOPTIONS="$OPTIONS" pgbench "$ANN_PG_DSN" -n -M prepared -c "$clients" \
            -j "$clients" -T "$SECONDS_PER_RUN" -f "$WORK/query.sql" 2>&1)
        printf '%-6s clients=%-3s %s  %s\n' "$route" "$clients" \
            "$(echo "$result" | grep '^tps' | sed 's/ (without.*//')" \
            "$(echo "$result" | grep 'latency average')"
    done
done
psql "$ANN_PG_DSN" -X -q -c 'DROP TABLE ann_pipeline_bench, ann_pipeline_queries'
