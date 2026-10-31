#!/bin/bash
# Unattended build, test, and benchmark run for the ann work on a GPU node,
# so a development session can disconnect and pick the results up later.
#
#   setsid nohup scripts/ann/gpu_handoff.sh >/dev/null 2>&1 < /dev/null &
#   tail -f "$ANN_HANDOFF_DIR/run.log"; cat "$ANN_HANDOFF_DIR/REPORT.md" when done
#   kill -- -$(cat "$ANN_HANDOFF_DIR/run.pid")     # stop early
#
# Steps:
# 1. Fresh clone of ANN_HANDOFF_REPO's ANN_HANDOFF_BRANCH in ANN_HANDOFF_SRC,
#    never a working tree someone is editing.
# 2. Build the cuVS + RDMA worker and the RDMA-enabled extension, and install
#    the extension with ANN_HANDOFF_PG's pg_config.
# 3. Run every test alone, in its own process group, with a hard time limit:
#    a hang is recorded as TIMEOUT and its leftover workers are killed. Tests
#    run pinned to ANN_HANDOFF_TEST_CORES and use no GPUs.
# 4. Optionally wait until nothing listens on ANN_HANDOFF_WAIT_PORTS (another
#    workload still using the GPUs), then run the cuVS GPU suites and
#    benchmark the same load over TCP, RDMA with spin polling, and RDMA with
#    interrupts (benchmarks/ann/cuvs_load.py; needs its generated table and a
#    built index at ANN_HANDOFF_INDEX).
# 5. Write $ANN_HANDOFF_DIR/REPORT.md: per-step results, logs, benchmark
#    output, and what is left.
#
# Requirements on the node: the cuVS SDK environment in ANN_HANDOFF_CUVS_ENV
# (exports CUVS_PREFIX, CUDA_HOME, CPPFLAGS, LDFLAGS), rdma-core, a PostgreSQL
# install whose cluster (ANN_HANDOFF_PGDATA, at ANN_HANDOFF_PGHOST and
# ANN_HANDOFF_PGPORT) the current user can use as a superuser, and Python
# with psycopg, numpy, cupy, and cuvs (see docs/ann-cuvs-index.md).
set -u

: "${ANN_HANDOFF_DIR:=$HOME/annenv/handoff}"
: "${ANN_HANDOFF_SRC:=$HOME/annenv/pgvector-handoff}"
: "${ANN_HANDOFF_REPO:=https://github.com/chenqin/pgvector}"
: "${ANN_HANDOFF_BRANCH:=master}"
: "${ANN_HANDOFF_PG:=$HOME/annenv/pg}"
: "${ANN_HANDOFF_PGDATA:=$HOME/annenv/pgdata}"
: "${ANN_HANDOFF_CUVS_ENV:=$HOME/annenv/cuvs-env.sh}"
: "${ANN_HANDOFF_PYTHON_BIN:=$HOME/annenv/bin}"
: "${ANN_HANDOFF_PGHOST:=/tmp}"
: "${ANN_HANDOFF_PGPORT:=54329}"
: "${ANN_HANDOFF_TEST_CORES:=84-99}"
: "${ANN_HANDOFF_WORKER_CORES:=0-95}"
: "${ANN_HANDOFF_CLIENT_CORES:=96-223}"
: "${ANN_HANDOFF_RDMA_DEVICE:=mlx5_0}"
: "${ANN_HANDOFF_WAIT_PORTS:=}"
: "${ANN_HANDOFF_WAIT_HOURS:=12}"
: "${ANN_HANDOFF_BENCH_PORT:=7820}"
: "${ANN_HANDOFF_INDEX:=$HOME/annenv/idx/bench10m-float32}"

H=$ANN_HANDOFF_DIR
R=$H/results
SRC=$ANN_HANDOFF_SRC
PGBIN=$ANN_HANDOFF_PG/bin
DSN_ADMIN="host=$ANN_HANDOFF_PGHOST port=$ANN_HANDOFF_PGPORT dbname=postgres"
DSN_TEST="host=$ANN_HANDOFF_PGHOST port=$ANN_HANDOFF_PGPORT dbname=handoff_test"
TEST_CORES=$ANN_HANDOFF_TEST_CORES
RDMA_DEVICE=$ANN_HANDOFF_RDMA_DEVICE
PEER_PORTS=$ANN_HANDOFF_WAIT_PORTS
PEER_WAIT_HOURS=$ANN_HANDOFF_WAIT_HOURS
BENCH_PORT=$ANN_HANDOFF_BENCH_PORT

mkdir -p "$R"
echo $$ > "$H/run.pid"
: > "$R/summary.tsv"
exec >> "$H/run.log" 2>&1
log() { echo "[$(date '+%F %T')] $*"; }

# run NAME SECONDS COMMAND...: process-group-isolated, time-limited step.
run() {
	local name=$1 limit=$2 start=$SECONDS pid rc status
	shift 2
	setsid bash -c "$*" > "$R/$name.log" 2>&1 < /dev/null &
	pid=$!
	while kill -0 "$pid" 2>/dev/null; do
		if (( SECONDS - start > limit )); then
			kill -TERM -- "-$pid" 2>/dev/null; sleep 5; kill -KILL -- "-$pid" 2>/dev/null
			wait "$pid" 2>/dev/null
			printf '%s\tTIMEOUT\t%ss\n' "$name" "$limit" >> "$R/summary.tsv"
			log "$name: TIMEOUT after ${limit}s"
			return 124
		fi
		sleep 1
	done
	wait "$pid"; rc=$?
	if (( rc == 0 )); then status=PASS; else status="FAIL($rc)"; fi
	printf '%s\t%s\t%ss\n' "$name" "$status" "$(( SECONDS - start ))" >> "$R/summary.tsv"
	log "$name: $status in $(( SECONDS - start ))s"
	return $rc
}

peers_running() {
	local port
	for port in $PEER_PORTS; do
		ss -ltn 2>/dev/null | grep -q "127.0.0.1:$port " && return 0
	done
	return 1
}

log "handoff starting"
. "$ANN_HANDOFF_CUVS_ENV"
export PATH="$ANN_HANDOFF_PYTHON_BIN:$PGBIN:$PATH"

# 1. Source
if [ ! -d "$SRC/.git" ]; then
	git clone -q "$ANN_HANDOFF_REPO" "$SRC" || { log "clone failed"; exit 1; }
fi
cd "$SRC" || exit 1
git fetch -q origin && git checkout -q "$ANN_HANDOFF_BRANCH" && git reset -q --hard "origin/$ANN_HANDOFF_BRANCH"
COMMIT=$(git log --oneline -1)
log "source at $COMMIT"

# 2. Build, pinned to the test cores
T="taskset -c $TEST_CORES"
run build-worker 1800 "$T make -C ann clean >/dev/null && $T make -C ann USE_CUVS=1 USE_RDMA=1 && $T make -C ann ann-batch-tests"
run build-extension 1800 "$T make PG_CONFIG=$PGBIN/pg_config USE_RDMA=1 -j8 && make PG_CONFIG=$PGBIN/pg_config USE_RDMA=1 install"
$PGBIN/pg_ctl -D "$ANN_HANDOFF_PGDATA" status > /dev/null || $PGBIN/pg_ctl -D "$ANN_HANDOFF_PGDATA" -l "$H/postgres.log" start
sleep 2
$PGBIN/psql "$DSN_ADMIN" -Xq -c "DROP DATABASE IF EXISTS handoff_test" -c "CREATE DATABASE handoff_test"
$PGBIN/psql "$DSN_TEST" -Xq -c "CREATE EXTENSION vector"

# 3. Tests, one per process group
export ANN_PG_DSN="$DSN_TEST"
run ann-check 1800 "$T make -C ann check"
run installcheck 1800 "$T make PG_CONFIG=$PGBIN/pg_config installcheck PGHOST=$ANN_HANDOFF_PGHOST PGPORT=$ANN_HANDOFF_PGPORT"
list_tests() {
	python - "$1" <<'EOF'
import sys, unittest, importlib.util
sys.path.insert(0, 'test/ann')
spec = importlib.util.spec_from_file_location('m', sys.argv[1]); m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
def walk(s):
    for t in s:
        yield from (walk(t) if isinstance(t, unittest.TestSuite) else [t])
for t in walk(unittest.defaultTestLoader.loadTestsFromModule(m)):
    print(t.id().split('.', 1)[1])
EOF
}
for suite in index pipeline remote; do
	for t in $(list_tests "test/ann/test_$suite.py"); do
		run "test_${suite}.${t}" 300 "$T python -W ignore test/ann/test_$suite.py $t -v"
	done
done
for t in $(list_tests test/ann/test_remote.py); do
	run "test_remote_rdma.${t}" 300 "ANN_TEST_RDMA=$RDMA_DEVICE $T python -W ignore test/ann/test_remote.py $t -v"
done

# 4. GPU work waits until other GPU workloads (ANN_HANDOFF_WAIT_PORTS) stop
waited=0
while peers_running && (( waited < PEER_WAIT_HOURS * 3600 )); do
	(( waited % 1800 == 0 )) && log "waiting for workers on ports $PEER_PORTS to exit"
	sleep 60; waited=$(( waited + 60 ))
done
if peers_running; then
	log "ports $PEER_PORTS still in use after ${PEER_WAIT_HOURS}h; skipping GPU suites and benchmarks"
	GPU_SKIPPED=1
else
	GPU_SKIPPED=0
	run check-cuvs 3600 "ANN_CUVS_TEST=1 python -W ignore test/ann/test_cuvs_gathered.py && ANN_CUVS_TEST=1 python -W ignore test/ann/test_cuvs_index.py"

	# Benchmark: same workload over TCP, RDMA with spin polling, RDMA with interrupts.
	TOKEN_FILE=$H/token
	head -c 32 /dev/urandom | base64 > "$TOKEN_FILE"; chmod 600 "$TOKEN_FILE"
	TOKEN=$(cat "$TOKEN_FILE")
	INDEX=$ANN_HANDOFF_INDEX
	if [ -f "$INDEX/manifest" ] && $PGBIN/psql "$DSN_ADMIN" -XAtc "SELECT 1 FROM bench10m_queries LIMIT 1" | grep -q 1; then
		for mode in tcp rdma-spin rdma-wait; do
			extra=""; opts="-c ann.worker_address=127.0.0.1:$BENCH_PORT -c ann.worker_auth_token=$TOKEN"
			case $mode in
				rdma-spin) extra="--rdma $RDMA_DEVICE --rdma-poll spin"; opts="$opts -c ann.worker_rdma=$RDMA_DEVICE" ;;
				rdma-wait) extra="--rdma $RDMA_DEVICE --rdma-poll wait"; opts="$opts -c ann.worker_rdma=$RDMA_DEVICE" ;;
			esac
			setsid taskset -c $ANN_HANDOFF_WORKER_CORES ann/ann-worker-cuvs --backend cuvs --listen "$H/ann.sock" \
				--listen-tcp 127.0.0.1:$BENCH_PORT --auth-token-file "$TOKEN_FILE" --lanes 96 --itopk 64 $extra \
				> "$R/worker-$mode.log" 2>&1 < /dev/null &
			worker=$!
			for i in $(seq 1 100); do ss -ltn | grep -q "127.0.0.1:$BENCH_PORT " && break; sleep 0.2; done
			recall="--no-recall"; [ $mode = tcp ] && recall=""
			run "bench-$mode" 3600 "PGOPTIONS='$opts' taskset -c $ANN_HANDOFF_CLIENT_CORES python benchmarks/ann/cuvs_load.py bench \
				--dsn '$DSN_ADMIN' --table bench10m --index hb_$mode --path $INDEX --clients 64,256,1024 \
				--threads 96 --seconds 15 --workloads ann,hybrid-1pct --options '$opts' $recall"
			python - "$BENCH_PORT" "$TOKEN" > "$R/worker-$mode.stats" 2>&1 <<'EOF'
import json, socket, struct, sys
def frame(op, req, payload=b''):
    return b'PVAN' + struct.pack('<HHQQII', 1, op, req, 0, len(payload), 0) + payload
def recv(c):
    h = b''
    while len(h) < 32: h += c.recv(32 - len(h))
    n = struct.unpack_from('<I', h, 24)[0]; d = b''
    while len(d) < n: d += c.recv(n - len(d))
    return d
c = socket.create_connection(('127.0.0.1', int(sys.argv[1])))
c.sendall(frame(13, 1, sys.argv[2].encode())); recv(c)
c.sendall(frame(8, 2)); print(recv(c).decode())
EOF
			kill -TERM -- "-$worker" 2>/dev/null; sleep 3; kill -KILL -- "-$worker" 2>/dev/null
		done
	else
		log "benchmark data missing (bench10m table or $INDEX); skipping benchmarks"
	fi
fi

# 5. Report
{
	echo "# Handoff report"
	echo
	echo "Finished $(date '+%F %T') on commit \`$COMMIT\` in \`$SRC\`."
	echo
	echo "## Steps"
	echo
	echo "| Step | Result | Time |"
	echo "| --- | --- | --- |"
	awk -F'\t' '{printf "| %s | %s | %s |\n", $1, $2, $3}' "$R/summary.tsv"
	echo
	echo "Logs: \`$R/<step>.log\`."
	for mode in tcp rdma-spin rdma-wait; do
		if [ -f "$R/bench-$mode.log" ]; then
			echo; echo "## Benchmark: $mode"; echo; echo '```'
			grep -E "recall|qps=" "$R/bench-$mode.log"
			echo '```'
			[ -f "$R/worker-$mode.stats" ] && echo "Worker stats: \`$(cat "$R/worker-$mode.stats")\`"
		fi
	done
	echo
	echo "## What is left"
	echo
	failed=$(grep -cvE $'\tPASS\t' "$R/summary.tsv")
	if (( failed > 0 )); then
		echo "- **$failed step(s) did not pass.** Read their logs first:"
		grep -vE $'\tPASS\t' "$R/summary.tsv" | awk -F'\t' '{printf "  - `%s` (%s)\n", $1, $2}'
	else
		echo "- All steps passed."
	fi
	[ "${GPU_SKIPPED:-0}" = 1 ] && echo "- GPU suites and benchmarks were skipped: ports $PEER_PORTS stayed in use. Rerun this script."
	echo "- Record the TCP vs RDMA numbers above in the RDMA section of \`docs/ann-networked.md\`."
} > "$H/REPORT.md"
log "handoff finished; see $H/REPORT.md"
