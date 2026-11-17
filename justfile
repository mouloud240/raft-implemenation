# Run the application with live reload (air watches .go/.proto/.toml files)
run-dev:
    air

alias dev := run-dev

# Regenerate Go protobuf + gRPC code from all .proto files
gen-proto:
    protoc \
        --go_out=paths=source_relative:. \
        --go-grpc_out=paths=source_relative:. \
        $(find . -name '*.proto' -not -path './tmp/*')

# Fast 3-node sandbox: elect, then simulate client commands
# (set/increment/delete) and require cluster-wide state machine convergence
sandbox-commands:
    go test -run 'TestSandboxCommands' -v -count=1 -timeout 120s .

# 5-node sandbox: elect, command, kill leader, restart, command again
sandbox-election:
    go test -run 'TestSandboxElection' -v -count=1 -timeout 200s .

# Boot caddy + nodes 1-3 in the background for manual curl tests (logs+pids in /tmp/opencode/raft-demo).
up:
    #!/usr/bin/env zsh
    set -eu
    DIR=/tmp/opencode/raft-demo
    just -q down >/dev/null 2>&1 || true
    mkdir -p "$DIR"/d1 "$DIR"/d2 "$DIR"/d3
    go build -o "$DIR/node" .
    caddy run --config deploy/Caddyfile >"$DIR/caddy.log" 2>&1 &
    echo $! > "$DIR/caddy.pid"
    sleep 1
    for i in 1 2 3; do
      RAFT_NODE_ID=$i RAFT_DATA_DIR="$DIR/d$i" RAFT_PROXY=caddy "$DIR/node" >"$DIR/n$i.log" 2>&1 &
      echo $! > "$DIR/n$i.pid"
    done
    sleep 2
    echo "cluster up; clients hit :8080 (caddy). Logs in $DIR."
    echo "  put:  curl -i -X POST localhost:8080/set -H 'Content-Type: application/json' -d '{\"key\":\"a\",\"value\":1,\"operation\":0}'"
    echo "  get:  curl -i localhost:8080/get/a"

# Stop (and clean pids of) everything `just up` started.
down:
    #!/usr/bin/env zsh
    DIR=/tmp/opencode/raft-demo
    for f in "$DIR"/*.pid(N); do
      kill -9 "$(cat "$f")" 2>/dev/null
      rm -f "$f"
    done
    echo "demo down."

# Full sandbox: both tests (commands + election/failover)
sandbox:
    go test -run 'TestSandbox' -v -count=1 -timeout 300s .

# Full sandbox under the race detector (reports remaining unlocked state reads)
sandbox-race:
    go test -run 'TestSandbox' -v -count=1 -timeout 300s -race .

# Re-compile deploy/Caddyfile (source of truth) into deploy/caddy.json, the go:embed-ed artifact. Needs caddy.
gen-caddy:
    caddy adapt --config deploy/Caddyfile --pretty > deploy/caddy.json

# Start node N (gRPC 5005N, client API 808N) with route pushes enabled; run 1|2|3 in separate shells with run-proxy.
run-node id:
    RAFT_NODE_ID={{id}} RAFT_PROXY=caddy go run .

# Start caddy with the bootstrap config: clients hit :8080, elections re-point the upstream via admin API :2019/load.
run-proxy:
    caddy run --config deploy/Caddyfile

# List available recipes
default:
    @just --list
