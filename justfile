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

# Full sandbox: both tests (commands + election/failover)
sandbox:
    go test -run 'TestSandbox' -v -count=1 -timeout 300s .

# Full sandbox under the race detector (reports remaining unlocked state reads)
sandbox-race:
    go test -run 'TestSandbox' -v -count=1 -timeout 300s -race .

# List available recipes
default:
    @just --list
