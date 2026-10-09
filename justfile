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

# Spin up a 5-node in-process cluster: elect, kill the leader, restart it (verbose logs)
sandbox:
    go test -run 'TestSandboxElection' -v -count=1 -timeout 150s .

# List available recipes
default:
    @just --list
