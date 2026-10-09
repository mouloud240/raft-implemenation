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

# Spin up a 3-node in-process cluster and observe an election (verbose logs)
sandbox:
    go test -run 'TestSandboxElection' -v -count=1 -timeout 90s .

# List available recipes
default:
    @just --list
