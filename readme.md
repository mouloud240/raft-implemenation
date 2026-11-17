
### Raft Implementation


This repository is a simple raft Implementation from golang for educational purposes. It is based on the original Raft paper by Diego Ongaro and John Ousterhout.

### AI usage


The ai usage for this project will be kept to the very minimum. it will be used to generate some test cases and the sandbox  , but other than that every line you see in this repository is written by me.
This is because this project is supposed to be educational and we want to keep the slop away from it   (we don't really care about the sandbox Implementation as long as it works, this is why we used ai for it )

### Leader routing (Caddy reverse proxy)

Client traffic enters through Caddy (:8080), which forwards to the current leader's client API. When a node wins an election it replaces Caddy's full config via the admin API (127.0.0.1:2019/load) — the proxy has a single writer (the promotion transition) and a single source of truth, so honest-but-deposed leaders can never fight the new leader over the route. Direct requests to a non-leader answer 409 with a leaderId hint instead of blocking; that fast-fail covers the ship between leader death and the next route push.

- `RAFT_PROXY=caddy` enables the push (default: noop, logs only)
- `deploy/Caddyfile` is the human-readable source of truth; `just gen-caddy` re-compiles it into the go:embed-ed `deploy/caddy.json`
- ports: gRPC 5005N, client API 808N, Caddy :8080 (node N = RAFT_NODE_ID)
- manual smoke: `just run-proxy`, then `just run-node 1|2|3` in separate shells, then e.g. `curl -i -X POST localhost:8080/set -d '{"key":"a","value":1,"operation":0}'`


