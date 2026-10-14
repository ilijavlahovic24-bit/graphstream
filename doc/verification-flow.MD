# Rust flow
# 1) Building project
cargo build -p engine --bin graphstream
# 2) Testing project
cargo test
# 3) Single Node command and vizualization
cargo run -p engine --bin graphstream -- --dataset crates/engine/data/example.json viz serve --port 7475 --mode playback --speed 100 --autoplay


# Kubernetes flow
# 1) Install operator + monitoring (no sample yet)
cd deploy
./install.sh

# 2) Confirm operator is running
kubectl -n graphstream-system get pods
# NAME                                   READY   STATUS    RESTARTS   AGE
# graphstream-operator-6f8d4b7c9c-abcde  1/1     Running   0          30s

# 3) Install a sample cluster
kubectl apply -f 05-samples/graphstreamcluster-minimal.yaml

# 4) Watch it converge
kubectl -n graphstream-clusters get gsc -w
# NAME      SHARDS   READY   PHASE         AGE
# minimal   2        0       Pending       2s
# minimal   2        1       Progressing   12s
# minimal   2        2       Ready         28s

# 5) Inspect child objects
kubectl -n graphstream-clusters get statefulsets,svc,cm

# 6) Check status conditions
kubectl -n graphstream-clusters get gsc minimal -o yaml | \
yq '.status'

# 7) Metrics
kubectl -n graphstream-clusters port-forward svc/minimal 9090:7474 &
curl -s http://localhost:9090/metrics | grep graphstream_

# 8) Cleanup
./uninstall.sh