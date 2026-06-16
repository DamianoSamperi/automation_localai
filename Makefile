# ── Configuration ─────────────────────────────────────────────────
SIDECAR_IMAGE   ?= dami00/localai-worker-sidecar:latest
REGISTRY        ?= docker.io
CONFIG_FILE     ?= clusters.json
ORCHESTRATOR    ?= ./orchestrator
PORT            ?= 8090

# ── Default target ────────────────────────────────────────────────
.PHONY: all
all: build-sidecar run

# ═══════════════════════════════════════════════════════
# ORCHESTRATOR
# ═══════════════════════════════════════════════════════

.PHONY: build
build:
	@echo "▶ Building orchestrator..."
	go mod tidy
	go build -o orchestrator .
	@echo "✅ orchestrator binary ready"

.PHONY: run
run: build
	@echo "▶ Starting orchestrator on http://localhost:$(PORT)"
	$(ORCHESTRATOR) $(CONFIG_FILE)

.PHONY: run-dev
run-dev:
	@echo "▶ Running orchestrator (no rebuild)..."
	go run . $(CONFIG_FILE)

# ═══════════════════════════════════════════════════════
# SIDECAR
# ═══════════════════════════════════════════════════════

.PHONY: build-sidecar
build-sidecar:
	@echo "▶ Building sidecar image (amd64)..."
	docker build \
		--platform linux/amd64 \
		-t $(SIDECAR_IMAGE) \
		./sidecar
	@echo "✅ Sidecar image built: $(SIDECAR_IMAGE)"

.PHONY: push-sidecar
push-sidecar: build-sidecar
	@echo "▶ Pushing sidecar to $(REGISTRY)..."
	docker push $(SIDECAR_IMAGE)
	@echo "✅ Pushed: $(SIDECAR_IMAGE)"

.PHONY: sidecar
sidecar: push-sidecar
	@echo "✅ Sidecar build+push complete"

# ═══════════════════════════════════════════════════════
# KUBERNETES HELPERS
# ═══════════════════════════════════════════════════════

.PHONY: install-gateway-crds
install-gateway-crds:
	@echo "▶ Installing Kubernetes Gateway API CRDs..."
	kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.5.1/standard-install.yaml
	@echo "✅ Gateway API CRDs installed"

.PHONY: install-istio
install-istio:
	@echo "▶ Installing Istio with Gateway API support..."
	@which istioctl > /dev/null 2>&1 || (echo "❌ istioctl not found. Download from https://istio.io/latest/docs/setup/getting-started/" && exit 1)
	istioctl install --set profile=default \
		--set values.pilot.env.PILOT_ENABLE_GATEWAY_API=true \
		--set values.pilot.env.PILOT_ENABLE_GATEWAY_API_STATUS=true \
		-y
	@echo "✅ Istio installed"

.PHONY: enable-istio-injection
enable-istio-injection:
	@echo "▶ Enabling Istio sidecar injection on local-ai namespace..."
	kubectl label namespace local-ai istio-injection=enabled --overwrite
	@echo "✅ Done"

.PHONY: setup-cluster
setup-cluster: install-gateway-crds install-istio enable-istio-injection
	@echo "✅ Cluster fully configured for LocalAI Orchestrator"

# ═══════════════════════════════════════════════════════
# STATUS / DEBUG
# ═══════════════════════════════════════════════════════

.PHONY: status
status:
	@echo "=== Pods (local-ai) ==="
	kubectl get pods -n local-ai -o wide
	@echo ""
	@echo "=== Services (local-ai) ==="
	kubectl get svc -n local-ai
	@echo ""
	@echo "=== Gateways (local-ai) ==="
	kubectl get gateway -n local-ai
	@echo ""
	@echo "=== HTTPRoutes (local-ai) ==="
	kubectl get httproute -n local-ai

.PHONY: logs-master
logs-master:
	kubectl logs -n local-ai -l cluster-id=$(CLUSTER),role=master --all-containers -f

.PHONY: logs-workers
logs-workers:
	kubectl logs -n local-ai -l cluster-id=$(CLUSTER),role=worker --all-containers -f

.PHONY: clean-all
clean-all:
	@echo "⚠ Deleting ALL local-ai resources..."
	kubectl delete all,gateway,httproute,pvc,configmap -n local-ai --all --ignore-not-found=true
	@echo "✅ Cleaned"

.PHONY: clean-cluster
clean-cluster:
	kubectl delete deployment,service,gateway,httproute -n local-ai \
		-l cluster-id=$(CLUSTER) --ignore-not-found=true
	kubectl delete configmap localai-worker-registry -n local-ai --ignore-not-found=true

# ═══════════════════════════════════════════════════════
# EXPERIMENTS
# ═══════════════════════════════════════════════════════

.PHONY: check-locust
check-locust:
	@which locust > /dev/null 2>&1 && echo "✅ locust: $$(locust --version)" || \
		echo "❌ locust not found. Run: make install-locust"

.PHONY: install-locust
install-locust:
	pip install locust

.PHONY: clean-experiments
clean-experiments:
	rm -rf experiments/results/* experiments/locustfiles/*

# ═══════════════════════════════════════════════════════
# HELP
# ═══════════════════════════════════════════════════════

.PHONY: help
help:
	@echo ""
	@echo "LocalAI Orchestrator — Targets:"
	@echo ""
	@echo "  make build              Build orchestrator binary"
	@echo "  make run                Build + run (localhost:$(PORT))"
	@echo "  make run-dev            go run (no rebuild)"
	@echo ""
	@echo "  make build-sidecar      Build sidecar Docker image (amd64)"
	@echo "  make push-sidecar       Build + push sidecar"
	@echo ""
	@echo "  make setup-cluster      Install Gateway CRDs + Istio + injection"
	@echo "  make install-gateway-crds"
	@echo "  make install-istio"
	@echo "  make enable-istio-injection"
	@echo ""
	@echo "  make status             Show pods/services/gateways"
	@echo "  make logs-master   CLUSTER=cluster-1"
	@echo "  make logs-workers  CLUSTER=cluster-1"
	@echo "  make clean-cluster CLUSTER=cluster-1"
	@echo "  make clean-all          ⚠ Delete ALL local-ai resources"
	@echo ""
	@echo "  make check-locust       Check locust installation"
	@echo "  make install-locust     pip install locust"
	@echo "  make clean-experiments  Remove experiment results"
	@echo ""
