.PHONY: all build test clean docker-build up down logs benchmark k8s-dry-run

# Variables
GO := go
BIN_DIR := bin
DOCKER_COMPOSE := docker compose -f deploy/docker-compose.yml

all: test build

build:
	@mkdir -p $(BIN_DIR)
	@echo "==> Building Gateway..."
	CGO_ENABLED=0 $(GO) build -ldflags="-w -s" -o $(BIN_DIR)/gateway ./cmd/gateway
	@echo "==> Building Mock LLM Worker..."
	CGO_ENABLED=0 $(GO) build -ldflags="-w -s" -o $(BIN_DIR)/mock_llm ./cmd/mock_llm
	@echo "==> Binaries built in $(BIN_DIR)/"

test:
	@echo "==> Running tests..."
	CGO_ENABLED=0 $(GO) test -v ./...

docker-build:
	@echo "==> Building Docker images..."
	$(DOCKER_COMPOSE) build

up:
	@echo "==> Launching full production stack (Gateway, Redis, Postgres, Worker, Prometheus)..."
	$(DOCKER_COMPOSE) up -d

down:
	@echo "==> Stopping production stack..."
	$(DOCKER_COMPOSE) down -v

logs:
	$(DOCKER_COMPOSE) logs -f

benchmark:
	@echo "==> Executing benchmark..."
	./scripts/benchmark.sh

k8s-dry-run:
	@echo "==> Validating Kubernetes manifests..."
	kubectl apply --dry-run=client -f deploy/kubernetes/

clean:
	@echo "==> Cleaning build artifacts..."
	rm -rf $(BIN_DIR)
