.PHONY: up down migrate smoke test fmt lint ps
up:
	@test -f .env || cp .env.example .env
	./scripts/compose.sh up -d --wait --wait-timeout 240
down:
	./scripts/compose.sh down --remove-orphans
ps:
	./scripts/compose.sh ps
migrate:
	./scripts/migrate.sh
smoke:
	python3 scripts/smoke.py
test:
	python3 -m unittest discover -s tests -v
	@if command -v go >/dev/null; then go test -race ./...; else ./scripts/go.sh go test -race ./...; fi
fmt:
	@if command -v gofmt >/dev/null; then gofmt -w $$(find cmd internal tests -name '*.go'); else ./scripts/go.sh sh -c 'gofmt -w $$(find cmd internal tests -name "*.go")'; fi
lint:
	@if command -v go >/dev/null; then go vet ./...; else ./scripts/go.sh go vet ./...; fi

.PHONY: ingest-up ingest-down test-go-docker test-integration build
build:
	@if command -v go >/dev/null; then go build ./cmd/ingest-api; else ./scripts/go.sh go build ./cmd/ingest-api; fi

test-go-docker:
	./scripts/go.sh sh -c 'go vet ./... && go test -race ./...'

test-integration:
	GO_DOCKER_NETWORK=streamforge_default KAFKA_BROKERS=kafka:19092 ./scripts/go.sh go test -race -v -tags=integration -count=1 ./tests/integration

ingest-up:
	docker compose --env-file .env -f deploy/compose.yaml -f deploy/compose.ingest.yaml up -d --build --wait --wait-timeout 240

ingest-down:
	docker compose --env-file .env -f deploy/compose.yaml -f deploy/compose.ingest.yaml stop ingest-api

.PHONY: ingest-smoke
ingest-smoke:
	python3 scripts/ingest-smoke.py
