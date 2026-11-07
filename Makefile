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

test-integration: go-cache
	docker compose --env-file .env -f deploy/compose.yaml -f deploy/compose.ingest.yaml -f deploy/compose.consumer.yaml -f deploy/compose.query.yaml run --rm --no-deps integration-tests

ingest-up:
	docker compose --env-file .env -f deploy/compose.yaml -f deploy/compose.ingest.yaml -f deploy/compose.consumer.yaml -f deploy/compose.query.yaml up -d --build --wait --wait-timeout 240 ingest-api

ingest-down:
	docker compose --env-file .env -f deploy/compose.yaml -f deploy/compose.ingest.yaml -f deploy/compose.consumer.yaml -f deploy/compose.query.yaml stop ingest-api

.PHONY: ingest-smoke
ingest-smoke:
	python3 scripts/ingest-smoke.py

.PHONY: consumer-up consumer-down
consumer-up:
	docker compose --env-file .env -f deploy/compose.yaml -f deploy/compose.ingest.yaml -f deploy/compose.consumer.yaml -f deploy/compose.query.yaml up -d --build --wait --wait-timeout 240 consumer
consumer-down:
	docker compose --env-file .env -f deploy/compose.yaml -f deploy/compose.ingest.yaml -f deploy/compose.consumer.yaml -f deploy/compose.query.yaml stop consumer

.PHONY: consumer-smoke
consumer-smoke:
	python3 scripts/consumer-smoke.py

.PHONY: go-cache
go-cache:
	@docker volume create streamforge_go_mod >/dev/null
	@docker volume create streamforge_go_build >/dev/null

.PHONY: query-up query-down
query-up:
	docker compose --env-file .env -f deploy/compose.yaml -f deploy/compose.ingest.yaml -f deploy/compose.consumer.yaml -f deploy/compose.query.yaml up -d --build --wait --wait-timeout 240 query-api
query-down:
	docker compose --env-file .env -f deploy/compose.yaml -f deploy/compose.ingest.yaml -f deploy/compose.consumer.yaml -f deploy/compose.query.yaml stop query-api

.PHONY: observe-up observe-down
observe-up:
	docker compose --env-file .env -f deploy/compose.yaml -f deploy/compose.ingest.yaml -f deploy/compose.consumer.yaml -f deploy/compose.query.yaml -f deploy/compose.observability.yaml up -d --build --wait --wait-timeout 240
	python3 scripts/observe-health.py
observe-down:
	docker compose --env-file .env -f deploy/compose.yaml -f deploy/compose.ingest.yaml -f deploy/compose.consumer.yaml -f deploy/compose.query.yaml -f deploy/compose.observability.yaml stop prometheus grafana otel-collector jaeger

.PHONY: query-smoke reconcile observe-smoke
query-smoke:
	python3 scripts/query-smoke.py
reconcile:
	python3 scripts/reconcile.py "$(MANIFEST)"
observe-smoke:
	python3 scripts/observe-smoke.py "$(MANIFEST)"
