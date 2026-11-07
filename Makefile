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
	docker compose --env-file .env -f deploy/compose.yaml -f deploy/compose.ingest.yaml -f deploy/compose.consumer.yaml run --rm --no-deps integration-tests

ingest-up:
	docker compose --env-file .env -f deploy/compose.yaml -f deploy/compose.ingest.yaml -f deploy/compose.consumer.yaml up -d --build --wait --wait-timeout 240 ingest-api

ingest-down:
	docker compose --env-file .env -f deploy/compose.yaml -f deploy/compose.ingest.yaml -f deploy/compose.consumer.yaml stop ingest-api

.PHONY: ingest-smoke
ingest-smoke:
	python3 scripts/ingest-smoke.py

.PHONY: consumer-up consumer-down
consumer-up:
	docker compose --env-file .env -f deploy/compose.yaml -f deploy/compose.ingest.yaml -f deploy/compose.consumer.yaml up -d --build --wait --wait-timeout 240 consumer
consumer-down:
	docker compose --env-file .env -f deploy/compose.yaml -f deploy/compose.ingest.yaml -f deploy/compose.consumer.yaml stop consumer

.PHONY: consumer-smoke
consumer-smoke:
	python3 scripts/consumer-smoke.py

.PHONY: go-cache
go-cache:
	@docker volume create streamforge_go_mod >/dev/null
	@docker volume create streamforge_go_build >/dev/null
