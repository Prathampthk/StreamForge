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

.PHONY: tools-build fault-test scaling-test benchmark benchmark-audit k6-smoke
tools-build:
	docker compose --env-file .env -f deploy/compose.yaml -f deploy/compose.bench.yaml --profile tools build eventgen replayctl
fault-test:
	python3 scripts/local-faults.py
scaling-test: go-cache
	docker compose --env-file .env -f deploy/compose.yaml -f deploy/compose.ingest.yaml -f deploy/compose.consumer.yaml -f deploy/compose.query.yaml run --rm --no-deps -e STREAMFORGE_RUN_SCALING=1 integration-tests go test -race -tags=integration ./tests/integration -run TestMeasuredConsumerScaling -count=1 -timeout=10m
benchmark:
	python3 scripts/benchmark.py --duration 15m --rate 10500 --name steady-15m
benchmark-audit:
	python3 scripts/bench-audit.py load/results/raw/steady-15m --drain-timeout 120
k6-smoke:
	docker run --rm --network streamforge_default -v "$(CURDIR)/load/k6:/scripts:ro" -e DURATION=5s -e EVENTS_PER_SECOND=1000 -e TENANT=merchant_demo -e INGEST_URL=http://ingest-api:8080 -e INGEST_API_KEY=streamforge_demo_key_local_only_change_me grafana/k6:1.2.3 run /scripts/mixed.js

.PHONY: cloud-check security reproducible workflow-check
cloud-check:
	./scripts/terraform.sh init -backend=false -input=false
	TF_DIR=infra/terraform/bootstrap ./scripts/terraform.sh init -backend=false -input=false
	TF_DIR=infra/terraform ./scripts/terraform.sh fmt -check -recursive
	./scripts/terraform.sh validate
	./scripts/terraform.sh test
	TF_DIR=infra/terraform/bootstrap ./scripts/terraform.sh validate
	python3 scripts/aws/check-render.py
	$(MAKE) workflow-check
security:
	./scripts/go.sh go run golang.org/x/vuln/cmd/govulncheck@v1.1.4 ./...
reproducible:
	./scripts/reproducible.sh
	./scripts/reproducible-image.sh
workflow-check:
	./scripts/go.sh go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12

.PHONY: cloud-db-check
cloud-db-check:
	python3 scripts/aws/check-db-grants.py

.PHONY: replicated-kafka-test
replicated-kafka-test:
	python3 scripts/check-replicated-kafka.py

.PHONY: process-shutdown-test
process-shutdown-test:
	python3 scripts/check-replicated-kafka.py --with-process-shutdown

.PHONY: incremental-analytics-test
incremental-analytics-test:
	python3 experiments/incremental-analytics/prototype.py run

.PHONY: incremental-analytics-stress
incremental-analytics-stress:
	python3 experiments/incremental-analytics/prototype.py stress
