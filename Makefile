.PHONY: up down migrate smoke test fmt lint ps
up:
	@test -f .env || cp .env.example .env
	./scripts/compose.sh up -d --wait --wait-timeout 240
down:
	./scripts/compose.sh down
ps:
	./scripts/compose.sh ps
migrate:
	./scripts/migrate.sh
smoke:
	python3 scripts/smoke.py
test:
	python3 -m unittest discover -s tests -v
fmt:
	@test -z "$$(gofmt -l $$(find . -name '*.go'))"
lint:
	go vet ./...
	go test -race ./...
