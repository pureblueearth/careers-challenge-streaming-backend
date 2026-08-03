# Convenience targets. Python 3.10+ stdlib only, no pip dependencies.

PYTHON ?= python3
SERVICE_URL ?= http://localhost:8080

.PHONY: help example smoke baseline burst offline adversarial test up run down logs

help:
	@echo "Targets:"
	@echo "  run            build + start the stack (mosquitto + redis + service) in the foreground"
	@echo "  up             same, detached; down stops it; logs tails the service"
	@echo "  test           run the Go unit + property tests"
	@echo "  example        run the example stub service on :8080 (replace with yours)"
	@echo "  smoke          30s baseline run against \$$SERVICE_URL + scorecard"
	@echo "  baseline       60s baseline"
	@echo "  burst          3min run with two 10x bursts"
	@echo "  offline        2min run with 20% of devices going offline + replaying"
	@echo "  adversarial    4min run combining burst + offline + clock skew"
	@echo
	@echo "Override SERVICE_URL=... or DEVICES=... as needed."

# --- solution service (Go + Docker Compose) ---

test:
	go test ./...

run:
	docker-compose up --build

up:
	docker-compose up --build -d

down:
	docker-compose down -v

logs:
	docker-compose logs -f service

# --- provided evaluation harness ---

DEVICES ?= 50

example:
	$(PYTHON) example_solution/service.py

smoke:
	$(PYTHON) eval/check.py smoke --target $(SERVICE_URL) --devices $(DEVICES)

baseline:
	$(PYTHON) eval/check.py baseline --target $(SERVICE_URL) --devices $(DEVICES)

burst:
	$(PYTHON) eval/check.py burst --target $(SERVICE_URL) --devices $(DEVICES)

offline:
	$(PYTHON) eval/check.py offline --target $(SERVICE_URL) --devices $(DEVICES)

adversarial:
	$(PYTHON) eval/check.py adversarial --target $(SERVICE_URL) --devices $(DEVICES)
