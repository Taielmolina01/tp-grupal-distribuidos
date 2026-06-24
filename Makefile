SHELL := /bin/bash
PWD := $(shell pwd)

INPUT_DIR    ?= ./input
OUTPUT_DIR   ?= ./output
EXPECTED_DIR ?= ./expected_output
N_CLIENTS    ?= 1

LIME  := \033[38;2;138;206;0m
RED   := \033[31m
RESET := \033[0m

ROUNDS=5 
SLEEP_ROUND=10
AMOUNT_OF_CONTAINERS_TO_RESTART=3

up:
	mkdir -p output
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	docker compose -f docker-compose.yaml logs --follow
.PHONY: up

down:
	docker compose -f docker-compose.yaml stop -t 1
	docker compose -f docker-compose.yaml down
.PHONY: down

logs:
	docker compose -f docker-compose.yaml logs
.PHONY: logs

test:
	mkdir -p output
	rm ./output/* -f
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	go run ./verify_output.go
	docker compose -f docker-compose.yaml stop -t 1
	docker compose -f docker-compose.yaml down
.PHONY: test

compose:
	@cd scripts/compose-gen && GOWORK=off go run . $(if $(CONFIG),-config $(CONFIG),)
.PHONY: compose

switch:
	@echo Escenarios de prueba:
	@echo "1) Un cliente, una sola réplica de cada elemento"
	@echo "2) Múltiples clientes, una sola réplica de cada elemento"
	@echo "3) Múltiples clientes, sum replicado, un solo aggregation"
	@echo "4) Múltiples clientes, múltiples réplicas"
	@echo "5) Múltiples clientes, múltiples réplicas, nombres al azar"
	@read -p "Selecciona uno [1-5]: " option;	\
	cp ./scenarios/$${option}.yaml docker-compose.yaml
.PHONY: switch

EXPECTED_ENV = INPUT_DIR=$(PWD)/$(INPUT_DIR) EXPECTED_DIR=$(PWD)/$(EXPECTED_DIR) OUTPUT_DIR=$(PWD)/$(OUTPUT_DIR) N_CLIENTS=$(N_CLIENTS)

CHAOS_MONKEY_EXPECTED_ENV = YAML_PATH=$(PWD)/docker-compose.yaml ROUNDS=$(ROUNDS) SLEEP_ROUND=$(SLEEP_ROUND) AMOUNT_OF_CONTAINERS_TO_RESTART=$(AMOUNT_OF_CONTAINERS_TO_RESTART)

build-expected:
	@cd scripts/expected-output && GOWORK=off $(EXPECTED_ENV) go run . build
.PHONY: build-expected

verify-output:
	@cd scripts/expected-output && GOWORK=off $(EXPECTED_ENV) go run . verify
.PHONY: verify-output

output-test: build-expected verify-output
.PHONY: output-test

build-race:
	@go build -race ./src/...
	@cd src && go build -race ./...
.PHONY: build-race

chaos-monkey:
	@cd scripts/chaos-monkey && GOWORK=off $(CHAOS_MONKEY_EXPECTED_ENV) go run . chaos-monkey
.PHONY: chaos-monkey