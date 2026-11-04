.DEFAULT_GOAL := help
SHELL := /bin/bash
COMPOSE := docker compose
LOCAL_DSN := postgresql://warehouse:warehouse@localhost:5433/warehouse
# Fechas portables (Linux y macOS)
day = $(shell python3 -c "import datetime as d; print(d.date.today() + d.timedelta(days=$(1)))")

help: ## Muestra esta ayuda
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

setup: ## Crea .env, venv local e instala dependencias de desarrollo
	@test -f .env || (cp .env.example .env && sed -i.bak "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$$(id -u)/" .env && rm -f .env.bak)
	python3 -m venv .venv && .venv/bin/pip install -q -r requirements-dev.txt
	@echo "Listo. Activá el entorno con: source .venv/bin/activate"

up: ## Construye y levanta Airflow + warehouse
	$(COMPOSE) up -d --build
	@echo "Airflow: http://localhost:8080  |  Warehouse: localhost:5433 (warehouse/warehouse)"

down: ## Apaga los contenedores (conserva los datos)
	$(COMPOSE) down

reset: ## Apaga y BORRA volúmenes y data lake local
	$(COMPOSE) down -v
	rm -rf data/lake/bronze logs/*/

logs: ## Sigue los logs del scheduler
	$(COMPOSE) logs -f airflow-scheduler

trigger: ## Dispara el DAG para hoy
	$(COMPOSE) exec airflow-scheduler airflow dags unpause ml_medallion_daily
	$(COMPOSE) exec airflow-scheduler airflow dags trigger ml_medallion_daily

backfill: ## Backfill de las últimas 2 semanas (START=YYYY-MM-DD END=YYYY-MM-DD para cambiar)
	$(COMPOSE) exec airflow-scheduler airflow backfill create --dag-id ml_medallion_daily \
		--from-date $${START:-$(call day,-14)} --to-date $${END:-$(call day,0)}

demo-volume-drop: ## Corre un día simulando una caída de volumen (debe fallar DQ)
	ML_SAMPLE_SCENARIO=volume_drop WAREHOUSE_DSN=$(LOCAL_DSN) PYTHONPATH=src LAKE_ROOT=data/lake \
		.venv/bin/python -m ml_pipeline run --date $(call day,1) || true

demo-schema-break: ## Corre un día simulando un cambio de esquema (debe fallar el contrato)
	ML_SAMPLE_SCENARIO=schema_break WAREHOUSE_DSN=$(LOCAL_DSN) PYTHONPATH=src LAKE_ROOT=data/lake \
		.venv/bin/python -m ml_pipeline run --date $(call day,2) || true

run-local: ## Corre el pipeline SIN Airflow contra el warehouse de Docker (DATE=YYYY-MM-DD)
	WAREHOUSE_DSN=$(LOCAL_DSN) PYTHONPATH=src LAKE_ROOT=data/lake \
		.venv/bin/python -m ml_pipeline run --date $${DATE:-$(call day,0)}

test: ## Tests unitarios
	.venv/bin/pytest -m "not integration"

test-integration: ## Tests end-to-end contra el warehouse de Docker (¡borra sus datos!)
	WAREHOUSE_DSN=$(LOCAL_DSN) .venv/bin/pytest -m integration

lint: ## Ruff (lint + formato)
	.venv/bin/ruff check . && .venv/bin/ruff format --check .

psql: ## Consola SQL del warehouse
	$(COMPOSE) exec warehouse psql -U warehouse -d warehouse

.PHONY: help setup up down reset logs trigger backfill demo-volume-drop demo-schema-break run-local test test-integration lint psql
