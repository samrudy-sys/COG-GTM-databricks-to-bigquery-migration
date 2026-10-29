PY ?= .venv/bin/python
AIRFLOW_CONSTRAINTS = https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.10.txt
EMULATOR_IMAGE = ghcr.io/goccy/bigquery-emulator:latest
UNIT ?=

.PHONY: setup setup-legacy emulator inventory baseline parity parity-all integration report lint serve

setup:            ## venv with Airflow + Google provider (target side)
	test -x $(PY) || python3 -m venv .venv
	$(PY) -m pip install -q -r requirements.txt --constraint $(AIRFLOW_CONSTRAINTS)
	$(PY) -m pip install -q ruff==0.6.9  # newer than the Airflow constraints pin

setup-legacy: setup  ## + PySpark/Delta to replay the legacy estate (needs Java 17)
	$(PY) -m pip install -q -r requirements-legacy.txt

emulator:         ## local BigQuery emulator on :9050 (project demo-proj)
	@docker inspect bqemu >/dev/null 2>&1 && docker start bqemu >/dev/null || \
	  docker run -d --name bqemu -p 9050:9050 -p 9060:9060 $(EMULATOR_IMAGE) --project=demo-proj >/dev/null
	@for i in $$(seq 1 30); do curl -sf localhost:9050/bigquery/v2/projects/demo-proj/datasets >/dev/null && echo "emulator up" && exit 0; sleep 1; done; exit 1

inventory:        ## discover the ADF + Databricks estate -> migration/scope/
	$(PY) tools/inventory.py

baseline:         ## replay the legacy estate on Spark+Delta -> migration/baseline/
	$(PY) tools/parity.py baseline

parity:           ## parity for one unit: make parity UNIT=salesdaily
	$(PY) tools/parity.py run $(UNIT)

parity-all:       ## parity for every migrated unit (isolated, upstream from baseline)
	$(PY) tools/parity.py run --all

integration:      ## whole product end-to-end on BigQuery via the master DAG wave order
	$(PY) tools/parity.py integration

report:           ## HTML migration sign-off report -> reports/index.html
	$(PY) tools/report.py

lint:
	$(PY) -m ruff check tools target
	$(PY) tools/parity.py check-dags

serve: report     ## serve the sign-off report on :8000
	cd reports && python3 -m http.server 8000
