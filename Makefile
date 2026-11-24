PKGS = org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.iceberg:iceberg-aws-bundle:1.5.2,org.postgresql:postgresql:42.7.3
SUBMIT = docker compose exec -T spark spark-submit --packages $(PKGS) --conf spark.sql.shuffle.partitions=8
EVENTS ?= 200000
UPDATE_RATIO ?= 0.85

.PHONY: help up connector stream load drift bench replay snapshots rollback query test down clean

help:
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-12s %s\n",$$1,$$2}'

up: ## start postgres, kafka, debezium, minio, iceberg rest catalog, spark
	docker compose up -d
	@echo "waiting for connect..." && sleep 20
	docker compose exec -u root -T spark mkdir -p /opt/bitnami/spark/.ivy2/cache /opt/bitnami/spark/.ivy2/jars /opt/bench
	docker compose exec -u root -T spark chown -R 1001:0 /opt/bitnami/spark/.ivy2 /opt/bench

connector: ## register the Debezium connector for public.blocks
	curl -sf -X POST -H "Content-Type: application/json" \
		--data @config/debezium-blocks.json http://localhost:8083/connectors | head -c 400
	@echo "\nstatus:" && curl -sf http://localhost:8083/connectors/blocks-cdc/status

stream: ## run the streaming upsert job (foreground; ctrl-c to stop)
	$(SUBMIT) /opt/app/pipeline/stream_to_iceberg.py

load: ## generate an update-heavy workload against Postgres (needs: pip install -r requirements.txt)
	PG_HOST=localhost python3 generator/workload.py --events $(EVENTS) --update-ratio $(UPDATE_RATIO)

drift: ## add a column upstream mid-stream and watch the table evolve
	PG_HOST=localhost python3 generator/workload.py --events 20000 --drift-at 5000

backfill: ## snapshot-merge Postgres into the lakehouse (safe while streaming)
	$(SUBMIT) /opt/app/pipeline/backfill.py

bench: ## summarize throughput from the stream's batch metrics
	docker compose exec -T spark python3 /opt/app/bench/benchmark.py --out /opt/app/bench/report.json
	@cat bench/report.json

replay: ## reprocess the whole topic and assert the table is unchanged
	$(SUBMIT) /opt/app/pipeline/replay.py --mode replay

snapshots: ## list Iceberg snapshots
	$(SUBMIT) /opt/app/pipeline/replay.py --mode snapshots

rollback: ## roll the table back: make rollback SNAPSHOT_ID=...
	$(SUBMIT) /opt/app/pipeline/replay.py --mode rollback --snapshot-id $(SNAPSHOT_ID)

query: ## row count and update mix in the lakehouse
	$(SUBMIT) /opt/app/pipeline/query.py

test: ## unit tests (no infrastructure needed)
	python3 -m pytest -q

down: ## stop everything
	docker compose down -v
