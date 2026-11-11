PY ?= .venv/bin/python
LIMIT ?= 5000
GROUP ?= wikistream-duckdb

.PHONY: install up down produce consume consume-2 report lag replay dlq crash-demo test typecheck e2e clean

install:            ## create a virtualenv and install the project with dev tools
	python3 -m venv .venv
	.venv/bin/pip install -e '.[dev]'

up:                 ## start Kafka and wait until it is healthy
	docker compose up -d --wait

down:               ## stop Kafka and delete its data
	docker compose down -v

produce:            ## stream LIMIT live Wikimedia edits into Kafka
	$(PY) -m wikistream.producer --limit $(LIMIT)

consume:            ## one consumer: Kafka -> DuckDB, stops after 10 s without new messages
	$(PY) -m wikistream.consumer --idle-timeout 10

consume-2:          ## two consumers in the same group: watch the partitions being split
	$(PY) -m wikistream.consumer --workers 2 --idle-timeout 10

report:             ## summary of what landed in DuckDB
	$(PY) -m wikistream.report

lag:                ## committed offsets and lag per partition for the consumer group
	docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
		--bootstrap-server localhost:9092 --describe --group $(GROUP)

replay:             ## rewind the group to the start of the topic: everything is re-read, nothing duplicated
	docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
		--bootstrap-server localhost:9092 --group $(GROUP) \
		--reset-offsets --to-earliest --topic wiki.recentchange --execute
	$(PY) -m wikistream.consumer --idle-timeout 10

dlq:                ## show the dead-letter topic with the error header on each message
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 --topic wiki.recentchange.dlq \
		--from-beginning --timeout-ms 5000 --property print.headers=true

crash-demo:         ## crash after writing a batch but before committing, then recover
	-$(PY) -m wikistream.consumer --crash-after-batches 1 --idle-timeout 10
	@echo "Waiting 10 s for Kafka to notice the crashed consumer (session.timeout.ms = 6 s)..."
	@sleep 10
	$(PY) -m wikistream.consumer --idle-timeout 10

test:               ## unit tests (no broker needed)
	$(PY) -m pytest

typecheck:          ## mypy --strict
	$(PY) -m mypy

e2e:                ## end-to-end test against the local broker (make up first)
	$(PY) -m pytest -m integration

clean:
	rm -rf data .pytest_cache .mypy_cache
