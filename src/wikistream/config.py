"""Settings in one place. Everything can be overridden with environment variables."""

from __future__ import annotations

import os

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")

# Main topic and dead-letter topic. Three partitions so that a consumer group
# with two or three members has something to split.
TOPIC = os.environ.get("WIKISTREAM_TOPIC", "wiki.recentchange")
DLQ_TOPIC = os.environ.get("WIKISTREAM_DLQ_TOPIC", "wiki.recentchange.dlq")
PARTITIONS = 3

CONSUMER_GROUP = os.environ.get("WIKISTREAM_GROUP", "wikistream-duckdb")

DB_PATH = os.environ.get("WIKISTREAM_DB", "data/wikistream.duckdb")

# Wikimedia EventStreams: a public Server-Sent Events feed of every change on
# every Wikimedia wiki. Their API policy asks clients to send a descriptive
# User-Agent with contact details.
STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = os.environ.get(
    "WIKISTREAM_USER_AGENT",
    "wikistream-kafka/0.1 (https://github.com/nschevokin/wikistream-kafka; nikchevokin@outlook.com)",
)
