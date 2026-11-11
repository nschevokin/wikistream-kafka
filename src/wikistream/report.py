"""Print a short summary of what landed in DuckDB."""

from __future__ import annotations

from wikistream import config
from wikistream.sink import DuckDBSink


def main() -> None:
    sink = DuckDBSink(config.DB_PATH)
    total = sink.count()
    print(f"Edits stored: {total}")
    if not total:
        print("Nothing yet: run `make produce` and `make consume` first.")
        sink.close()
        return

    ((first, last, bot_share, wikis),) = sink.query(
        "SELECT min(event_ts), max(event_ts), round(avg(is_bot::INT), 3), count(DISTINCT wiki) FROM edits"
    )
    print(f"Time span:    {first} -> {last}")
    print(f"Wikis:        {wikis}")
    print(f"Bot share:    {bot_share}")

    print("\nTop 10 wikis by edits:")
    for wiki, edits, share in sink.query(
        "SELECT wiki, count(*) AS n, round(avg(is_bot::INT), 2) FROM edits GROUP BY wiki ORDER BY n DESC LIMIT 10"
    ):
        print(f"  {wiki:<20} {edits:>7}   bot share {share}")

    print("\nRows per Kafka partition (keys spread wikis across partitions):")
    for partition, n, distinct_wikis in sink.query(
        "SELECT kafka_partition, count(*), count(DISTINCT wiki) FROM edits GROUP BY 1 ORDER BY 1"
    ):
        print(f"  partition {partition}: {n:>7} rows from {distinct_wikis} wikis")
    sink.close()


if __name__ == "__main__":
    main()
