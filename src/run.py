#!/usr/bin/env python3
"""Orchestrator: bronze -> silver -> gold. Prints row counts per layer.

Usage:
    python data/generate.py   # raw extracts (seeded)
    python -m src.run         # full medallion pipeline (needs Java + PySpark)

On Databricks, the same stages run as notebooks/01..03 (or a Delta Live
Tables pipeline); this runner is the local-dev equivalent.
"""
from __future__ import annotations

from src import bronze, gold, silver


def main() -> dict:
    spark = bronze.get_spark("retail-lakehouse")
    try:
        print("=" * 60)
        print("BRONZE — raw extracts -> Delta (schema enforcement)")
        print("=" * 60)
        bronze_counts = bronze.main(spark)

        print("=" * 60)
        print("SILVER — clean/conform: CDC merge + SCD Type 2")
        print("=" * 60)
        silver_counts = silver.main(spark)

        print("=" * 60)
        print("GOLD — aggregate marts + parquet export")
        print("=" * 60)
        gold_counts = gold.main(spark)

        print("=" * 60)
        print("PIPELINE COMPLETE")
        print("=" * 60)
        return {"bronze": bronze_counts, "silver": silver_counts, "gold": gold_counts}
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
