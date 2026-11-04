"""CLI: corre el mismo pipeline que el DAG, sin Airflow (útil para debug y CI).

python -m ml_pipeline init-db
python -m ml_pipeline run --date 2026-11-03
python -m ml_pipeline backfill --start 2026-10-20 --end 2026-11-03
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta

from ml_pipeline import bronze, db, gold, silver
from ml_pipeline.config import get_settings
from ml_pipeline.quality import checks


def run_day(run_date: date, run_id: str | None = None) -> None:
    settings = get_settings()
    run_id = run_id or f"cli__{run_date.isoformat()}"
    db.run_migrations(settings.warehouse_dsn)
    for site in settings.sites:
        bronze.extract_to_bronze(site, run_date, run_id, settings)
    bronze.load_fx_rates(run_date, run_id, settings)
    for site in settings.sites:
        silver.build_silver(site, run_date, run_id, settings)
    checks.run_checks("silver", run_date, run_id, settings)
    gold.build_gold(run_date, run_id, settings)
    checks.run_checks("gold", run_date, run_id, settings)
    logging.getLogger(__name__).info("Pipeline OK para %s", run_date)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    parser = argparse.ArgumentParser(prog="ml_pipeline")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init-db")
    run = sub.add_parser("run")
    run.add_argument("--date", type=date.fromisoformat, default=date.today())
    bf = sub.add_parser("backfill")
    bf.add_argument("--start", type=date.fromisoformat, required=True)
    bf.add_argument("--end", type=date.fromisoformat, required=True)
    args = parser.parse_args()

    if args.cmd == "init-db":
        db.run_migrations(get_settings().warehouse_dsn)
    elif args.cmd == "run":
        run_day(args.date)
    else:
        day = args.start
        while day <= args.end:
            run_day(day)
            day += timedelta(days=1)


if __name__ == "__main__":
    main()
