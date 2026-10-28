"""Interface de linha de comando para preparar o dataset."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from pathlib import Path

from pydantic import ValidationError

from fraudshield.common.settings import PrepareSettings
from fraudshield.prepare.pipeline import PreparationError, run_preparation


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="fraudshield-prepare",
        description="Prepara recortes temporais e artefatos auditáveis do IEEE-CIS.",
    )
    parser.add_argument("--transactions", type=Path, required=True)
    parser.add_argument("--identity", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--train-rows", type=int, default=120_000)
    parser.add_argument("--validation-rows", type=int, default=30_000)
    parser.add_argument("--test-rows", type=int, default=50_000)
    parser.add_argument(
        "--threads",
        type=int,
        default=int(os.getenv("FRAUDSHIELD_DUCKDB_THREADS", "2")),
    )
    parser.add_argument(
        "--memory-limit",
        default=os.getenv("FRAUDSHIELD_DUCKDB_MEMORY_LIMIT", "1GB"),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        settings = PrepareSettings(
            transactions=arguments.transactions,
            identity=arguments.identity,
            output=arguments.output,
            train_rows=arguments.train_rows,
            validation_rows=arguments.validation_rows,
            test_rows=arguments.test_rows,
            threads=arguments.threads,
            memory_limit=arguments.memory_limit,
        )
        result = run_preparation(settings)
    except (PreparationError, ValidationError) as error:
        print(f"Erro de preparação: {error}", file=sys.stderr)
        return 2

    summary = {
        "output": str(result.output),
        "run_id": result.run_id,
        "splits": result.report["splits"],
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

