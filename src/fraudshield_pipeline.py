"""Pipeline DuckDB para preparar os recortes temporais do FraudShield."""

from __future__ import annotations

import json
import shutil
import uuid
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any

import duckdb

from fraudshield_report import (
    null_counts,
    quote_identifier,
    schema_fingerprint,
    sha256_file,
    table_schema,
    write_json,
)
from fraudshield_settings import PrepareSettings

PIPELINE_VERSION = 1
MANIFEST_FORMAT = "fraudshield-preparation-manifest-v1"
EXPECTED_ARTIFACTS = {
    "train.parquet",
    "validation.parquet",
    "test.parquet",
    "replay.parquet",
    "test_labels.parquet",
    "quality_report.json",
    "manifest.json",
}


class PreparationError(RuntimeError):
    """Erro conhecido durante a preparação dos dados."""


class DataValidationError(PreparationError):
    """Entrada incompatível com os invariantes do pipeline."""


@dataclass(frozen=True, slots=True)
class PreparationResult:
    output: Path
    run_id: str
    report: dict[str, Any]


def _sql_string(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _scalar(connection: duckdb.DuckDBPyConnection, query: str) -> Any:
    row = connection.execute(query).fetchone()
    if row is None:
        raise PreparationError("consulta de validação não retornou resultado")
    return row[0]


def _column_names(connection: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    return [str(row[1]) for row in connection.execute(f"PRAGMA table_info('{table}')").fetchall()]


def _require_columns(table: str, available: list[str], required: set[str]) -> None:
    missing = sorted(required.difference(available))
    if missing:
        raise DataValidationError(
            f"{table} não possui as colunas obrigatórias: {', '.join(missing)}"
        )


def _load_csv(
    connection: duckdb.DuckDBPyConnection,
    table: str,
    path: Path,
) -> None:
    connection.execute(
        f"""
        CREATE TEMP TABLE {quote_identifier(table)} AS
        SELECT *
        FROM read_csv_auto(
            {_sql_string(path)},
            header = true,
            sample_size = -1
        )
        """
    )


def _validate_sources(
    connection: duckdb.DuckDBPyConnection,
    settings: PrepareSettings,
) -> dict[str, int]:
    transaction_columns = _column_names(connection, "transactions")
    identity_columns = _column_names(connection, "identity")
    _require_columns(
        "transactions",
        transaction_columns,
        {"TransactionID", "TransactionDT", "isFraud"},
    )
    _require_columns("identity", identity_columns, {"TransactionID"})

    if "is_fraud" in {name.casefold() for name in transaction_columns}:
        raise DataValidationError("transactions contém alias de rótulo proibido: is_fraud")
    identity_labels = {
        name for name in identity_columns if name.casefold() in {"isfraud", "is_fraud"}
    }
    if identity_labels:
        raise DataValidationError(
            "identity contém rótulo proibido: " + ", ".join(sorted(identity_labels))
        )
    event_metadata = {
        "schema_version",
        "transaction_id",
        "source_transaction_dt",
        "run_id",
        "correlation_id",
    }
    metadata_collisions = sorted(
        name
        for name in transaction_columns + identity_columns
        if name.casefold() in event_metadata
    )
    if metadata_collisions:
        raise DataValidationError(
            "colunas de entrada colidem com metadados do replay: "
            + ", ".join(metadata_collisions)
        )

    checks = {
        "transaction_rows": int(_scalar(connection, "SELECT COUNT(*) FROM transactions")),
        "identity_rows": int(_scalar(connection, "SELECT COUNT(*) FROM identity")),
        "null_transaction_ids": int(
            _scalar(connection, "SELECT COUNT(*) FROM transactions WHERE TransactionID IS NULL")
        ),
        "null_transaction_times": int(
            _scalar(connection, "SELECT COUNT(*) FROM transactions WHERE TransactionDT IS NULL")
        ),
        "null_identity_ids": int(
            _scalar(connection, "SELECT COUNT(*) FROM identity WHERE TransactionID IS NULL")
        ),
        "invalid_labels": int(
            _scalar(
                connection,
                "SELECT COUNT(*) FROM transactions "
                "WHERE TRY_CAST(isFraud AS DOUBLE) IS NULL "
                "OR TRY_CAST(isFraud AS DOUBLE) NOT IN (0.0, 1.0)",
            )
        ),
        "duplicate_transaction_ids": int(
            _scalar(
                connection,
                "SELECT COUNT(*) FROM ("
                "SELECT TransactionID FROM transactions "
                "GROUP BY TransactionID HAVING COUNT(*) > 1)",
            )
        ),
        "duplicate_identity_ids": int(
            _scalar(
                connection,
                "SELECT COUNT(*) FROM ("
                "SELECT TransactionID FROM identity "
                "GROUP BY TransactionID HAVING COUNT(*) > 1)",
            )
        ),
    }

    failure_messages = {
        "null_transaction_ids": "TransactionID nulo em transactions",
        "null_transaction_times": "TransactionDT nulo em transactions",
        "null_identity_ids": "TransactionID nulo em identity",
        "invalid_labels": "isFraud nulo ou fora de {0, 1}",
        "duplicate_transaction_ids": "TransactionID duplicado em transactions",
        "duplicate_identity_ids": "TransactionID duplicado em identity",
    }
    failures = [message for name, message in failure_messages.items() if checks[name] > 0]
    if failures:
        raise DataValidationError("; ".join(failures))

    if checks["transaction_rows"] < settings.required_rows:
        raise DataValidationError(
            "linhas insuficientes em transactions: "
            f"necessárias={settings.required_rows}, encontradas={checks['transaction_rows']}"
        )
    return checks


def _create_joined_tables(connection: duckdb.DuckDBPyConnection) -> dict[str, int | float]:
    connection.execute(
        """
        CREATE TEMP TABLE joined AS
        SELECT t.*, i.* EXCLUDE (TransactionID)
        FROM transactions AS t
        LEFT JOIN identity AS i USING (TransactionID)
        """
    )
    connection.execute(
        """
        CREATE TEMP TABLE ordered AS
        SELECT
            *,
            ROW_NUMBER() OVER (ORDER BY TransactionDT, TransactionID) AS __row_number
        FROM joined
        """
    )

    joined_rows = int(_scalar(connection, "SELECT COUNT(*) FROM joined"))
    matched_rows = int(
        _scalar(
            connection,
            "SELECT COUNT(*) FROM transactions t INNER JOIN identity i USING (TransactionID)",
        )
    )
    orphan_rows = int(
        _scalar(
            connection,
            "SELECT COUNT(*) FROM identity i "
            "LEFT JOIN transactions t USING (TransactionID) "
            "WHERE t.TransactionID IS NULL",
        )
    )
    coverage = matched_rows / joined_rows if joined_rows else 0.0
    return {
        "joined_rows": joined_rows,
        "matched_identity_rows": matched_rows,
        "unmatched_transaction_rows": joined_rows - matched_rows,
        "orphan_identity_rows": orphan_rows,
        "identity_coverage": coverage,
    }


def _split_ranges(settings: PrepareSettings) -> dict[str, tuple[int, int]]:
    train_end = settings.train_rows
    validation_end = train_end + settings.validation_rows
    test_end = validation_end + settings.test_rows
    return {
        "train": (1, train_end),
        "validation": (train_end + 1, validation_end),
        "test": (validation_end + 1, test_end),
    }


def _split_report(
    connection: duckdb.DuckDBPyConnection,
    ranges: dict[str, tuple[int, int]],
) -> dict[str, dict[str, int | float]]:
    result: dict[str, dict[str, int | float]] = {}
    for name, (start, end) in ranges.items():
        row = connection.execute(
            """
            SELECT
                COUNT(*),
                SUM(CASE WHEN TRY_CAST(isFraud AS INTEGER) = 1 THEN 1 ELSE 0 END),
                AVG(TRY_CAST(isFraud AS DOUBLE)),
                MIN(TransactionDT),
                MAX(TransactionDT)
            FROM ordered
            WHERE __row_number BETWEEN ? AND ?
            """,
            [start, end],
        ).fetchone()
        if row is None:
            raise PreparationError(f"não foi possível medir o recorte {name}")
        result[name] = {
            "rows": int(row[0]),
            "fraud_count": int(row[1] or 0),
            "fraud_rate": float(row[2] or 0.0),
            "transaction_dt_min": int(row[3]),
            "transaction_dt_max": int(row[4]),
        }
    return result


def _copy_parquet(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    destination: Path,
) -> None:
    connection.execute(
        f"COPY ({query}) TO {_sql_string(destination)} "
        "(FORMAT PARQUET, COMPRESSION ZSTD)"
    )


def _write_parquet_outputs(
    connection: duckdb.DuckDBPyConnection,
    staging: Path,
    ranges: dict[str, tuple[int, int]],
    run_id: str,
) -> dict[str, int]:
    row_counts: dict[str, int] = {}
    for name, (start, end) in ranges.items():
        query = (
            "SELECT * EXCLUDE (__row_number) FROM ordered "
            f"WHERE __row_number BETWEEN {start} AND {end} "
            "ORDER BY TransactionDT, TransactionID"
        )
        _copy_parquet(connection, query, staging / f"{name}.parquet")
        row_counts[f"{name}.parquet"] = end - start + 1

    test_start, test_end = ranges["test"]
    labels_query = (
        "SELECT CAST(TransactionID AS VARCHAR) AS transaction_id, "
        "CAST(isFraud AS UTINYINT) AS isFraud FROM ordered "
        f"WHERE __row_number BETWEEN {test_start} AND {test_end} "
        "ORDER BY TransactionDT, TransactionID"
    )
    _copy_parquet(connection, labels_query, staging / "test_labels.parquet")
    row_counts["test_labels.parquet"] = test_end - test_start + 1

    run_literal = _sql_string(run_id)
    correlation_prefix = _sql_string(f"corr-{run_id}-")
    replay_query = (
        "SELECT 1::INTEGER AS schema_version, "
        "CAST(TransactionID AS VARCHAR) AS transaction_id, "
        "CAST(TransactionDT AS BIGINT) AS source_transaction_dt, "
        f"{run_literal} AS run_id, "
        f"{correlation_prefix} || CAST(TransactionID AS VARCHAR) AS correlation_id, "
        "* EXCLUDE (TransactionID, TransactionDT, isFraud, __row_number) "
        "FROM ordered "
        f"WHERE __row_number BETWEEN {test_start} AND {test_end} "
        "ORDER BY TransactionDT, TransactionID"
    )
    _copy_parquet(connection, replay_query, staging / "replay.parquet")
    row_counts["replay.parquet"] = test_end - test_start + 1
    return row_counts


def _build_run_id(settings: PrepareSettings, input_hashes: dict[str, str]) -> str:
    payload = {
        "pipeline_version": PIPELINE_VERSION,
        "inputs": input_hashes,
        "splits": {
            "train_rows": settings.train_rows,
            "validation_rows": settings.validation_rows,
            "test_rows": settings.test_rows,
        },
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return "prep-" + sha256(canonical.encode("utf-8")).hexdigest()[:16]


def _artifact_manifest(staging: Path, row_counts: dict[str, int]) -> dict[str, Any]:
    artifacts: dict[str, dict[str, Any]] = {}
    for path in sorted(staging.iterdir(), key=lambda item: item.name):
        if not path.is_file() or path.name == "manifest.json":
            continue
        item: dict[str, Any] = {
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }
        if path.name in row_counts:
            item["rows"] = row_counts[path.name]
        artifacts[path.name] = item
    return artifacts


def _validate_managed_output(output: Path) -> None:
    if not output.is_dir():
        raise PreparationError(f"output existe e não é um diretório: {output}")
    entries = {path.name for path in output.iterdir()}
    if entries != EXPECTED_ARTIFACTS:
        raise PreparationError(
            "output existente não contém exatamente os artefatos gerenciados; "
            f"esperado={sorted(EXPECTED_ARTIFACTS)}, atual={sorted(entries)}"
        )
    try:
        manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise PreparationError("manifesto existente é ilegível ou inválido") from error
    if manifest.get("format") != MANIFEST_FORMAT:
        raise PreparationError("manifesto existente não pertence ao FraudShield")
    if manifest.get("pipeline_version") != PIPELINE_VERSION:
        raise PreparationError("versão do manifesto existente não é compatível")
    artifact_names = set(manifest.get("artifacts", {}))
    expected_manifest_entries = EXPECTED_ARTIFACTS - {"manifest.json"}
    if artifact_names != expected_manifest_entries:
        raise PreparationError("manifesto existente não descreve todos os artefatos esperados")


def _recover_managed_backups(output: Path) -> None:
    backups = sorted(output.parent.glob(f".{output.name}.backup-*"))
    if not backups:
        return
    for backup in backups:
        _validate_managed_output(backup)
    if output.exists():
        for backup in backups:
            shutil.rmtree(backup, ignore_errors=True)
        return
    if len(backups) != 1:
        raise PreparationError(
            "mais de um backup pendente foi encontrado; recuperação automática recusada"
        )
    backups[0].rename(output)


def _commit_staging(staging: Path, output: Path) -> None:
    backup = output.parent / f".{output.name}.backup-{uuid.uuid4().hex}"
    had_previous_output = output.exists()
    if had_previous_output:
        _validate_managed_output(output)
        output.rename(backup)

    try:
        staging.rename(output)
    except Exception:
        if had_previous_output and backup.exists() and not output.exists():
            backup.rename(output)
        raise
    else:
        if backup.exists():
            shutil.rmtree(backup, ignore_errors=True)


def run_preparation(settings: PrepareSettings) -> PreparationResult:
    """Executa validação, recortes temporais e publicação atômica dos artefatos."""

    for input_path in (settings.transactions, settings.identity):
        if not input_path.is_file():
            raise PreparationError(f"arquivo de entrada não encontrado: {input_path}")

    output_parent = settings.output.parent
    output_parent.mkdir(parents=True, exist_ok=True)
    _recover_managed_backups(settings.output)
    staging = output_parent / f".{settings.output.name}.staging-{uuid.uuid4().hex}"
    staging.mkdir()
    connection: duckdb.DuckDBPyConnection | None = None

    try:
        input_hashes = {
            "transactions_sha256": sha256_file(settings.transactions),
            "identity_sha256": sha256_file(settings.identity),
        }
        run_id = _build_run_id(settings, input_hashes)

        connection = duckdb.connect(database=":memory:")
        connection.execute(f"SET threads = {settings.threads}")
        connection.execute(f"SET memory_limit = {_sql_string(settings.memory_limit)}")
        temp_directory = staging / "duckdb_tmp"
        temp_directory.mkdir()
        connection.execute(f"SET temp_directory = {_sql_string(temp_directory)}")

        _load_csv(connection, "transactions", settings.transactions)
        _load_csv(connection, "identity", settings.identity)
        source_checks = _validate_sources(connection, settings)
        join_checks = _create_joined_tables(connection)
        if join_checks["joined_rows"] != source_checks["transaction_rows"]:
            raise DataValidationError("o LEFT JOIN alterou a quantidade de transações")

        ranges = _split_ranges(settings)
        splits = _split_report(connection, ranges)
        row_counts = _write_parquet_outputs(connection, staging, ranges, run_id)

        transaction_columns = _column_names(connection, "transactions")
        identity_columns = _column_names(connection, "identity")
        joined_schema = table_schema(connection, "joined")
        excluded_from_features = {"TransactionID", "TransactionDT", "isFraud"}
        feature_schema = [
            column for column in joined_schema if column["name"] not in excluded_from_features
        ]
        report: dict[str, Any] = {
            "pipeline_version": PIPELINE_VERSION,
            "run_id": run_id,
            "input": {
                "transaction_rows": source_checks["transaction_rows"],
                "identity_rows": source_checks["identity_rows"],
                **input_hashes,
            },
            "quality": {
                "duplicate_transaction_ids": source_checks["duplicate_transaction_ids"],
                "duplicate_identity_ids": source_checks["duplicate_identity_ids"],
                "null_transaction_ids": source_checks["null_transaction_ids"],
                "null_transaction_times": source_checks["null_transaction_times"],
                "null_identity_ids": source_checks["null_identity_ids"],
                "invalid_labels": source_checks["invalid_labels"],
                "transaction_nulls": null_counts(
                    connection, "transactions", transaction_columns
                ),
                "identity_nulls": null_counts(connection, "identity", identity_columns),
            },
            "join": join_checks,
            "splits": splits,
            "ignored_transaction_rows": source_checks["transaction_rows"]
            - settings.required_rows,
            "schema": {
                "joined": {
                    "columns": joined_schema,
                    "fingerprint": schema_fingerprint(joined_schema),
                },
                "features": {
                    "columns": feature_schema,
                    "fingerprint": schema_fingerprint(feature_schema),
                },
            },
        }
        write_json(staging / "quality_report.json", report)

        manifest = {
            "format": MANIFEST_FORMAT,
            "pipeline_version": PIPELINE_VERSION,
            "run_id": run_id,
            "configuration": {
                "train_rows": settings.train_rows,
                "validation_rows": settings.validation_rows,
                "test_rows": settings.test_rows,
                "threads": settings.threads,
                "memory_limit": settings.memory_limit,
            },
            "joined_schema_fingerprint": report["schema"]["joined"]["fingerprint"],
            "feature_schema_fingerprint": report["schema"]["features"]["fingerprint"],
            "artifacts": _artifact_manifest(staging, row_counts),
        }
        write_json(staging / "manifest.json", manifest)

        shutil.rmtree(temp_directory, ignore_errors=True)
        actual_artifacts = {path.name for path in staging.iterdir() if path.is_file()}
        if actual_artifacts != EXPECTED_ARTIFACTS:
            raise PreparationError(
                "conjunto inesperado de artefatos: "
                f"esperado={sorted(EXPECTED_ARTIFACTS)}, atual={sorted(actual_artifacts)}"
            )

        connection.close()
        connection = None
        _commit_staging(staging, settings.output)
        return PreparationResult(output=settings.output, run_id=run_id, report=report)
    except Exception:
        if connection is not None:
            connection.close()
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)
        raise

