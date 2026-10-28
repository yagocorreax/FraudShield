from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from fraudshield_events import transaction_event_from_flat_row
from fraudshield_pipeline import DataValidationError, PreparationError, run_preparation
from fraudshield_report import sha256_file
from fraudshield_settings import PrepareSettings

FIXTURES = Path(__file__).parents[1] / "fixtures"


def _settings(output: Path, transactions: Path | None = None) -> PrepareSettings:
    return PrepareSettings(
        transactions=transactions or FIXTURES / "transactions.csv",
        identity=FIXTURES / "identity.csv",
        output=output,
        train_rows=6,
        validation_rows=3,
        test_rows=3,
        threads=1,
        memory_limit="512MB",
    )


def _ids(path: Path) -> list[int]:
    connection = duckdb.connect(database=":memory:")
    try:
        rows = connection.execute(
            "SELECT TransactionID FROM read_parquet(?) ORDER BY TransactionDT, TransactionID",
            [str(path)],
        ).fetchall()
    finally:
        connection.close()
    return [int(row[0]) for row in rows]


def test_pipeline_builds_temporal_splits_and_auditable_outputs(tmp_path: Path) -> None:
    output = tmp_path / "prepared"
    result = run_preparation(_settings(output))

    assert result.output == output.resolve()
    assert {path.name for path in output.iterdir()} == {
        "train.parquet",
        "validation.parquet",
        "test.parquet",
        "replay.parquet",
        "test_labels.parquet",
        "quality_report.json",
        "manifest.json",
    }
    assert _ids(output / "train.parquet") == [1001, 1002, 1003, 1004, 1005, 1006]
    assert _ids(output / "validation.parquet") == [1007, 1008, 1009]
    assert _ids(output / "test.parquet") == [1010, 1011, 1012]

    report = json.loads((output / "quality_report.json").read_text(encoding="utf-8"))
    assert report["input"]["transaction_rows"] == 12
    assert report["input"]["identity_rows"] == 7
    assert report["join"] == {
        "identity_coverage": 0.5,
        "joined_rows": 12,
        "matched_identity_rows": 6,
        "orphan_identity_rows": 1,
        "unmatched_transaction_rows": 6,
    }
    assert {name: values["rows"] for name, values in report["splits"].items()} == {
        "train": 6,
        "validation": 3,
        "test": 3,
    }
    assert {name: values["fraud_count"] for name, values in report["splits"].items()} == {
        "train": 2,
        "validation": 1,
        "test": 1,
    }
    assert report["splits"]["train"]["fraud_rate"] == pytest.approx(2 / 6)
    assert report["splits"]["validation"]["fraud_rate"] == pytest.approx(1 / 3)
    assert report["splits"]["test"]["fraud_rate"] == pytest.approx(1 / 3)

    connection = duckdb.connect(database=":memory:")
    try:
        replay_description = connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(output / "replay.parquet")]
        ).fetchall()
        replay_columns = {row[0] for row in replay_description}
        replay_ids = [
            row[0]
            for row in connection.execute(
                "SELECT transaction_id FROM read_parquet(?) ORDER BY source_transaction_dt",
                [str(output / "replay.parquet")],
            ).fetchall()
        ]
        label_rows = connection.execute(
            "SELECT transaction_id, isFraud FROM read_parquet(?) ORDER BY transaction_id",
            [str(output / "test_labels.parquet")],
        ).fetchall()
        replay_cursor = connection.execute(
            "SELECT * FROM read_parquet(?) ORDER BY source_transaction_dt LIMIT 1",
            [str(output / "replay.parquet")],
        )
        replay_values = replay_cursor.fetchone()
        replay_names = [item[0] for item in replay_cursor.description]
    finally:
        connection.close()

    assert "isFraud" not in replay_columns
    assert "is_fraud" not in replay_columns
    assert {
        "schema_version",
        "transaction_id",
        "source_transaction_dt",
        "run_id",
        "correlation_id",
    }.issubset(replay_columns)
    assert replay_ids == ["1010", "1011", "1012"]
    assert label_rows == [("1010", 0), ("1011", 1), ("1012", 0)]

    assert replay_values is not None
    event = transaction_event_from_flat_row(
        dict(zip(replay_names, replay_values, strict=True)),
        replay_sent_at=datetime(2026, 10, 27, 21, 0, tzinfo=UTC),
    )
    assert event.transaction_id == "1010"
    assert "isFraud" not in event.features
    assert "TransactionAmt" in event.features

    manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    for name, metadata in manifest["artifacts"].items():
        assert sha256_file(output / name) == metadata["sha256"]
    feature_names = {column["name"] for column in report["schema"]["features"]["columns"]}
    assert not {"TransactionID", "TransactionDT", "isFraud"}.intersection(feature_names)


def test_reexecution_is_deterministic_and_replaces_only_managed_output(tmp_path: Path) -> None:
    output = tmp_path / "prepared"
    first = run_preparation(_settings(output))
    first_manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))

    second = run_preparation(_settings(output))
    second_manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))

    assert first.run_id == second.run_id
    assert first_manifest == second_manifest


def test_duplicate_transaction_fails_without_publishing_partial_output(tmp_path: Path) -> None:
    source = (FIXTURES / "transactions.csv").read_text(encoding="utf-8")
    duplicate_source = tmp_path / "transactions_with_duplicate.csv"
    duplicate_source.write_text(source + "1001,120,999.00,W,0\n", encoding="utf-8")
    output = tmp_path / "prepared"

    with pytest.raises(DataValidationError, match="duplicado"):
        run_preparation(_settings(output, transactions=duplicate_source))

    assert not output.exists()
    assert not list(tmp_path.glob(".prepared.staging-*"))


@pytest.mark.parametrize("invalid_label", ["fraud", "0.5"])
def test_invalid_label_fails_with_validation_error(
    tmp_path: Path,
    invalid_label: str,
) -> None:
    source = (FIXTURES / "transactions.csv").read_text(encoding="utf-8")
    invalid_source = tmp_path / "transactions_with_invalid_label.csv"
    invalid_source.write_text(
        source.replace("1001,10,10.00,W,0", f"1001,10,10.00,W,{invalid_label}"),
        encoding="utf-8",
    )

    with pytest.raises(DataValidationError, match="isFraud"):
        run_preparation(_settings(tmp_path / "prepared", transactions=invalid_source))


def test_replay_rejects_label_alias_from_source(tmp_path: Path) -> None:
    with pytest.raises(DataValidationError, match="alias de rótulo"):
        run_preparation(
            _settings(
                tmp_path / "prepared",
                transactions=FIXTURES / "transactions_with_alias.csv",
            )
        )


def test_rerun_refuses_extra_file_in_managed_output(tmp_path: Path) -> None:
    output = tmp_path / "prepared"
    run_preparation(_settings(output))
    note = output / "notes.txt"
    note.write_text("arquivo do usuário", encoding="utf-8")

    with pytest.raises(PreparationError, match="exatamente os artefatos"):
        run_preparation(_settings(output))

    assert note.read_text(encoding="utf-8") == "arquivo do usuário"


def test_next_run_recovers_interrupted_managed_backup(tmp_path: Path) -> None:
    output = tmp_path / "prepared"
    first = run_preparation(_settings(output))
    interrupted_backup = tmp_path / ".prepared.backup-simulated-crash"
    output.rename(interrupted_backup)

    recovered = run_preparation(_settings(output))

    assert recovered.run_id == first.run_id
    assert output.is_dir()
    assert not interrupted_backup.exists()

