"""Conversão entre a linha Parquet intermediária e o contrato Kafka."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from fraudshield.common.contracts import TransactionEvent

_EVENT_METADATA = {
    "schema_version",
    "transaction_id",
    "source_transaction_dt",
    "run_id",
    "correlation_id",
}


def transaction_event_from_flat_row(
    row: Mapping[str, Any],
    *,
    replay_sent_at: datetime,
) -> TransactionEvent:
    """Valida uma linha de ``replay.parquet`` no instante de publicação."""

    missing = sorted(_EVENT_METADATA.difference(row))
    if missing:
        raise ValueError(f"metadados ausentes no replay: {', '.join(missing)}")
    features = {name: value for name, value in row.items() if name not in _EVENT_METADATA}
    return TransactionEvent(
        schema_version=row["schema_version"],
        transaction_id=str(row["transaction_id"]),
        source_transaction_dt=int(row["source_transaction_dt"]),
        replay_sent_at=replay_sent_at,
        correlation_id=str(row["correlation_id"]),
        run_id=str(row["run_id"]),
        features=features,
    )

