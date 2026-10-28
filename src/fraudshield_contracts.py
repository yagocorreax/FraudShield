"""Contratos versionados dos eventos principais do FraudShield."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from hashlib import sha256
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


def _require_non_empty(value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError("o valor não pode ser vazio")
    return normalized


def _normalize_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("o timestamp precisa conter fuso horário")
    return value.astimezone(UTC)


class Decision(StrEnum):

    APPROVE = "APPROVE"
    REVIEW = "REVIEW"
    BLOCK = "BLOCK"


class TransactionEvent(BaseModel):
    """Evento enviado ao tópico ``transactions.raw``."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = 1
    transaction_id: str
    source_transaction_dt: int = Field(ge=0)
    replay_sent_at: datetime
    correlation_id: str
    run_id: str
    features: dict[str, Any]

    _non_empty_ids = field_validator(
        "transaction_id", "correlation_id", "run_id"
    )(_require_non_empty)
    _aware_timestamp = field_validator("replay_sent_at")(_normalize_utc)

    @field_validator("features")
    @classmethod
    def reject_labels(cls, value: dict[str, Any]) -> dict[str, Any]:
        forbidden = {"isFraud", "is_fraud"}.intersection(value)
        if forbidden:
            names = ", ".join(sorted(forbidden))
            raise ValueError(f"features contém rótulo proibido: {names}")
        if not value:
            raise ValueError("features não pode ser vazio")
        return value


class FraudDecision(BaseModel):
    """Decisão enviada ao tópico ``fraud.decisions``."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = 1
    transaction_id: str
    correlation_id: str
    run_id: str
    model_version: str
    fraud_score: float = Field(ge=0.0, le=1.0)
    decision: Decision
    processed_at: datetime

    _non_empty_ids = field_validator(
        "transaction_id", "correlation_id", "run_id", "model_version"
    )(_require_non_empty)
    _aware_timestamp = field_validator("processed_at")(_normalize_utc)

    @property
    def decision_id(self) -> str:
        """Chave idempotente da decisão dentro de uma execução do modelo."""

        raw_key = f"{self.run_id}\x1f{self.transaction_id}\x1f{self.model_version}"
        return sha256(raw_key.encode("utf-8")).hexdigest()

