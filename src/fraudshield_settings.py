"""Configurações validadas para os comandos do FraudShield."""

from __future__ import annotations

import re
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_MEMORY_LIMIT_PATTERN = re.compile(r"^[1-9]\d*(?:MB|GB)$", re.IGNORECASE)


class PrepareSettings(BaseModel):
    """Parâmetros do pipeline de preparação temporal."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    transactions: Path
    identity: Path
    output: Path
    train_rows: int = Field(default=120_000, gt=0)
    validation_rows: int = Field(default=30_000, gt=0)
    test_rows: int = Field(default=50_000, gt=0)
    threads: int = Field(default=2, ge=1, le=4)
    memory_limit: str = "1GB"

    @field_validator("transactions", "identity", "output")
    @classmethod
    def normalize_path(cls, value: Path) -> Path:
        return value.expanduser().resolve()

    @field_validator("memory_limit")
    @classmethod
    def validate_memory_limit(cls, value: str) -> str:
        normalized = value.upper()
        if not _MEMORY_LIMIT_PATTERN.fullmatch(normalized):
            raise ValueError("use um limite como 512MB, 1GB ou 2GB")
        return normalized

    @model_validator(mode="after")
    def validate_paths(self) -> PrepareSettings:
        if self.transactions == self.identity:
            raise ValueError("transactions e identity precisam ser arquivos diferentes")
        if self.output in {self.transactions, self.identity}:
            raise ValueError("output não pode sobrescrever um arquivo de entrada")
        return self

    @property
    def required_rows(self) -> int:
        return self.train_rows + self.validation_rows + self.test_rows

