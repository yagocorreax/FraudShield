"""Funções de auditoria e manifesto do pipeline de preparação."""

from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path
from typing import Any

import duckdb


def quote_identifier(name: str) -> str:
    """Escapa um identificador para SQL DuckDB."""

    return '"' + name.replace('"', '""') + '"'


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Calcula SHA-256 sem carregar o arquivo inteiro na memória."""

    digest = sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def table_schema(connection: duckdb.DuckDBPyConnection, table: str) -> list[dict[str, Any]]:
    rows = connection.execute(f"PRAGMA table_info({quote_identifier(table)})").fetchall()
    return [
        {
            "position": int(row[0]),
            "name": str(row[1]),
            "type": str(row[2]),
            "nullable": not bool(row[3]),
        }
        for row in rows
    ]


def schema_fingerprint(schema: list[dict[str, Any]]) -> str:
    payload = json.dumps(schema, ensure_ascii=False, separators=(",", ":"))
    return sha256(payload.encode("utf-8")).hexdigest()


def null_counts(
    connection: duckdb.DuckDBPyConnection,
    table: str,
    column_names: list[str],
) -> dict[str, int]:
    expressions = [
        f"SUM(CASE WHEN {quote_identifier(name)} IS NULL THEN 1 ELSE 0 END)"
        for name in column_names
    ]
    values = connection.execute(
        f"SELECT {', '.join(expressions)} FROM {quote_identifier(table)}"
    ).fetchone()
    if values is None:
        return {name: 0 for name in column_names}
    return {name: int(value or 0) for name, value in zip(column_names, values, strict=True)}


def write_json(path: Path, value: dict[str, Any]) -> None:
    serialized = json.dumps(
        value,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
        allow_nan=False,
    )
    path.write_text(serialized + "\n", encoding="utf-8", newline="\n")

