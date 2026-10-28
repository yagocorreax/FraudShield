from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from fraudshield.common.contracts import Decision, FraudDecision, TransactionEvent


def test_transaction_event_accepts_features_without_label() -> None:
    event = TransactionEvent(
        transaction_id="1001",
        source_transaction_dt=10,
        replay_sent_at=datetime(2026, 10, 27, 18, 0, tzinfo=UTC),
        correlation_id="corr-run-1001",
        run_id="run-1",
        features={"TransactionAmt": 10.0, "ProductCD": "W"},
    )

    assert event.schema_version == 1
    assert event.transaction_id == "1001"


@pytest.mark.parametrize("label", ["isFraud", "is_fraud"])
def test_transaction_event_rejects_label_in_features(label: str) -> None:
    with pytest.raises(ValidationError, match="rótulo proibido"):
        TransactionEvent(
            transaction_id="1001",
            source_transaction_dt=10,
            replay_sent_at=datetime(2026, 10, 27, 18, 0, tzinfo=UTC),
            correlation_id="corr-run-1001",
            run_id="run-1",
            features={"TransactionAmt": 10.0, label: 0},
        )


def test_contracts_reject_naive_timestamps() -> None:
    with pytest.raises(ValidationError, match="fuso horário"):
        TransactionEvent(
            transaction_id="1001",
            source_transaction_dt=10,
            replay_sent_at=datetime(2026, 10, 27, 18, 0),
            correlation_id="corr-run-1001",
            run_id="run-1",
            features={"TransactionAmt": 10.0},
        )


def test_contract_normalizes_aware_timestamp_to_utc() -> None:
    event = TransactionEvent(
        transaction_id="1001",
        source_transaction_dt=10,
        replay_sent_at=datetime(
            2026,
            10,
            27,
            18,
            0,
            tzinfo=timezone(timedelta(hours=-3)),
        ),
        correlation_id="corr-run-1001",
        run_id="run-1",
        features={"TransactionAmt": 10.0},
    )

    assert event.replay_sent_at == datetime(2026, 10, 27, 21, 0, tzinfo=UTC)


def test_fraud_decision_has_stable_idempotency_key() -> None:
    base = {
        "transaction_id": "1001",
        "correlation_id": "corr-run-1001",
        "run_id": "run-1",
        "model_version": "fraud-xgb-v1",
        "fraud_score": 0.91,
        "decision": Decision.REVIEW,
        "processed_at": datetime(2026, 10, 27, 18, 0, tzinfo=UTC),
    }
    first = FraudDecision(**base)
    second = FraudDecision(**base)
    another_run = FraudDecision(**{**base, "run_id": "run-2"})

    assert first.decision_id == second.decision_id
    assert first.decision_id != another_run.decision_id


@pytest.mark.parametrize("score", [-0.01, 1.01])
def test_fraud_decision_rejects_score_outside_probability(score: float) -> None:
    with pytest.raises(ValidationError):
        FraudDecision(
            transaction_id="1001",
            correlation_id="corr-run-1001",
            run_id="run-1",
            model_version="fraud-xgb-v1",
            fraud_score=score,
            decision=Decision.REVIEW,
            processed_at=datetime(2026, 10, 27, 18, 0, tzinfo=UTC),
        )

