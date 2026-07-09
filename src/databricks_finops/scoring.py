from __future__ import annotations


SCORE_WEIGHTS: dict[str, float] = {
    "cost": 0.45,
    "waste": 0.25,
    "reliability": 0.15,
    "tagging": 0.10,
    "frequency": 0.05,
}


def clamp_score(value: float) -> float:
    return max(0.0, min(100.0, float(value)))


def priority_score_sql_expr(
    cost: str = "cost_score",
    waste: str = "waste_score",
    reliability: str = "reliability_score",
    tagging: str = "tagging_score",
    frequency: str = "frequency_score",
) -> str:
    return (
        f"{cost} * {SCORE_WEIGHTS['cost']:.2f}\n"
        f"                    + {waste} * {SCORE_WEIGHTS['waste']:.2f}\n"
        f"                    + {reliability} * {SCORE_WEIGHTS['reliability']:.2f}\n"
        f"                    + {tagging} * {SCORE_WEIGHTS['tagging']:.2f}\n"
        f"                    + {frequency} * {SCORE_WEIGHTS['frequency']:.2f}"
    )


def weighted_priority_score(
    cost_score: float,
    waste_score: float,
    reliability_score: float,
    tagging_score: float,
    frequency_score: float,
) -> float:
    """Transparent 0-100 priority score used by SQL and tests.

    Cost dominates, but utilization, reliability, tagging/attribution risk, and
    workload frequency all contribute so the backlog is useful across business
    and platform review conversations.
    """

    return round(
        clamp_score(cost_score) * SCORE_WEIGHTS["cost"]
        + clamp_score(waste_score) * SCORE_WEIGHTS["waste"]
        + clamp_score(reliability_score) * SCORE_WEIGHTS["reliability"]
        + clamp_score(tagging_score) * SCORE_WEIGHTS["tagging"]
        + clamp_score(frequency_score) * SCORE_WEIGHTS["frequency"],
        6,
    )
