from __future__ import annotations

from typing import TypedDict


class MicrogenresDocument(TypedDict):
    confidence: float
    microgenre_scores: dict[str, float]
    rationale: str


def coerce_microgenres_document(value: object) -> MicrogenresDocument | None:
    """Validate and normalize the compact Redis microgenre document shape."""
    if not isinstance(value, dict):
        return None

    confidence = value.get("confidence")
    scores = value.get("microgenre_scores")
    rationale = value.get("rationale")
    if not isinstance(confidence, int | float) or isinstance(confidence, bool):
        return None
    if not isinstance(scores, dict):
        return None
    if not isinstance(rationale, str):
        return None

    resolved_scores: dict[str, float] = {}
    for key, score in scores.items():
        if not isinstance(key, str):
            return None
        if not isinstance(score, int | float) or isinstance(score, bool):
            return None
        resolved_scores[key] = float(score)

    return {
        "confidence": float(confidence),
        "microgenre_scores": resolved_scores,
        "rationale": rationale,
    }
