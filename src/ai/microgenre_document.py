from __future__ import annotations

from ai.microgenre_batch_models import MicroGenreBatchSidecarRecord
from ai.prompts.microgenre_classifier import MicroGenreScoreResult
from core.microgenres import MicrogenresDocument, coerce_microgenres_document


def microgenre_result_to_redis(result: MicroGenreScoreResult) -> MicrogenresDocument:
    """Return the compact Redis document shape for a classifier result."""
    return {
        "confidence": result.confidence,
        "microgenre_scores": dict(result.microgenre_scores),
        "rationale": result.rationale,
    }


def microgenre_sidecar_to_redis(row: MicroGenreBatchSidecarRecord) -> MicrogenresDocument | None:
    """Convert a successful JSONL sidecar row into the compact Redis shape."""
    if row.error is not None or row.classification is None:
        return None
    return microgenre_result_to_redis(row.classification)


valid_microgenres_value = coerce_microgenres_document
