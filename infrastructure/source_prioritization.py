from __future__ import annotations

import json
from pathlib import Path

from domain.source_prioritization import SourceEvaluation, SourceSpec


class SourcePrioritizationError(RuntimeError):
    pass


def _to_source(entry: dict) -> SourceSpec:
    try:
        return SourceSpec(
            key=str(entry["key"]),
            nome=str(entry["nome"]),
            prioridade=str(entry["prioridade"]).upper(),  # type: ignore[arg-type]
            area=str(entry["area"]),
            cobertura_municipal=float(entry["cobertura_municipal"]),
            atualizacao_dias=int(entry["atualizacao_dias"]),
            licenca_aberta=bool(entry["licenca_aberta"]),
            schema_quality=float(entry["schema_quality"]),
            endpoint=str(entry["endpoint"]),
            notes=str(entry.get("notes", "")),
        )
    except KeyError as exc:
        raise SourcePrioritizationError(f"campo obrigatorio ausente no catalogo: {exc}") from exc


def load_source_catalog(path: Path) -> list[SourceSpec]:
    if not path.exists():
        raise SourcePrioritizationError(f"catalogo de fontes nao encontrado: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise SourcePrioritizationError("catalogo invalido: esperado array JSON")
    out = [_to_source(item) for item in payload]
    if not out:
        raise SourcePrioritizationError("catalogo invalido: nenhuma fonte cadastrada")
    return out


def evaluate_source(source: SourceSpec) -> SourceEvaluation:
    reasons: list[str] = []
    accepted = True

    if source.cobertura_municipal < 0.8:
        accepted = False
        reasons.append("cobertura municipal abaixo de 80%")
    if source.atualizacao_dias > 120:
        accepted = False
        reasons.append("atualizacao muito esparsa (>120 dias)")
    if not source.licenca_aberta:
        accepted = False
        reasons.append("licenca nao aberta")
    if source.schema_quality < 0.7:
        accepted = False
        reasons.append("qualidade de schema abaixo do minimo (0.7)")

    if source.prioridade == "A":
        if source.cobertura_municipal < 0.95:
            accepted = False
            reasons.append("fonte A requer cobertura >= 95%")
        if source.atualizacao_dias > 45:
            accepted = False
            reasons.append("fonte A requer atualizacao <= 45 dias")
        if source.schema_quality < 0.85:
            accepted = False
            reasons.append("fonte A requer schema_quality >= 0.85")

    score = (
        source.cobertura_municipal * 0.45
        + min(1.0, 45.0 / max(1.0, float(source.atualizacao_dias))) * 0.20
        + (1.0 if source.licenca_aberta else 0.0) * 0.20
        + source.schema_quality * 0.15
    )
    return SourceEvaluation(
        source=source,
        accepted=accepted,
        score=round(float(score), 4),
        reasons=tuple(reasons),
    )


def prioritize_sources(sources: list[SourceSpec]) -> dict[str, list[SourceEvaluation]]:
    evals = [evaluate_source(s) for s in sources]
    accepted = [ev for ev in evals if ev.accepted]
    rejected = [ev for ev in evals if not ev.accepted]

    accepted_a = sorted(
        [ev for ev in accepted if ev.source.prioridade == "A"],
        key=lambda ev: ev.score,
        reverse=True,
    )
    accepted_b = sorted(
        [ev for ev in accepted if ev.source.prioridade == "B"],
        key=lambda ev: ev.score,
        reverse=True,
    )
    rejected_sorted = sorted(rejected, key=lambda ev: ev.score, reverse=True)
    return {"accepted_a": accepted_a, "accepted_b": accepted_b, "rejected": rejected_sorted}


def render_prioritization_report(groups: dict[str, list[SourceEvaluation]]) -> dict:
    def _as_dict(ev: SourceEvaluation) -> dict:
        return {
            "key": ev.source.key,
            "nome": ev.source.nome,
            "prioridade": ev.source.prioridade,
            "area": ev.source.area,
            "score": ev.score,
            "accepted": ev.accepted,
            "endpoint": ev.source.endpoint,
            "reasons": list(ev.reasons),
        }

    return {
        "accepted_a": [_as_dict(ev) for ev in groups["accepted_a"]],
        "accepted_b": [_as_dict(ev) for ev in groups["accepted_b"]],
        "rejected": [_as_dict(ev) for ev in groups["rejected"]],
    }

