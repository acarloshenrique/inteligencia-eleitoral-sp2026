from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class MatchResult:
    matched_df: pd.DataFrame
    review_queue_df: pd.DataFrame


def score_similarity(left: str, right: str) -> float:
    return float(SequenceMatcher(a=str(left), b=str(right)).ratio())


def build_alias_dimension(
    dim_municipio: pd.DataFrame,
    alias_map: dict[str, list[str]] | None = None,
) -> pd.DataFrame:
    alias_rows: list[dict[str, str]] = []
    alias_map = alias_map or {}
    for _, row in dim_municipio.iterrows():
        municipio_id = str(row["municipio_id_ibge7"]).strip()
        official = str(row["nome_municipio"]).strip()
        municipio_norm = str(row["municipio_norm"]).strip()
        aliases = {str(a).strip() for a in alias_map.get(municipio_id, []) if str(a).strip()}
        aliases.add(official)
        aliases.add(municipio_norm)
        for alias in aliases:
            alias_rows.append(
                {
                    "municipio_id_ibge7": municipio_id,
                    "alias_nome": str(alias).strip(),
                    "alias_norm": str(alias).strip(),
                }
            )
    if not alias_rows:
        return pd.DataFrame(columns=["municipio_id_ibge7", "alias_nome", "alias_norm"])
    return pd.DataFrame(alias_rows).drop_duplicates(subset=["municipio_id_ibge7", "alias_norm"]).reset_index(drop=True)


def _candidate_payload(rows: pd.DataFrame, *, score_col: str | None = None) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for _, row in rows.iterrows():
        item = {
            "municipio_id_ibge7": row.get("municipio_id_ibge7"),
            "codigo_tse": row.get("codigo_tse"),
            "codigo_ibge": row.get("codigo_ibge"),
            "nome_municipio": row.get("nome_municipio"),
        }
        if score_col:
            item["score"] = round(float(row.get(score_col, 0.0) or 0.0), 6)
        payload.append(item)
    return payload


def _mark_manual_review(
    *,
    base: pd.DataFrame,
    row_id: int,
    input_row: pd.Series,
    candidates: list[dict[str, Any]],
    reason: str,
) -> dict[str, Any]:
    best_score = candidates[0].get("score") if candidates and "score" in candidates[0] else None
    base.loc[base["_match_row_id"] == row_id, "join_method"] = "manual_review"
    base.loc[base["_match_row_id"] == row_id, "join_confidence"] = best_score if best_score is not None else 0.0
    base.loc[base["_match_row_id"] == row_id, "needs_review"] = True
    base.loc[base["_match_row_id"] == row_id, "match_conflict_reason"] = reason
    base.loc[base["_match_row_id"] == row_id, "match_candidates"] = str(candidates[:5])
    return {
        "municipio_input": input_row.get("municipio_input"),
        "municipio_norm_input": input_row.get("municipio_norm_input"),
        "join_status": "manual_review",
        "join_method": "manual_review",
        "join_confidence": best_score,
        "needs_review": True,
        "match_candidates": candidates[:5],
        "best_score": best_score,
        "reason": reason,
    }


def _apply_unique_match(
    *,
    base: pd.DataFrame,
    row_id: int,
    candidates: pd.DataFrame,
    method: str,
    confidence: float,
) -> bool:
    unique = candidates.dropna(subset=["municipio_id_ibge7"]).drop_duplicates(subset=["municipio_id_ibge7"])
    if len(unique) != 1:
        return False
    row = unique.iloc[0]
    mask = base["_match_row_id"] == row_id
    for col in ["municipio_id_ibge7", "codigo_tse", "codigo_ibge", "nome_municipio", "municipio_norm"]:
        base.loc[mask, col] = row.get(col)
    base.loc[mask, "join_method"] = method
    base.loc[mask, "join_confidence"] = confidence
    base.loc[mask, "match_conflict_reason"] = pd.NA
    return True


def layered_match_territory(
    *,
    base_df: pd.DataFrame,
    dim_municipio: pd.DataFrame,
    dim_alias: pd.DataFrame,
    input_name_col: str,
    input_code_col: str | None = None,
    fuzzy_threshold: float = 0.90,
    ambiguous_gap: float = 0.03,
) -> MatchResult:
    dim_lookup = dim_municipio[
        ["municipio_id_ibge7", "codigo_tse", "codigo_ibge", "nome_municipio", "municipio_norm"]
    ].copy()
    dim_lookup["codigo_tse"] = dim_lookup["codigo_tse"].astype(str).str.strip()
    dim_lookup["municipio_norm"] = dim_lookup["municipio_norm"].astype(str).str.strip()

    alias_lookup = dim_alias[["municipio_id_ibge7", "alias_nome", "alias_norm"]].copy()
    alias_lookup["alias_norm"] = alias_lookup["alias_norm"].astype(str).str.strip()
    alias_join = alias_lookup.merge(dim_lookup, on="municipio_id_ibge7", how="left")

    base = base_df.copy()
    base["_match_row_id"] = range(len(base))
    base["municipio_input"] = base[input_name_col].astype(str).fillna("").str.strip()
    if "municipio_norm_input" in base.columns:
        base["municipio_norm_input"] = base["municipio_norm_input"].astype(str).fillna("").str.strip()
    else:
        base["municipio_norm_input"] = base["municipio_input"].astype(str).str.strip()
    base["join_method"] = pd.NA
    base["join_confidence"] = pd.NA
    base["needs_review"] = False
    base["match_candidates"] = pd.NA
    base["match_conflict_reason"] = pd.NA

    review_rows: list[dict[str, Any]] = []

    if input_code_col and input_code_col in base.columns:
        base[input_code_col] = base[input_code_col].astype(str).str.strip()
        for _, row in base.loc[base["join_method"].isna()].iterrows():
            code = str(row.get(input_code_col, "")).strip()
            if not code:
                continue
            candidates = dim_lookup.loc[dim_lookup["codigo_tse"] == code]
            if candidates.empty:
                continue
            row_id = int(row["_match_row_id"])
            if not _apply_unique_match(base=base, row_id=row_id, candidates=candidates, method="exact_code", confidence=1.0):
                review_rows.append(
                    _mark_manual_review(
                        base=base,
                        row_id=row_id,
                        input_row=row,
                        candidates=_candidate_payload(candidates),
                        reason="conflicting_exact_code",
                    )
                )

    for method, lookup, left_col, right_col, confidence, conflict_reason in [
        ("exact_name", dim_lookup, "municipio_norm_input", "municipio_norm", 1.0, "conflicting_exact_name"),
        ("historical_alias", alias_join, "municipio_norm_input", "alias_norm", 0.99, "conflicting_alias"),
    ]:
        for _, row in base.loc[base["join_method"].isna()].iterrows():
            value = str(row.get(left_col, "")).strip()
            if not value:
                continue
            candidates = lookup.loc[lookup[right_col] == value]
            if candidates.empty:
                continue
            row_id = int(row["_match_row_id"])
            if not _apply_unique_match(base=base, row_id=row_id, candidates=candidates, method=method, confidence=confidence):
                review_rows.append(
                    _mark_manual_review(
                        base=base,
                        row_id=row_id,
                        input_row=row,
                        candidates=_candidate_payload(candidates),
                        reason=conflict_reason,
                    )
                )

    for _, row in base.loc[base["join_method"].isna()].iterrows():
        normalized = str(row["municipio_norm_input"]).strip()
        fuzzy_candidates: list[dict[str, Any]] = []
        for _, candidate in alias_join.iterrows():
            alias_norm = str(candidate.get("alias_norm", "")).strip()
            if not alias_norm:
                continue
            score = score_similarity(normalized, alias_norm)
            fuzzy_candidates.append(
                {
                    "municipio_id_ibge7": candidate.get("municipio_id_ibge7"),
                    "codigo_tse": candidate.get("codigo_tse"),
                    "codigo_ibge": candidate.get("codigo_ibge"),
                    "nome_municipio": candidate.get("nome_municipio"),
                    "alias_norm": alias_norm,
                    "score": round(score, 6),
                }
            )
        fuzzy_candidates.sort(key=lambda item: item["score"], reverse=True)
        deduped: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        for candidate in fuzzy_candidates:
            municipio_id = str(candidate.get("municipio_id_ibge7", ""))
            if municipio_id in seen_ids:
                continue
            seen_ids.add(municipio_id)
            deduped.append(candidate)
        top = deduped[:3]
        best = top[0] if top else None
        second = top[1] if len(top) > 1 else None
        row_id = int(row["_match_row_id"])
        if best and best["score"] >= fuzzy_threshold and (
            second is None or (best["score"] - second["score"]) >= ambiguous_gap
        ):
            mask = base["_match_row_id"] == row_id
            base.loc[mask, "municipio_id_ibge7"] = best["municipio_id_ibge7"]
            base.loc[mask, "codigo_tse"] = best["codigo_tse"]
            base.loc[mask, "codigo_ibge"] = best["codigo_ibge"]
            base.loc[mask, "nome_municipio"] = best["nome_municipio"]
            base.loc[mask, "municipio_norm"] = best["alias_norm"]
            base.loc[mask, "join_method"] = "fuzzy_score"
            base.loc[mask, "join_confidence"] = best["score"]
            base.loc[mask, "match_conflict_reason"] = pd.NA
            continue

        reason = "ambiguous_fuzzy" if best else "no_candidate"
        review_rows.append(
            _mark_manual_review(
                base=base,
                row_id=row_id,
                input_row=row,
                candidates=top,
                reason=reason,
            )
        )

    base["join_status"] = base["join_method"].map(
        lambda value: "matched"
        if str(value) in {"exact_code", "exact_name", "historical_alias", "fuzzy_score"}
        else "manual_review"
        if str(value) == "manual_review"
        else "no_match"
    )
    base["needs_review"] = base["join_status"].eq("manual_review") | base["needs_review"].fillna(False).astype(bool)
    matched_df = base.drop(columns=["_match_row_id"])
    review_queue = pd.DataFrame(review_rows)
    return MatchResult(matched_df=matched_df, review_queue_df=review_queue)
