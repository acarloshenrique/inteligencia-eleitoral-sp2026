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
        aliases = set(alias_map.get(municipio_id, []))
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
    alias_lookup = dim_alias[["municipio_id_ibge7", "alias_nome", "alias_norm"]].copy()
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

    if input_code_col and input_code_col in base.columns:
        code_map = dim_lookup.copy()
        code_map["codigo_tse"] = code_map["codigo_tse"].astype(str).str.strip()
        base[input_code_col] = base[input_code_col].astype(str).str.strip()
        code_match = base[[input_code_col, "_match_row_id"]].merge(
            code_map,
            left_on=input_code_col,
            right_on="codigo_tse",
            how="left",
        )
        matched_ids = code_match["municipio_id_ibge7"].notna()
        if matched_ids.any():
            by_id = code_match.loc[matched_ids].set_index("_match_row_id")
            for col in ["municipio_id_ibge7", "codigo_tse", "codigo_ibge", "nome_municipio", "municipio_norm"]:
                base.loc[base["_match_row_id"].isin(by_id.index), col] = (
                    base.loc[base["_match_row_id"].isin(by_id.index), "_match_row_id"].map(by_id[col])
                )
            base.loc[base["_match_row_id"].isin(by_id.index), "join_method"] = "exact_code"
            base.loc[base["_match_row_id"].isin(by_id.index), "join_confidence"] = 1.0

    unmatched_mask = base["join_method"].isna()
    if unmatched_mask.any():
        exact_name = base.loc[unmatched_mask, ["_match_row_id", "municipio_norm_input"]].merge(
            dim_lookup,
            left_on="municipio_norm_input",
            right_on="municipio_norm",
            how="left",
        )
        matched_ids = exact_name["municipio_id_ibge7"].notna()
        if matched_ids.any():
            by_id = exact_name.loc[matched_ids].set_index("_match_row_id")
            for col in ["municipio_id_ibge7", "codigo_tse", "codigo_ibge", "nome_municipio", "municipio_norm"]:
                base.loc[base["_match_row_id"].isin(by_id.index), col] = (
                    base.loc[base["_match_row_id"].isin(by_id.index), "_match_row_id"].map(by_id[col])
                )
            base.loc[base["_match_row_id"].isin(by_id.index), "join_method"] = "exact_name"
            base.loc[base["_match_row_id"].isin(by_id.index), "join_confidence"] = 1.0

    unmatched_mask = base["join_method"].isna()
    if unmatched_mask.any():
        alias_name = base.loc[unmatched_mask, ["_match_row_id", "municipio_norm_input"]].merge(
            alias_join,
            left_on="municipio_norm_input",
            right_on="alias_norm",
            how="left",
        )
        matched_ids = alias_name["municipio_id_ibge7"].notna()
        if matched_ids.any():
            by_id = alias_name.loc[matched_ids].set_index("_match_row_id")
            for col in ["municipio_id_ibge7", "codigo_tse", "codigo_ibge", "nome_municipio", "municipio_norm"]:
                base.loc[base["_match_row_id"].isin(by_id.index), col] = (
                    base.loc[base["_match_row_id"].isin(by_id.index), "_match_row_id"].map(by_id[col])
                )
            base.loc[base["_match_row_id"].isin(by_id.index), "join_method"] = "historical_alias"
            base.loc[base["_match_row_id"].isin(by_id.index), "join_confidence"] = 0.99

    review_rows: list[dict[str, Any]] = []
    unmatched_mask = base["join_method"].isna()
    for idx, row in base.loc[unmatched_mask].iterrows():
        normalized = str(row["municipio_norm_input"]).strip()
        candidates: list[dict[str, Any]] = []
        for _, candidate in alias_join.iterrows():
            alias_norm = str(candidate.get("alias_norm", "")).strip()
            if not alias_norm:
                continue
            score = score_similarity(normalized, alias_norm)
            candidates.append(
                {
                    "municipio_id_ibge7": candidate.get("municipio_id_ibge7"),
                    "codigo_tse": candidate.get("codigo_tse"),
                    "codigo_ibge": candidate.get("codigo_ibge"),
                    "nome_municipio": candidate.get("nome_municipio"),
                    "alias_norm": alias_norm,
                    "score": round(score, 6),
                }
            )
        candidates.sort(key=lambda item: item["score"], reverse=True)
        top = candidates[:3]
        best = top[0] if top else None
        second = top[1] if len(top) > 1 else None
        if best and best["score"] >= fuzzy_threshold and (
            second is None or (best["score"] - second["score"]) >= ambiguous_gap
        ):
            base.at[idx, "municipio_id_ibge7"] = best["municipio_id_ibge7"]
            base.at[idx, "codigo_tse"] = best["codigo_tse"]
            base.at[idx, "codigo_ibge"] = best["codigo_ibge"]
            base.at[idx, "nome_municipio"] = best["nome_municipio"]
            base.at[idx, "municipio_norm"] = best["alias_norm"]
            base.at[idx, "join_method"] = "fuzzy_score"
            base.at[idx, "join_confidence"] = best["score"]
            continue

        base.at[idx, "join_method"] = "manual_review"
        base.at[idx, "join_confidence"] = best["score"] if best else pd.NA
        base.at[idx, "needs_review"] = True
        base.at[idx, "match_candidates"] = str(top)
        review_rows.append(
            {
                "municipio_input": row["municipio_input"],
                "municipio_norm_input": normalized,
                "join_status": "manual_review",
                "join_method": "manual_review",
                "join_confidence": best["score"] if best else None,
                "needs_review": True,
                "match_candidates": top,
                "best_score": best["score"] if best else None,
                "reason": "ambiguous_fuzzy" if best else "no_candidate",
            }
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
