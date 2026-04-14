from __future__ import annotations

import pandas as pd

from infrastructure.data_quality import apply_row_quality_scores


SCORE_COLUMNS = [
    "score_potencial_eleitoral",
    "score_oportunidade",
    "score_eficiencia_midia",
    "score_custo",
    "score_risco",
]


def _normalize(values: pd.Series, *, invert: bool = False, neutral: float = 0.5) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    min_v = numeric.min()
    max_v = numeric.max()
    if pd.isna(min_v) or pd.isna(max_v) or max_v == min_v:
        out = pd.Series([neutral] * len(values), index=values.index, dtype=float)
    else:
        out = (numeric - min_v) / (max_v - min_v)
    out = out.fillna(neutral).clip(0.0, 1.0)
    return (1.0 - out) if invert else out


def _base_municipios(*frames: pd.DataFrame) -> pd.DataFrame:
    ids: set[str] = set()
    for frame in frames:
        if not frame.empty and "municipio_id_ibge7" in frame.columns:
            ids.update(frame["municipio_id_ibge7"].dropna().astype(str).str.strip().tolist())
    return pd.DataFrame({"municipio_id_ibge7": sorted(v for v in ids if v)})


def _ensure_metric(df: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if column not in df.columns:
        return pd.Series([default] * len(df), index=df.index, dtype=float)
    return pd.to_numeric(df[column], errors="coerce").fillna(default)


def build_modular_allocation_scores(
    *,
    mart_municipio: pd.DataFrame,
    mart_potencial: pd.DataFrame,
    mart_territorial: pd.DataFrame,
    mart_custo: pd.DataFrame,
    mart_sensibilidade: pd.DataFrame,
    mart_midia: pd.DataFrame,
    features: pd.DataFrame | None = None,
    score_weights: dict[str, float] | None = None,
    risk_weights: dict[str, float] | None = None,
) -> pd.DataFrame:
    features = features if features is not None else pd.DataFrame()
    score_weights = score_weights or {
        "potencial_eleitoral": 0.30,
        "oportunidade": 0.25,
        "eficiencia_midia": 0.20,
        "custo": 0.15,
        "risco_invertido": 0.10,
    }
    risk_weights = risk_weights or {"volatilidade_historica": 0.70, "qualidade_dados": 0.30}
    base = _base_municipios(
        mart_municipio, mart_potencial, mart_territorial, mart_custo, mart_sensibilidade, mart_midia, features
    )
    if base.empty:
        return pd.DataFrame(columns=["municipio_id_ibge7", *SCORE_COLUMNS, "score_alocacao", "roi_politico_estimado"])

    score = base.copy()
    merge_specs = [
        (mart_municipio, ["municipio_id_ibge7", "ranking_medio_3ciclos", "indice_medio_3ciclos", "data_quality_score"]),
        (mart_potencial, ["municipio_id_ibge7", "potencial_eleitoral_ajustado_social", "pop_total"]),
        (
            mart_territorial,
            ["municipio_id_ibge7", "score_priorizacao_territorial_sp", "ipvs", "emprego_norm", "saude_norm"],
        ),
        (mart_custo, ["municipio_id_ibge7", "custo_mobilizacao_relativo"]),
        (mart_sensibilidade, ["municipio_id_ibge7", "sensibilidade_investimento_publico"]),
        (features, ["municipio_id_ibge7", "volatilidade_historica", "competitividade", "crescimento_eleitoral"]),
    ]
    for frame, columns in merge_specs:
        if frame is not None and not frame.empty:
            existing = [c for c in columns if c in frame.columns]
            if "municipio_id_ibge7" in existing:
                score = score.merge(
                    frame[existing].drop_duplicates("municipio_id_ibge7"), on="municipio_id_ibge7", how="left"
                )

    if not mart_midia.empty:
        media = mart_midia.copy()
        if "performance" not in media.columns:
            media["performance"] = _ensure_metric(media, "ctr") * 0.6 + _ensure_metric(media, "taxa_conversao") * 0.4
        media_agg = (
            media.groupby("municipio_id_ibge7", dropna=False)
            .agg(
                gasto_midia=("gasto", "sum"),
                ctr_medio=("ctr", "mean"),
                cpc_medio=("cpc", "mean"),
                conversao_total=("conversao", "sum"),
                performance_midia=("performance", "mean"),
            )
            .reset_index()
        )
        score = score.merge(media_agg, on="municipio_id_ibge7", how="left")

    score["score_potencial_eleitoral"] = _normalize(
        _ensure_metric(score, "potencial_eleitoral_ajustado_social") + _ensure_metric(score, "indice_medio_3ciclos")
    )
    score["score_oportunidade"] = _normalize(
        _ensure_metric(score, "score_priorizacao_territorial_sp")
        + _ensure_metric(score, "sensibilidade_investimento_publico")
        + _ensure_metric(score, "crescimento_eleitoral")
    )
    score["score_eficiencia_midia"] = _normalize(
        _ensure_metric(score, "performance_midia")
        + _ensure_metric(score, "ctr_medio")
        + _ensure_metric(score, "conversao_total"),
        neutral=0.5,
    )
    score["score_custo"] = _normalize(
        _ensure_metric(score, "custo_mobilizacao_relativo", 0.5) + _normalize(_ensure_metric(score, "cpc_medio")),
        invert=True,
        neutral=0.5,
    )
    risk_base = _normalize(_ensure_metric(score, "volatilidade_historica"), neutral=0.25)
    quality_risk = 1.0 - _ensure_metric(score, "data_quality_score", 1.0).clip(0.0, 1.0)
    score["score_risco"] = (
        (risk_weights["volatilidade_historica"] * risk_base + risk_weights["qualidade_dados"] * quality_risk)
        .clip(0.0, 1.0)
        .round(6)
    )
    score["score_alocacao"] = (
        100.0
        * (
            score_weights["potencial_eleitoral"] * score["score_potencial_eleitoral"]
            + score_weights["oportunidade"] * score["score_oportunidade"]
            + score_weights["eficiencia_midia"] * score["score_eficiencia_midia"]
            + score_weights["custo"] * score["score_custo"]
            + score_weights["risco_invertido"] * (1.0 - score["score_risco"])
        )
    ).round(6)
    score["roi_politico_estimado"] = (
        score["score_alocacao"]
        / (1.0 + _ensure_metric(score, "cpc_medio") + _ensure_metric(score, "custo_mobilizacao_relativo"))
    ).round(6)
    media_spend = _ensure_metric(score, "gasto_midia")
    media_perf = _ensure_metric(score, "performance_midia")
    score["desperdicio_midia"] = (media_spend > media_spend.median()) & (media_perf < media_perf.median())
    score["motivo_desperdicio"] = score["desperdicio_midia"].map(
        {True: "gasto acima da mediana com performance abaixo da mediana", False: "sem desperdicio relevante detectado"}
    )
    score = score.sort_values(["score_alocacao", "roi_politico_estimado"], ascending=[False, False]).reset_index(
        drop=True
    )
    score["ranking"] = score.index + 1
    for col in SCORE_COLUMNS + ["score_alocacao", "roi_politico_estimado"]:
        score[col] = pd.to_numeric(score[col], errors="coerce").fillna(0.0).round(6)
    return apply_row_quality_scores(
        score,
        critical_columns=["municipio_id_ibge7", "ranking", "score_alocacao"],
        source_columns=SCORE_COLUMNS + ["roi_politico_estimado"],
    )


def simulate_budget(scores: pd.DataFrame, *, total_budget: float) -> pd.DataFrame:
    if scores.empty:
        return pd.DataFrame(
            columns=[
                "municipio_id_ibge7",
                "verba_simulada",
                "impacto_incremental_estimado",
                "roi_politico_estimado",
                "desperdicio_midia",
            ]
        )
    out = scores.copy()
    budget = max(0.0, float(total_budget))
    weights = (
        pd.to_numeric(out["score_alocacao"], errors="coerce").fillna(0.0)
        * (1.0 - pd.to_numeric(out["score_risco"], errors="coerce").fillna(0.0))
    ).clip(lower=0.0)
    if float(weights.sum()) <= 0.0:
        weights = pd.Series([1.0] * len(out), index=out.index, dtype=float)
    out["verba_simulada"] = (weights / weights.sum() * budget).round(2)
    out["impacto_incremental_estimado"] = (
        (out["verba_simulada"] / 1000.0) * pd.to_numeric(out["roi_politico_estimado"], errors="coerce").fillna(0.0)
    ).round(6)
    out["pergunta_respondida"] = out["verba_simulada"].map(
        lambda v: f"Se investir R$ {v:,.2f}, priorizar execucao com ROI politico estimado"
    )
    return out.sort_values(["impacto_incremental_estimado", "score_alocacao"], ascending=[False, False]).reset_index(
        drop=True
    )


def recommend_allocation(
    *,
    scores: pd.DataFrame,
    budget_simulation: pd.DataFrame,
    mart_message: pd.DataFrame,
    total_budget: float,
) -> pd.DataFrame:
    if scores.empty:
        return pd.DataFrame(
            columns=[
                "ranking",
                "municipio_id_ibge7",
                "verba_sugerida",
                "canal_ideal",
                "mensagem_ideal",
                "justificativa",
            ]
        )
    rec = scores[
        [
            "municipio_id_ibge7",
            "ranking",
            *SCORE_COLUMNS,
            "score_alocacao",
            "roi_politico_estimado",
            "desperdicio_midia",
            "motivo_desperdicio",
        ]
    ].copy()
    rec = rec.merge(
        budget_simulation[["municipio_id_ibge7", "verba_simulada", "impacto_incremental_estimado"]],
        on="municipio_id_ibge7",
        how="left",
    )
    if not mart_message.empty:
        renamed = (
            mart_message.sort_values(["municipio_id_ibge7", "ranking_mensagem_cidade"])
            .drop_duplicates("municipio_id_ibge7")
            .rename(columns={"mensagem": "mensagem_ideal", "plataforma": "canal_ideal"})
        )
        keep = [
            c
            for c in [
                "municipio_id_ibge7",
                "municipio",
                "mensagem_ideal",
                "tema",
                "narrativa",
                "publico_alvo",
                "canal_ideal",
            ]
            if c in renamed.columns
        ]
        rec = rec.merge(renamed[keep], on="municipio_id_ibge7", how="left")
    if "canal_ideal" not in rec.columns:
        rec["canal_ideal"] = "midia_paga"
    if "mensagem_ideal" not in rec.columns:
        rec["mensagem_ideal"] = "mensagem com maior performance territorial disponivel"
    rec["canal_ideal"] = rec["canal_ideal"].fillna("midia_paga")
    rec["mensagem_ideal"] = rec["mensagem_ideal"].fillna("mensagem com maior performance territorial disponivel")
    rec["verba_sugerida"] = pd.to_numeric(rec["verba_simulada"], errors="coerce").fillna(0.0).round(2)
    rec["justificativa"] = rec.apply(
        lambda row: (
            f"Score {row['score_alocacao']:.1f}: potencial {row['score_potencial_eleitoral']:.2f}, "
            f"oportunidade {row['score_oportunidade']:.2f}, eficiencia de midia {row['score_eficiencia_midia']:.2f}, "
            f"custo {row['score_custo']:.2f}, risco {row['score_risco']:.2f}. {row['motivo_desperdicio']}"
        ),
        axis=1,
    )
    rec = rec.sort_values(["ranking", "impacto_incremental_estimado"], ascending=[True, False]).reset_index(drop=True)
    rec["ranking"] = rec.index + 1
    rec["orcamento_total_simulado"] = round(float(total_budget), 2)
    return apply_row_quality_scores(
        rec,
        critical_columns=["municipio_id_ibge7", "ranking", "verba_sugerida", "canal_ideal", "mensagem_ideal"],
        source_columns=["score_alocacao", "roi_politico_estimado", "verba_sugerida", "impacto_incremental_estimado"],
    )
