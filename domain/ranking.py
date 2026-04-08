import pandas as pd


def calcular_ranking(df_mun: pd.DataFrame, clusters: list[str] | None = None, busca: str = "") -> pd.DataFrame:
    out = df_mun.copy()
    if clusters:
        out = out[out["cluster"].isin(clusters)]
    if busca:
        out = out[out["municipio"].str.upper().str.contains(busca.upper(), na=False)]
    return out.sort_values("ranking_final").reset_index(drop=True)
