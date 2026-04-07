import numpy as np
import pandas as pd

from domain.constants import ALOC_COLS


def calcular_alocacao(df_mun, budget, cargo, n, split_d, pesos_cluster, tetos, cargos_est):
    top = df_mun.nsmallest(n, "ranking_final").copy()
    top = top[top["cluster"] != "Descarte"]
    if top.empty:
        return pd.DataFrame(columns=ALOC_COLS)

    top["pw"] = top["indice_final"] * top["cluster"].map(pesos_cluster).fillna(0.1)
    top["pn"] = top["pw"] / top["pw"].sum()
    top["budget"] = (top["pn"] * budget).round(0)

    if cargo not in cargos_est:
        cap = float(tetos.get(cargo, budget))
        top["budget"] = top["budget"].clip(upper=cap)

    pq = pd.to_numeric(top.get("PD_qt", 50), errors="coerce").fillna(50.0)
    po = pd.to_numeric(top.get("pop_censo2022", 50000), errors="coerce").fillna(50000.0)
    bd = (top["budget"] * split_d).round(0)
    bo = (top["budget"] * (1 - split_d)).round(0)
    j = (pq / 100.0).clip(upper=1.0)
    s = 1.0 - j
    bw = np.where(po < 20_000, 0.05, 0.0)

    meta_fb_ig = (0.40 + s * 0.15) - bw
    meta_fb_ig = np.maximum(meta_fb_ig, 0.30)
    youtube = np.full(len(top), 0.25)
    tiktok = 0.10 + j * 0.15
    whatsapp = 0.10 + s * 0.05 + bw
    google_ads = np.full(len(top), 0.10)
    dt = meta_fb_ig + youtube + tiktok + whatsapp + google_ads

    evento_presencial = np.select([pq > 60, pq >= 40], [0.55, 0.40], default=0.25)
    radio_local = np.select([pq > 60, pq >= 40], [0.30, 0.35], default=0.45)
    impresso = np.select([pq > 60, pq >= 40], [0.15, 0.25], default=0.30)

    return pd.DataFrame(
        {
            "municipio": top["municipio"],
            "cluster": top["cluster"],
            "ranking": top["ranking_final"].astype(int),
            "indice": top["indice_final"].round(1),
            "PD_qt": pq.round(1),
            "pop": po.astype(int),
            "budget": top["budget"],
            "digital": bd,
            "offline": bo,
            "meta_fb_ig": (bd * (meta_fb_ig / dt)).round(0),
            "youtube": (bd * (youtube / dt)).round(0),
            "tiktok": (bd * (tiktok / dt)).round(0),
            "whatsapp": (bd * (whatsapp / dt)).round(0),
            "google_ads": (bd * (google_ads / dt)).round(0),
            "evento_presencial": (bo * evento_presencial).round(0),
            "radio_local": (bo * radio_local).round(0),
            "impresso": (bo * impresso).round(0),
        }
    )[ALOC_COLS]
