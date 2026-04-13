import unicodedata

import numpy as np
import pandas as pd

from domain.constants import ALOC_COLS


def _normalize_label(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(value))
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def calcular_alocacao(df_mun, budget, cargo, n, split_d, pesos_cluster, tetos, cargos_est, channel_weights=None):
    top = df_mun.nsmallest(n, "ranking_final").copy()
    top = top[top["cluster"] != "Descarte"]
    if top.empty:
        return pd.DataFrame(columns=ALOC_COLS)

    cluster_key = top["cluster"].map(_normalize_label)
    top["pw"] = top["indice_final"] * cluster_key.map(pesos_cluster).fillna(0.1)
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

    if channel_weights is None:
        digital_base = {"meta_fb_ig": 0.40, "youtube": 0.25, "tiktok": 0.10, "whatsapp": 0.10, "google_ads": 0.10}
        sensitivity = {
            "meta_fb_ig_low_pd_bonus": 0.15,
            "tiktok_high_pd_bonus": 0.15,
            "whatsapp_low_pd_bonus": 0.05,
            "small_city_bias": 0.05,
            "meta_min": 0.30,
        }
        offline = {
            "high_pd_threshold": 60,
            "mid_pd_threshold": 40,
            "high_pd": {"evento_presencial": 0.55, "radio_local": 0.30, "impresso": 0.15},
            "mid_pd": {"evento_presencial": 0.40, "radio_local": 0.35, "impresso": 0.25},
            "low_pd": {"evento_presencial": 0.25, "radio_local": 0.45, "impresso": 0.30},
        }
    else:
        payload = channel_weights.model_dump() if hasattr(channel_weights, "model_dump") else channel_weights
        digital_base = payload["digital_base"]
        sensitivity = payload["digital_sensitivity"]
        offline = payload["offline_by_pd"]

    meta_fb_ig = (digital_base["meta_fb_ig"] + s * sensitivity["meta_fb_ig_low_pd_bonus"]) - bw
    meta_fb_ig = np.maximum(meta_fb_ig, sensitivity["meta_min"])
    youtube = np.full(len(top), digital_base["youtube"])
    tiktok = digital_base["tiktok"] + j * sensitivity["tiktok_high_pd_bonus"]
    whatsapp = digital_base["whatsapp"] + s * sensitivity["whatsapp_low_pd_bonus"] + bw
    google_ads = np.full(len(top), digital_base["google_ads"])
    dt = meta_fb_ig + youtube + tiktok + whatsapp + google_ads

    high = offline["high_pd"]
    mid = offline["mid_pd"]
    low = offline["low_pd"]
    high_threshold = offline["high_pd_threshold"]
    mid_threshold = offline["mid_pd_threshold"]
    evento_presencial = np.select([pq > high_threshold, pq >= mid_threshold], [high["evento_presencial"], mid["evento_presencial"]], default=low["evento_presencial"])
    radio_local = np.select([pq > high_threshold, pq >= mid_threshold], [high["radio_local"], mid["radio_local"]], default=low["radio_local"])
    impresso = np.select([pq > high_threshold, pq >= mid_threshold], [high["impresso"], mid["impresso"]], default=low["impresso"])

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
