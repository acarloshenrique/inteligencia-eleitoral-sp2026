TETOS = {
    "deputado_federal": 2_500_000,
    "deputado_estadual": 1_350_000,
    "governador": 70_680_000,
    "senador": 10_500_000,
    "vereador_grande": 340_000,
    "vereador_medio": 180_000,
    "vereador_pequeno": 60_000,
    "prefeito_grande": 4_200_000,
    "prefeito_medio": 1_600_000,
    "prefeito_pequeno": 420_000,
}

CARGOS_EST = {"deputado_federal", "deputado_estadual", "governador", "senador"}

PESOS_CLUSTER = {"Diamante": 1.0, "Alavanca": 0.70, "Consolidação": 0.45, "Descarte": 0.10}

ALOC_COLS = [
    "municipio",
    "cluster",
    "ranking",
    "indice",
    "PD_qt",
    "pop",
    "budget",
    "digital",
    "offline",
    "meta_fb_ig",
    "youtube",
    "tiktok",
    "whatsapp",
    "google_ads",
    "evento_presencial",
    "radio_local",
    "impresso",
]

SYSTEM_PROMPT = """Você é analista sênior de inteligência eleitoral SP 2026.
644 municípios paulistas ranqueados por: Territorial 35% + VS 25% + ISE 20% + PD 20%.
Clusters: Diamante (territorial>70 e VS>70) → máximo investimento | Alavanca → potencial latente | Consolidação → manutenção | Descarte → mínimo.
Responda em português, seja preciso, cite dados do contexto. Não invente valores."""
