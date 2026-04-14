import json
from pathlib import Path
import sys
import tempfile

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.settings import AppPaths
from infrastructure.medallion_pipeline import MedallionInputs, run_medallion_pipeline


def test_medallion_pipeline_builds_bronze_silver_gold_and_marts():
    with tempfile.TemporaryDirectory() as tmp:
        data_root = Path(tmp)
        ingestion_root = data_root / "ingestion"
        lake_root = data_root / "lake"
        bronze_root = lake_root / "bronze"
        silver_root = lake_root / "silver"
        gold_root = lake_root / "gold"
        gold_reports_root = gold_root / "reports"
        gold_serving_root = gold_root / "serving"
        catalog_root = gold_root / "_catalog"
        chroma = data_root / "chromadb"
        runtime_reports_root = data_root / "runtime_rel"
        for path in [
            ingestion_root,
            bronze_root,
            silver_root,
            gold_root,
            gold_reports_root,
            gold_serving_root,
            catalog_root,
            chroma,
            runtime_reports_root,
        ]:
            path.mkdir(parents=True, exist_ok=True)

        base_path = ingestion_root / "df_mun_20260408_100000.parquet"
        pd.DataFrame(
            [
                {"municipio": "Sao Paulo", "ranking_final": 1, "indice_final": 91.0, "ano": 2026, "turno": 1},
                {"municipio": "Campinas", "ranking_final": 2, "indice_final": 88.0, "ano": 2026, "turno": 1},
                {"municipio": "Sao Paulo", "ranking_final": 2, "indice_final": 86.0, "ano": 2024, "turno": 1},
                {"municipio": "Campinas", "ranking_final": 3, "indice_final": 80.0, "ano": 2024, "turno": 1},
                {"municipio": "Sao Paulo", "ranking_final": 2, "indice_final": 84.0, "ano": 2022, "turno": 1},
                {"municipio": "Campinas", "ranking_final": 4, "indice_final": 76.0, "ano": 2022, "turno": 1},
                {"municipio": "Sao Paulo", "ranking_final": 3, "indice_final": 81.0, "ano": 2020, "turno": 1},
            ]
        ).to_parquet(base_path, index=False)

        raw_dir = data_root / "open_data" / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        mapping_path = raw_dir / "municipios_tse_ibge.csv"
        pd.DataFrame(
            [
                {
                    "codigo_tse": "71072",
                    "codigo_ibge": "3550308",
                    "nome_municipio": "Sao Paulo",
                    "aliases": "SP Capital",
                },
                {"codigo_tse": "62919", "codigo_ibge": "3509502", "nome_municipio": "Campinas", "aliases": ""},
            ]
        ).to_csv(mapping_path, index=False)

        socio_path = raw_dir / "indicadores_municipais.csv"
        pd.DataFrame(
            [
                {"codigo_ibge": "3550308", "idhm": 0.81, "taxa_emprego": 0.67},
                {"codigo_ibge": "3509502", "idhm": 0.80, "taxa_emprego": 0.65},
            ]
        ).to_csv(socio_path, index=False)
        ibge_path = raw_dir / "ibge_pop_renda_educacao.csv"
        pd.DataFrame(
            [
                {
                    "codigo_ibge": "3550308",
                    "pop_total": 12000000,
                    "densidade_demografica": 7600,
                    "renda_media": 3200,
                    "educacao_indice": 0.78,
                    "urbanizacao_pct": 0.99,
                    "idade_mediana": 35,
                    "acesso_internet_pct": 0.92,
                    "estrutura_urbana_indice": 0.89,
                    "ruralidade_pct": 0.01,
                },
                {
                    "codigo_ibge": "3509502",
                    "pop_total": 1150000,
                    "densidade_demografica": 1400,
                    "renda_media": 2900,
                    "educacao_indice": 0.74,
                    "urbanizacao_pct": 0.97,
                    "idade_mediana": 33,
                    "acesso_internet_pct": 0.86,
                    "estrutura_urbana_indice": 0.81,
                    "ruralidade_pct": 0.03,
                },
            ]
        ).to_csv(ibge_path, index=False)
        seade_path = raw_dir / "seade_ipvs_emprego_saude.csv"
        pd.DataFrame(
            [
                {
                    "codigo_ibge": "3550308",
                    "ipvs": 0.65,
                    "emprego_formal": 0.68,
                    "indice_saude": 0.72,
                    "urbanizacao_pct": 0.99,
                    "acesso_internet_pct": 0.93,
                    "estrutura_urbana_indice": 0.91,
                    "ruralidade_pct": 0.01,
                },
                {
                    "codigo_ibge": "3509502",
                    "ipvs": 0.55,
                    "emprego_formal": 0.64,
                    "indice_saude": 0.70,
                    "urbanizacao_pct": 0.97,
                    "acesso_internet_pct": 0.88,
                    "estrutura_urbana_indice": 0.83,
                    "ruralidade_pct": 0.03,
                },
            ]
        ).to_csv(seade_path, index=False)
        social_path = raw_dir / "social_ads_engajamento.csv"
        pd.DataFrame(
            [
                {"codigo_ibge": "3550308", "ads_spend": 12000, "engajamento": 0.12, "impressoes": 900000},
                {"codigo_ibge": "3509502", "ads_spend": 5000, "engajamento": 0.09, "impressoes": 240000},
            ]
        ).to_csv(social_path, index=False)
        meta_ads_path = raw_dir / "meta_ads_pago.csv"
        pd.DataFrame(
            [
                {
                    "codigo_ibge": "3550308",
                    "plataforma": "meta_ads",
                    "campanha_id": "m1",
                    "campanha_nome": "Emprego SP",
                    "criativo": "video emprego",
                    "mensagem": "Mais emprego e renda para o futuro das familias",
                    "gasto": 1000,
                    "impressoes": 10000,
                    "cliques": 500,
                    "conversao": 25,
                    "ano": 2026,
                    "mes": 8,
                },
                {
                    "codigo_ibge": "3509502",
                    "plataforma": "meta_ads",
                    "campanha_id": "m2",
                    "campanha_nome": "Saude Campinas",
                    "criativo": "card saude",
                    "mensagem": "Saude com hospital melhor e medico perto",
                    "gasto": 600,
                    "impressoes": 8000,
                    "cliques": 320,
                    "conversao": 12,
                    "ano": 2026,
                    "mes": 8,
                },
            ]
        ).to_csv(meta_ads_path, index=False)
        google_ads_path = raw_dir / "google_ads_pago.csv"
        pd.DataFrame(
            [
                {
                    "codigo_ibge": "3550308",
                    "platform": "google_ads",
                    "campaign_id": "g1",
                    "campaign_name": "Seguranca SP",
                    "creative_name": "search seguranca",
                    "ad_text": "Seguranca para proteger sua familia da violencia",
                    "cost": 500,
                    "impressions": 5000,
                    "clicks": 100,
                    "conversions": 10,
                    "ano": 2026,
                    "mes": 8,
                },
                {
                    "codigo_ibge": "3509502",
                    "platform": "google_ads",
                    "campaign_id": "g2",
                    "campaign_name": "Educacao Campinas",
                    "creative_name": "search educacao",
                    "ad_text": "Educacao e creche para cuidar das criancas",
                    "cost": 300,
                    "impressions": 2000,
                    "clicks": 80,
                    "conversions": 4,
                    "ano": 2026,
                    "mes": 8,
                },
            ]
        ).to_csv(google_ads_path, index=False)
        fiscal_path = raw_dir / "transparencia_transferencias_emendas.csv"
        pd.DataFrame(
            [
                {"codigo_ibge": "3550308", "ano": 2026, "transferencias": 2000000, "emendas": 900000},
                {"codigo_ibge": "3550308", "ano": 2024, "transferencias": 1800000, "emendas": 800000},
                {"codigo_ibge": "3550308", "ano": 2022, "transferencias": 1600000, "emendas": 700000},
                {"codigo_ibge": "3509502", "ano": 2026, "transferencias": 900000, "emendas": 300000},
                {"codigo_ibge": "3509502", "ano": 2024, "transferencias": 850000, "emendas": 250000},
                {"codigo_ibge": "3509502", "ano": 2022, "transferencias": 800000, "emendas": 220000},
            ]
        ).to_csv(fiscal_path, index=False)

        secao_path = raw_dir / "resultados_secao.csv"
        pd.DataFrame(
            [
                {"municipio": "Sao Paulo", "zona": 1, "secao": 10, "votos_validos": 1000, "ano": 2026, "turno": 1},
                {"municipio": "Sao Paulo", "zona": 1, "secao": 11, "votos_validos": 900, "ano": 2026, "turno": 1},
                {"municipio": "Campinas", "zona": 2, "secao": 21, "votos_validos": 500, "ano": 2026, "turno": 1},
            ]
        ).to_csv(secao_path, index=False)

        paths = AppPaths(
            data_root=data_root,
            ingestion_root=ingestion_root,
            lake_root=lake_root,
            bronze_root=bronze_root,
            silver_root=silver_root,
            gold_root=gold_root,
            gold_reports_root=gold_reports_root,
            gold_serving_root=gold_serving_root,
            catalog_root=catalog_root,
            chromadb_path=chroma,
            runtime_reports_root=runtime_reports_root,
            ts="20260408_100000",
            metadata_db_path=data_root / "metadata" / "jobs.sqlite3",
            artifact_root=data_root / "artifacts",
        )

        result = run_medallion_pipeline(
            paths,
            MedallionInputs(
                base_parquet_path=base_path,
                mapping_csv_path=mapping_path,
                socio_csv_path=socio_path,
                secao_csv_path=secao_path,
                ibge_csv_path=ibge_path,
                seade_csv_path=seade_path,
                social_csv_path=social_path,
                meta_ads_csv_path=meta_ads_path,
                google_ads_csv_path=google_ads_path,
                fiscal_csv_path=fiscal_path,
                window_cycles=3,
            ),
            pipeline_version="medallion_test",
        )

        manifest_path = Path(result["manifest_path"])
        assert manifest_path.exists()
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        dataset_manifests = manifest["dataset_manifests"]
        assert "mart_municipio_eleitoral" in dataset_manifests
        mart_manifest = dataset_manifests["mart_municipio_eleitoral"]
        assert mart_manifest["fonte"] == "mart_municipio_eleitoral"
        assert mart_manifest["hash_arquivo"]
        assert "municipio_id_ibge7" in mart_manifest["schema_detectado"]
        assert "cobertura_territorial" in mart_manifest
        assert "periodo_referencia" in mart_manifest
        assert "qualidade_carga" in mart_manifest
        assert mart_manifest["versao_parser"] == "medallion_test"
        assert "dim_territorio" in dataset_manifests
        assert "dim_tempo" in dataset_manifests
        assert "mart_social_ads_engajamento" in dataset_manifests
        assert "mart_midia_paga_municipio" in dataset_manifests
        assert "mart_social_mensagem_territorial" in dataset_manifests
        assert "mart_social_canal_regiao" in dataset_manifests
        assert "mart_score_alocacao_modular" in dataset_manifests
        assert "mart_simulacao_orcamento" in dataset_manifests
        assert "mart_recomendacao_alocacao" in dataset_manifests
        assert "bronze" in manifest["layers"]
        assert "silver" in manifest["layers"]
        assert "gold" in manifest["layers"]
        assert len(manifest["layers"]["bronze"]["assets"]) == 10
        assert manifest["layers"]["gold"]["aggregation_level"] == "municipio"
        assert manifest["layers"]["gold"]["window_cycles"] == 3
        assert "serving" in manifest["layers"]
        assert "contracts" in manifest["layers"]
        assert "quality_metrics" in manifest["layers"]
        assert "matching" in manifest["layers"]
        assert "allocation" in manifest["layers"]
        assert manifest["layers"]["allocation"]["budget_simulado"] == 50000.0
        assert "risco" in manifest["layers"]["allocation"]["score_components"]
        assert "lgpd" in manifest["layers"]
        assert "join_success_pct" in manifest["layers"]["quality_metrics"]
        assert "null_critical_pct" in manifest["layers"]["quality_metrics"]
        assert "manual_review_rows" in manifest["layers"]["quality_metrics"]
        assert manifest["layers"]["matching"]["contract_fields"] == [
            "join_status",
            "join_method",
            "join_confidence",
            "needs_review",
        ]
        assert "update_delay_days" in manifest["layers"]["quality_metrics"]
        assert "drift_score" in manifest["layers"]["quality_metrics"]
        assert Path(manifest["layers"]["serving"]["serving_db_path"]).exists()
        assert Path(manifest["layers"]["serving"]["cache_path"]).exists()

        published = result["published"]
        mart_municipio = pd.read_parquet(published["mart_municipio_eleitoral"])
        mart_tendencia = pd.read_parquet(published["mart_tendencia_turno"])
        mart_contexto = pd.read_parquet(published["mart_contexto_socioeconomico"])
        mart_potencial = pd.read_parquet(published["mart_potencial_eleitoral_social"])
        mart_territorial = pd.read_parquet(published["mart_priorizacao_territorial_sp"])
        dim_territorio = pd.read_parquet(published["dim_territorio"])
        dim_tempo = pd.read_parquet(published["dim_tempo"])
        mart_custo = pd.read_parquet(published["mart_custo_mobilizacao"])
        mart_sensibilidade = pd.read_parquet(published["mart_sensibilidade_investimento_publico"])
        mart_midia_paga = pd.read_parquet(published["mart_midia_paga_municipio"])
        mart_social_msg = pd.read_parquet(published["mart_social_mensagem_territorial"])
        mart_social_canal = pd.read_parquet(published["mart_social_canal_regiao"])
        mart_score_alocacao = pd.read_parquet(published["mart_score_alocacao_modular"])
        mart_simulacao = pd.read_parquet(published["mart_simulacao_orcamento"])
        mart_recomendacao = pd.read_parquet(published["mart_recomendacao_alocacao"])
        fact_social = pd.read_parquet(manifest["layers"]["silver"]["datasets"]["fact_social"])

        assert len(mart_municipio) == 2
        assert set(mart_municipio["municipio_id_ibge7"].tolist()) == {"3550308", "3509502"}
        assert "canonical_key" in mart_municipio.columns
        assert {"coverage", "data_quality_score"}.issubset(mart_municipio.columns)
        assert mart_municipio["data_quality_score"].between(0, 1).all()
        assert len(mart_tendencia) == 2
        assert "votos_validos_medio_3ciclos" in mart_tendencia.columns
        assert len(mart_contexto) == 2
        assert len(dim_territorio) >= 5
        assert "territorio_id" in dim_territorio.columns
        assert "cod_tse_municipio" in dim_territorio.columns
        assert "cod_ibge_municipio" in dim_territorio.columns
        assert {
            "cod_tse",
            "cod_ibge",
            "municipio",
            "zona",
            "secao",
            "geolocalizacao",
            "coverage",
            "data_quality_score",
        }.issubset(dim_territorio.columns)
        assert dim_territorio["coverage"].between(0, 1).all()
        assert dim_territorio["data_quality_score"].between(0, 1).all()
        assert "zona_eleitoral" in dim_territorio.columns
        assert "secao_eleitoral" in dim_territorio.columns
        assert "vigencia_inicio" in dim_territorio.columns
        assert "vigencia_fim" in dim_territorio.columns
        assert any(str(v).startswith("mun:3550308") for v in dim_territorio["territorio_id"].tolist())
        assert any(str(v).startswith("zona:3550308:1") for v in dim_territorio["territorio_id"].tolist())
        assert any(str(v).startswith("secao:3550308:1:10") for v in dim_territorio["territorio_id"].tolist())
        assert {
            "tempo_id",
            "data",
            "ano",
            "mes",
            "semana",
            "fase_eleitoral",
            "fase_calendario",
            "evento",
            "pulso_midia",
            "is_pulso_midia",
        }.issubset(dim_tempo.columns)
        assert set(["historico_eleitoral", "pre_campanha", "janela_campanha"]).issubset(
            set(dim_tempo["fase_calendario"].unique().tolist())
        )
        assert "eleicao_turno_1" in set(dim_tempo["evento"].dropna().tolist())
        assert dim_tempo.loc[dim_tempo["evento"] == "eleicao_turno_1", "pulso_midia"].iloc[0] == "alto"
        assert len(mart_potencial) == 2
        assert "potencial_eleitoral_ajustado_social" in mart_potencial.columns
        assert "densidade_demografica" in mart_potencial.columns
        assert "acesso_internet_pct" in mart_potencial.columns
        assert "urbanizacao_pct" in mart_potencial.columns
        assert set(mart_potencial["janela_anos"].unique().tolist()) == {"2022,2024,2026"}
        assert len(mart_territorial) == 2
        assert "score_priorizacao_territorial_sp" in mart_territorial.columns
        assert "estrutura_urbana_indice" in mart_territorial.columns
        assert "ruralidade_pct" in mart_territorial.columns
        assert len(mart_custo) == 2
        assert "custo_mobilizacao_relativo" in mart_custo.columns
        assert "estrutura_urbana_indice" in mart_custo.columns
        assert "ruralidade_pct" in mart_custo.columns
        assert len(mart_sensibilidade) == 2
        assert "sensibilidade_investimento_publico" in mart_sensibilidade.columns
        assert {"gasto", "impressoes", "cliques", "ctr", "cpc", "conversao", "taxa_conversao"}.issubset(
            mart_midia_paga.columns
        )
        assert set(mart_midia_paga["plataforma"].tolist()) >= {"meta_ads", "google_ads"}
        assert mart_midia_paga["ctr"].between(0, 1).all()
        assert mart_midia_paga["cpc"].ge(0).all()
        assert {
            "territorio",
            "campanha",
            "gasto",
            "performance",
            "criativo",
            "mensagem",
            "tema",
            "emocao",
            "narrativa",
            "publico_alvo",
        }.issubset(fact_social.columns)
        assert {"emprego_e_renda", "saude", "seguranca", "educacao"}.issubset(set(fact_social["tema"].tolist()))
        assert {
            "municipio",
            "mensagem",
            "tema",
            "narrativa",
            "publico_alvo",
            "performance",
            "ranking_mensagem_cidade",
        }.issubset(mart_social_msg.columns)
        assert mart_social_msg.groupby("municipio_id_ibge7")["ranking_mensagem_cidade"].min().eq(1).all()
        assert {"regiao", "plataforma", "performance", "ranking_canal_regiao"}.issubset(mart_social_canal.columns)
        assert mart_social_canal["ranking_canal_regiao"].min() == 1
        assert {
            "score_potencial_eleitoral",
            "score_oportunidade",
            "score_eficiencia_midia",
            "score_custo",
            "score_risco",
            "score_alocacao",
            "roi_politico_estimado",
        }.issubset(mart_score_alocacao.columns)
        assert mart_score_alocacao["score_alocacao"].between(0, 100).all()
        assert round(float(mart_simulacao["verba_simulada"].sum()), 2) == 50000.00
        assert {"ranking", "verba_sugerida", "canal_ideal", "mensagem_ideal", "justificativa"}.issubset(
            mart_recomendacao.columns
        )
        assert mart_recomendacao["verba_sugerida"].sum() > 0

        latest_catalog = catalog_root / "datasets_latest.json"
        latest = json.loads(latest_catalog.read_text(encoding="utf-8"))
        assert latest["mart_municipio_eleitoral"]["dataset_version"] == result["run_id"]
        assert latest["mart_tendencia_turno"]["dataset_version"] == result["run_id"]
        assert latest["mart_contexto_socioeconomico"]["dataset_version"] == result["run_id"]
        assert latest["dim_territorio"]["dataset_version"] == result["run_id"]
        assert latest["dim_tempo"]["dataset_version"] == result["run_id"]
        assert latest["mart_potencial_eleitoral_social"]["dataset_version"] == result["run_id"]
        assert latest["mart_priorizacao_territorial_sp"]["dataset_version"] == result["run_id"]
        assert latest["mart_custo_mobilizacao"]["dataset_version"] == result["run_id"]
        assert latest["mart_sensibilidade_investimento_publico"]["dataset_version"] == result["run_id"]
        assert latest["mart_social_ads_engajamento"]["dataset_version"] == result["run_id"]
        assert latest["mart_midia_paga_municipio"]["dataset_version"] == result["run_id"]
        assert latest["mart_social_mensagem_territorial"]["dataset_version"] == result["run_id"]
        assert latest["mart_social_canal_regiao"]["dataset_version"] == result["run_id"]
        assert latest["mart_score_alocacao_modular"]["dataset_version"] == result["run_id"]
        assert latest["mart_simulacao_orcamento"]["dataset_version"] == result["run_id"]
        assert latest["mart_recomendacao_alocacao"]["dataset_version"] == result["run_id"]
        assert latest["features_eleitorais"]["dataset_version"] == result["run_id"]
        assert latest["mart_municipio_eleitoral"]["quality"]["data_quality_score_avg"] > 0
        feature_path = Path(result["features"]["feature_path"])
        assert feature_path.exists()
        features = pd.read_parquet(feature_path)
        assert {
            "densidade_eleitoral",
            "volatilidade_historica",
            "crescimento_eleitoral",
            "abstencao",
            "competitividade",
        }.issubset(features.columns)
        assert features["data_quality_score"].between(0, 1).all()

        lake_gold_dir = gold_root
        partition_sample = list(lake_gold_dir.glob("fonte=mart_municipio_eleitoral/ano=*/uf=*/part-*.parquet"))
        assert partition_sample, "esperava particoes parquet em gold por ano/uf/fonte"
