import pandas as pd

from config.settings import AppPaths
from infrastructure.tse_zone_pipeline import (
    TSEZoneInputs,
    normalize_tse_eleitorado_zona,
    normalize_tse_resultados_zona,
    run_tse_zone_section_pipeline,
)


def _paths(tmp_path):
    lake = tmp_path / "data_lake"
    gold = lake / "gold"
    silver = lake / "silver"
    bronze = lake / "bronze"
    catalog = lake / "catalog"
    for folder in [
        tmp_path / "ingestion",
        bronze,
        silver,
        gold,
        gold / "reports",
        gold / "serving",
        catalog,
        tmp_path / "chromadb",
        tmp_path / "metadata",
        tmp_path / "artifacts",
    ]:
        folder.mkdir(parents=True, exist_ok=True)
    return AppPaths(
        data_root=tmp_path,
        ingestion_root=tmp_path / "ingestion",
        lake_root=lake,
        bronze_root=bronze,
        silver_root=silver,
        gold_root=gold,
        gold_reports_root=gold / "reports",
        gold_serving_root=gold / "serving",
        catalog_root=catalog,
        chromadb_path=tmp_path / "chromadb",
        runtime_reports_root=tmp_path / "runtime_reports",
        ts="20260415_000000",
        metadata_db_path=tmp_path / "metadata" / "jobs.sqlite3",
        artifact_root=tmp_path / "artifacts",
        tenant_id="tenant_a",
        tenant_root=tmp_path,
    )


def test_normalize_tse_eleitorado_zona_secao():
    raw = pd.DataFrame(
        [
            {
                "SG_UF": "SP",
                "NM_MUNICIPIO": "CIDADE A",
                "CD_MUNICIPIO": "71072",
                "NR_ZONA": "10",
                "NR_SECAO": "1",
                "QT_ELEITOR": "100",
            },
            {
                "SG_UF": "SP",
                "NM_MUNICIPIO": "CIDADE A",
                "CD_MUNICIPIO": "71072",
                "NR_ZONA": "10",
                "NR_SECAO": "2",
                "QT_ELEITOR": "150",
            },
            {
                "SG_UF": "RJ",
                "NM_MUNICIPIO": "CIDADE X",
                "CD_MUNICIPIO": "1",
                "NR_ZONA": "9",
                "NR_SECAO": "1",
                "QT_ELEITOR": "999",
            },
        ]
    )

    zone, section = normalize_tse_eleitorado_zona(raw, uf="SP", ano_eleicao=2024, turno=1)

    assert len(section) == 2
    assert len(zone) == 1
    assert zone.loc[0, "eleitores_aptos"] == 250
    assert zone.loc[0, "secoes_total"] == 2
    assert zone.loc[0, "join_confidence"] >= 0.9


def test_normalize_tse_resultados_filtra_eleicao_regular_prefeito_e_preserva_candidatos():
    raw = pd.DataFrame(
        [
            {
                "SG_UF": "SP",
                "ANO_ELEICAO": "2024",
                "NR_TURNO": "1",
                "CD_ELEICAO": "619",
                "NM_MUNICIPIO": "SANTA B?RBARA D OESTE",
                "CD_MUNICIPIO": "70173",
                "NR_ZONA": "186",
                "NR_SECAO": "1",
                "CD_CARGO": "11",
                "SQ_CANDIDATO": "1",
                "QT_VOTOS": "60",
            },
            {
                "SG_UF": "SP",
                "ANO_ELEICAO": "2024",
                "NR_TURNO": "1",
                "CD_ELEICAO": "619",
                "NM_MUNICIPIO": "SANTA B?RBARA D OESTE",
                "CD_MUNICIPIO": "70173",
                "NR_ZONA": "186",
                "NR_SECAO": "1",
                "CD_CARGO": "11",
                "SQ_CANDIDATO": "2",
                "QT_VOTOS": "40",
            },
            {
                "SG_UF": "SP",
                "ANO_ELEICAO": "2024",
                "NR_TURNO": "1",
                "CD_ELEICAO": "619",
                "NM_MUNICIPIO": "SANTA B?RBARA D OESTE",
                "CD_MUNICIPIO": "70173",
                "NR_ZONA": "186",
                "NR_SECAO": "1",
                "CD_CARGO": "13",
                "SQ_CANDIDATO": "3",
                "QT_VOTOS": "999",
            },
            {
                "SG_UF": "SP",
                "ANO_ELEICAO": "2024",
                "NR_TURNO": "1",
                "CD_ELEICAO": "6161",
                "NM_MUNICIPIO": "SANTA B?RBARA D OESTE",
                "CD_MUNICIPIO": "70173",
                "NR_ZONA": "186",
                "NR_SECAO": "1",
                "CD_CARGO": "11",
                "SQ_CANDIDATO": "1",
                "QT_VOTOS": "888",
            },
        ]
    )

    out = normalize_tse_resultados_zona(raw, uf="SP", ano_eleicao=2024, turno=1)

    assert sorted(out["votavel_id"].tolist()) == ["1", "2"]
    assert out["votos_validos"].sum() == 100
    assert out["cod_tse_municipio"].unique().tolist() == ["70173"]


def test_run_tse_zone_section_pipeline_publishes_gold_and_silver(tmp_path):
    paths = _paths(tmp_path)
    eleitorado = tmp_path / "eleitorado.csv"
    eleitorado.write_text(
        "SG_UF;NM_MUNICIPIO;CD_MUNICIPIO;NR_ZONA;NR_SECAO;QT_ELEITOR\n"
        "SP;CIDADE A;71072;10;1;100\n"
        "SP;CIDADE A;71072;10;2;150\n",
        encoding="latin1",
    )

    result = run_tse_zone_section_pipeline(
        paths=paths,
        inputs=TSEZoneInputs(eleitorado_path=eleitorado, uf="SP", ano_eleicao=2024, turno=1),
    )

    assert result["quality"]["rows_zona"] == 1
    assert result["quality"]["rows_secao"] == 2
    assert (paths.gold_root / f"fact_zona_eleitoral_{result['run_id']}.parquet").exists()
    assert (paths.gold_root / f"features_zona_eleitoral_{result['run_id']}.parquet").exists()
    assert (paths.silver_root / f"dim_territorio_eleitoral_{result['run_id']}.parquet").exists()
