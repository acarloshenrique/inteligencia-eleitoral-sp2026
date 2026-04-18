from __future__ import annotations

from pathlib import Path

import pandas as pd

from lakehouse.base import BaseLakehouseIngestion, BaseLakehouseTransformation, LakehouseWriteResult
from lakehouse.config import LakehouseConfig
from lakehouse.registry import build_electoral_lakehouse_catalog


def generate_example_lakehouse(root: Path) -> list[LakehouseWriteResult]:
    """Materializa exemplos minimos bronze/silver/gold para testes locais e demos tecnicas."""
    config = LakehouseConfig(root=root)
    catalog = build_electoral_lakehouse_catalog()
    results: list[LakehouseWriteResult] = []

    bronze_contract = catalog.by_id("raw_tse_resultados_secao_boletim_urna")
    silver_contract = catalog.by_id("silver_tse_resultados_secao")
    gold_contract = catalog.by_id("gold_fact_territorio_eleitoral")
    if bronze_contract is None or silver_contract is None or gold_contract is None:
        raise RuntimeError("catalogo lakehouse incompleto para exemplos")

    raw_input = root / "examples" / "raw_tse_resultados_secao.csv"
    raw_input.parent.mkdir(parents=True, exist_ok=True)
    raw_input.write_text(
        "ANO_ELEICAO,SIGLA_UF,COD_MUN_TSE,ZONA,SECAO,TURNO,ID_CANDIDATO,VOTOS_NOMINAIS\n"
        "2024,SP,71072,1,10,1,cand-demo,100\n",
        encoding="utf-8",
    )
    results.append(
        BaseLakehouseIngestion(config).preserve_raw(
            input_path=raw_input,
            contract=bronze_contract,
            dataset_version="example_v1",
            run_id="example_bronze",
            partition_values={"ANO_ELEICAO": 2024, "SIGLA_UF": "SP"},
        )
    )

    silver = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2024,
                "TURNO": 1,
                "SIGLA_UF": "SP",
                "COD_MUN_TSE": "71072",
                "COD_MUN_IBGE": "3550308",
                "MUNICIPIO": "SAO PAULO",
                "ZONA": 1,
                "SECAO": 10,
                "ID_CANDIDATO": "cand-demo",
                "VOTOS_NOMINAIS": 100,
                "join_confidence": 0.98,
                "data_quality_score": 0.95,
                "source_hash_sha256": results[0].manifest_path.name,
                "ingested_at_utc": "2026-04-17T00:00:00+00:00",
            }
        ]
    )
    transformer = BaseLakehouseTransformation(config)
    results.append(
        transformer.write_dataframe(
            df=silver,
            contract=silver_contract,
            dataset_version="example_v1",
            run_id="example_silver",
            inputs=[str(results[0].output_path)],
            partition_values={"ANO_ELEICAO": 2024, "SIGLA_UF": "SP"},
            operation="normalize_tse_section_example",
            business_rule="Normalizar chaves TSE e adicionar qualidade de join para exemplo tecnico.",
        )
    )

    gold = pd.DataFrame(
        [
            {
                "territorio_id": "2024:SP:71072:ZE1:S10",
                "ANO_ELEICAO": 2024,
                "SIGLA_UF": "SP",
                "COD_MUN_TSE": "71072",
                "COD_MUN_IBGE": "3550308",
                "MUNICIPIO": "SAO PAULO",
                "ZONA": 1,
                "SECAO": 10,
                "LOCAL_VOTACAO": "ESCOLA DEMO",
                "CD_SETOR": "355030800001",
                "eleitores_aptos": 1000,
                "votos_validos": 700,
                "abstencao_pct": 0.22,
                "competitividade": 0.71,
                "join_confidence": 0.96,
                "data_quality_score": 0.94,
            }
        ]
    )
    results.append(
        transformer.write_dataframe(
            df=gold,
            contract=gold_contract,
            dataset_version="example_v1",
            run_id="example_gold",
            inputs=[str(results[1].output_path)],
            partition_values={"ANO_ELEICAO": 2024, "SIGLA_UF": "SP"},
            operation="build_gold_territory_example",
            business_rule="Consolidar exemplo gold territorial para scoring e dashboards.",
        )
    )
    return results
