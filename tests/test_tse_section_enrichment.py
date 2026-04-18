from __future__ import annotations

import pandas as pd

from scripts.enrich_tse_section_outputs import build_tse_ibge_crosswalk, normalize_name


def test_normalize_name_removes_accents_and_punctuation() -> None:
    assert normalize_name("São Bárbara d'Oeste") == "SAO BARBARA D OESTE"


def test_crosswalk_uses_auditable_fuzzy_match_for_luis_luiz_variant() -> None:
    master = pd.DataFrame(
        [
            {
                "uf": "SP",
                "cod_municipio_tse": "71013",
                "municipio_nome": "SÃO LUÍS DO PARAITINGA",
            }
        ]
    )
    ibge = pd.DataFrame(
        [
            {
                "cod_municipio_ibge": "3550001",
                "municipio_ibge_nome": "São Luiz do Paraitinga",
                "municipio_norm": normalize_name("São Luiz do Paraitinga"),
                "ibge_source": "fixture",
            }
        ]
    )

    crosswalk = build_tse_ibge_crosswalk(master, ibge)

    assert crosswalk.loc[0, "cod_municipio_ibge"] == "3550001"
    assert crosswalk.loc[0, "municipio_join_strategy"] == "fuzzy_normalized_name_tse_ibge"
    assert crosswalk.loc[0, "municipio_join_confidence"] == 0.94
