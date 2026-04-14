import pandas as pd

from infrastructure.territory_matching import build_alias_dimension, layered_match_territory


def test_layered_match_flags_conflicting_exact_code_for_review():
    dim = pd.DataFrame(
        [
            {
                "municipio_id_ibge7": "1111111",
                "codigo_tse": "100",
                "codigo_ibge": "1111111",
                "nome_municipio": "Cidade A",
                "municipio_norm": "cidade a",
            },
            {
                "municipio_id_ibge7": "2222222",
                "codigo_tse": "100",
                "codigo_ibge": "2222222",
                "nome_municipio": "Cidade B",
                "municipio_norm": "cidade b",
            },
        ]
    )
    aliases = build_alias_dimension(dim)
    base = pd.DataFrame([{"municipio": "Cidade A", "codigo_tse": "100", "municipio_norm_input": "cidade a"}])

    result = layered_match_territory(
        base_df=base,
        dim_municipio=dim,
        dim_alias=aliases,
        input_name_col="municipio",
        input_code_col="codigo_tse",
    )

    row = result.matched_df.iloc[0]
    assert row["join_status"] == "manual_review"
    assert row["join_method"] == "manual_review"
    assert bool(row["needs_review"]) is True
    assert row["match_conflict_reason"] == "conflicting_exact_code"
    assert len(result.review_queue_df) == 1


def test_layered_match_resolves_fuzzy_when_unambiguous():
    dim = pd.DataFrame(
        [
            {
                "municipio_id_ibge7": "3550308",
                "codigo_tse": "71072",
                "codigo_ibge": "3550308",
                "nome_municipio": "Sao Paulo",
                "municipio_norm": "sao paulo",
            },
            {
                "municipio_id_ibge7": "3509502",
                "codigo_tse": "62919",
                "codigo_ibge": "3509502",
                "nome_municipio": "Campinas",
                "municipio_norm": "campinas",
            },
        ]
    )
    aliases = build_alias_dimension(dim)
    base = pd.DataFrame([{"municipio": "Campina", "municipio_norm_input": "campina"}])

    result = layered_match_territory(base_df=base, dim_municipio=dim, dim_alias=aliases, input_name_col="municipio")

    row = result.matched_df.iloc[0]
    assert row["join_status"] == "matched"
    assert row["join_method"] == "fuzzy_score"
    assert row["municipio_id_ibge7"] == "3509502"
    assert float(row["join_confidence"]) >= 0.9
