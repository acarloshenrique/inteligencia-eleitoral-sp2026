from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5
from zipfile import ZipFile

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


MASTER_COLUMNS = [
    "master_record_id",
    "ano_eleicao",
    "uf",
    "cod_municipio_tse",
    "cod_municipio_ibge",
    "municipio_nome",
    "zona",
    "secao",
    "local_votacao",
    "candidate_id",
    "numero_candidato",
    "partido",
    "cd_setor",
    "territorial_cluster_id",
    "join_strategy",
    "join_confidence",
    "source_coverage_score",
]


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def digits(value: object) -> str:
    if pd.isna(value):
        return ""
    return "".join(ch for ch in str(value) if ch.isdigit())


def pad(value: object, width: int) -> str:
    raw = digits(value)
    return raw.zfill(width) if raw else ""


def zip_csv_name(path: Path) -> str:
    with ZipFile(path) as zf:
        return next(name for name in zf.namelist() if name.lower().endswith(".csv"))


def read_zip_chunks(path: Path, *, usecols: list[str], chunksize: int) -> pd.io.parsers.TextFileReader:
    csv_name = zip_csv_name(path)
    zf = ZipFile(path)
    handle = zf.open(csv_name)
    reader = pd.read_csv(
        handle,
        sep=";",
        encoding="latin1",
        dtype=str,
        usecols=usecols,
        chunksize=chunksize,
    )
    reader._tse_zip_handle = handle
    reader._tse_zip_file = zf
    return reader


def close_reader(reader: pd.io.parsers.TextFileReader) -> None:
    handle = getattr(reader, "_tse_zip_handle", None)
    zip_file = getattr(reader, "_tse_zip_file", None)
    if handle is not None:
        handle.close()
    if zip_file is not None:
        zip_file.close()


def canonical_section_keys(df: pd.DataFrame, *, year_col: str) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    out["ano_eleicao"] = pd.to_numeric(df[year_col], errors="coerce").fillna(0).astype("int64")
    out["uf"] = df["SG_UF"].astype(str).str.upper().str.strip()
    out["cod_municipio_tse"] = df["CD_MUNICIPIO"].map(lambda value: pad(value, 5))
    out["municipio_nome"] = df["NM_MUNICIPIO"].astype(str).str.strip()
    out["zona"] = df["NR_ZONA"].map(lambda value: pad(value, 4))
    out["secao"] = df["NR_SECAO"].map(lambda value: pad(value, 4))
    return out


def aggregate_votacao(path: Path, *, chunksize: int) -> pd.DataFrame:
    usecols = [
        "ANO_ELEICAO",
        "SG_UF",
        "CD_MUNICIPIO",
        "NM_MUNICIPIO",
        "NR_ZONA",
        "NR_SECAO",
        "NR_LOCAL_VOTACAO",
        "NM_LOCAL_VOTACAO",
        "QT_VOTOS",
    ]
    parts: list[pd.DataFrame] = []
    reader = read_zip_chunks(path, usecols=usecols, chunksize=chunksize)
    try:
        for chunk in reader:
            keys = canonical_section_keys(chunk, year_col="ANO_ELEICAO")
            keys["nr_local_votacao_votacao"] = chunk["NR_LOCAL_VOTACAO"].map(lambda value: pad(value, 4))
            keys["local_votacao_votacao"] = chunk["NM_LOCAL_VOTACAO"].astype(str).str.strip()
            keys["votos_total"] = pd.to_numeric(chunk["QT_VOTOS"], errors="coerce").fillna(0).astype("int64")
            grouped = (
                keys.groupby(
                    [
                        "ano_eleicao",
                        "uf",
                        "cod_municipio_tse",
                        "municipio_nome",
                        "zona",
                        "secao",
                        "nr_local_votacao_votacao",
                        "local_votacao_votacao",
                    ],
                    dropna=False,
                )
                .agg(votos_total=("votos_total", "sum"), votacao_linhas=("votos_total", "size"))
                .reset_index()
            )
            parts.append(grouped)
    finally:
        close_reader(reader)
    merged = pd.concat(parts, ignore_index=True)
    return (
        merged.groupby(
            [
                "ano_eleicao",
                "uf",
                "cod_municipio_tse",
                "municipio_nome",
                "zona",
                "secao",
                "nr_local_votacao_votacao",
                "local_votacao_votacao",
            ],
            dropna=False,
        )
        .agg(votos_total=("votos_total", "sum"), votacao_linhas=("votacao_linhas", "sum"))
        .reset_index()
    )


def aggregate_perfil(path: Path, *, chunksize: int) -> pd.DataFrame:
    usecols = [
        "ANO_ELEICAO",
        "SG_UF",
        "CD_MUNICIPIO",
        "NM_MUNICIPIO",
        "NR_ZONA",
        "NR_SECAO",
        "NR_LOCAL_VOTACAO",
        "NM_LOCAL_VOTACAO",
        "QT_ELEITORES_PERFIL",
        "QT_ELEITORES_BIOMETRIA",
        "QT_ELEITORES_DEFICIENCIA",
    ]
    parts: list[pd.DataFrame] = []
    reader = read_zip_chunks(path, usecols=usecols, chunksize=chunksize)
    try:
        for chunk in reader:
            keys = canonical_section_keys(chunk, year_col="ANO_ELEICAO")
            keys["nr_local_votacao_perfil"] = chunk["NR_LOCAL_VOTACAO"].map(lambda value: pad(value, 4))
            keys["local_votacao_perfil"] = chunk["NM_LOCAL_VOTACAO"].astype(str).str.strip()
            keys["eleitores_perfil"] = pd.to_numeric(chunk["QT_ELEITORES_PERFIL"], errors="coerce").fillna(0)
            keys["eleitores_biometria"] = pd.to_numeric(chunk["QT_ELEITORES_BIOMETRIA"], errors="coerce").fillna(0)
            keys["eleitores_deficiencia"] = pd.to_numeric(chunk["QT_ELEITORES_DEFICIENCIA"], errors="coerce").fillna(0)
            parts.append(
                keys.groupby(
                    [
                        "ano_eleicao",
                        "uf",
                        "cod_municipio_tse",
                        "municipio_nome",
                        "zona",
                        "secao",
                        "nr_local_votacao_perfil",
                        "local_votacao_perfil",
                    ],
                    dropna=False,
                )
                .sum(numeric_only=True)
                .reset_index()
            )
    finally:
        close_reader(reader)
    merged = pd.concat(parts, ignore_index=True)
    return (
        merged.groupby(
            [
                "ano_eleicao",
                "uf",
                "cod_municipio_tse",
                "municipio_nome",
                "zona",
                "secao",
                "nr_local_votacao_perfil",
                "local_votacao_perfil",
            ],
            dropna=False,
        )
        .sum(numeric_only=True)
        .reset_index()
    )


def aggregate_locais(path: Path, *, chunksize: int, uf: str) -> pd.DataFrame:
    usecols = [
        "AA_ELEICAO",
        "SG_UF",
        "CD_MUNICIPIO",
        "NM_MUNICIPIO",
        "NR_ZONA",
        "NR_SECAO",
        "NR_LOCAL_VOTACAO",
        "NM_LOCAL_VOTACAO",
        "DS_ENDERECO",
        "NM_BAIRRO",
        "NR_CEP",
        "NR_LATITUDE",
        "NR_LONGITUDE",
        "QT_ELEITOR_SECAO",
    ]
    parts: list[pd.DataFrame] = []
    reader = read_zip_chunks(path, usecols=usecols, chunksize=chunksize)
    try:
        for chunk in reader:
            chunk = chunk[chunk["SG_UF"].astype(str).str.upper().eq(uf)]
            if chunk.empty:
                continue
            keys = canonical_section_keys(chunk, year_col="AA_ELEICAO")
            keys["nr_local_votacao"] = chunk["NR_LOCAL_VOTACAO"].map(lambda value: pad(value, 4))
            keys["local_votacao"] = chunk["NM_LOCAL_VOTACAO"].astype(str).str.strip()
            keys["endereco"] = chunk["DS_ENDERECO"].astype(str).str.strip()
            keys["bairro"] = chunk["NM_BAIRRO"].astype(str).str.strip()
            keys["cep"] = chunk["NR_CEP"].map(digits)
            keys["latitude"] = pd.to_numeric(chunk["NR_LATITUDE"].str.replace(",", ".", regex=False), errors="coerce")
            keys["longitude"] = pd.to_numeric(chunk["NR_LONGITUDE"].str.replace(",", ".", regex=False), errors="coerce")
            keys["eleitores_local"] = pd.to_numeric(chunk["QT_ELEITOR_SECAO"], errors="coerce").fillna(0)
            parts.append(keys)
    finally:
        close_reader(reader)
    if not parts:
        return pd.DataFrame()
    merged = pd.concat(parts, ignore_index=True)
    return (
        merged.sort_values(["ano_eleicao", "uf", "cod_municipio_tse", "zona", "secao"])
        .drop_duplicates(["ano_eleicao", "uf", "cod_municipio_tse", "zona", "secao"], keep="first")
        .reset_index(drop=True)
    )


def build_master(votacao: pd.DataFrame, perfil: pd.DataFrame, locais: pd.DataFrame) -> pd.DataFrame:
    keys = ["ano_eleicao", "uf", "cod_municipio_tse", "zona", "secao"]
    base = locais.merge(
        votacao.drop(columns=["municipio_nome"], errors="ignore"),
        on=keys,
        how="outer",
    )
    base = base.merge(
        perfil.drop(columns=["municipio_nome"], errors="ignore"),
        on=keys,
        how="outer",
    )
    base["municipio_nome"] = base["municipio_nome"].fillna("").astype(str)
    for column in ["local_votacao", "local_votacao_votacao", "local_votacao_perfil"]:
        if column not in base.columns:
            base[column] = ""
    base["local_votacao"] = base["local_votacao"].where(
        base["local_votacao"].astype(str).str.len().gt(0), base["local_votacao_votacao"]
    )
    base["local_votacao"] = base["local_votacao"].where(
        base["local_votacao"].astype(str).str.len().gt(0), base["local_votacao_perfil"]
    )
    base["candidate_id"] = "aggregate"
    base["numero_candidato"] = ""
    base["partido"] = ""
    base["cod_municipio_ibge"] = ""
    base["cd_setor"] = ""
    base["territorial_cluster_id"] = (
        base["uf"].astype(str) + ":" + base["cod_municipio_tse"].astype(str) + ":Z" + base["zona"].astype(str)
    )
    has_vote = base["votos_total"].fillna(0).gt(0) if "votos_total" in base else pd.Series(False, index=base.index)
    has_profile = (
        base["eleitores_perfil"].fillna(0).gt(0) if "eleitores_perfil" in base else pd.Series(False, index=base.index)
    )
    has_local = (
        base["eleitores_local"].fillna(0).gt(0) if "eleitores_local" in base else pd.Series(False, index=base.index)
    )
    base["join_strategy"] = "exact_section_tse_votacao_perfil_local"
    base.loc[~has_vote, "join_strategy"] = "exact_section_tse_perfil_local_no_votes"
    base.loc[~has_profile, "join_strategy"] = "exact_section_tse_votacao_local_no_profile"
    base["join_confidence"] = (
        0.40 * has_vote.astype(float) + 0.35 * has_profile.astype(float) + 0.25 * has_local.astype(float)
    ).round(6)
    base["source_coverage_score"] = base["join_confidence"]
    base["master_record_id"] = base.apply(record_id, axis=1)
    ordered = base[MASTER_COLUMNS + [column for column in base.columns if column not in MASTER_COLUMNS]].copy()
    return ordered.drop_duplicates("master_record_id").reset_index(drop=True)


def record_id(row: pd.Series) -> str:
    raw = "|".join(str(row.get(column, "")) for column in MASTER_COLUMNS if column != "master_record_id")
    return str(uuid5(NAMESPACE_URL, f"tse-section-master:{raw}"))


def write_outputs(
    master: pd.DataFrame, output_dir: Path, dataset_version: str, sources: dict[str, str]
) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = output_dir / f"gold_territorial_electoral_master_index_{dataset_version}.parquet"
    manifest_path = output_dir / f"gold_territorial_electoral_master_index_{dataset_version}_manifest.json"
    coverage_path = output_dir / f"gold_territorial_electoral_master_index_{dataset_version}_coverage.json"
    master.to_parquet(parquet_path, index=False)
    coverage = {
        "dataset_id": "gold_territorial_electoral_master_index",
        "dataset_version": dataset_version,
        "rows": int(len(master)),
        "municipios": int(master["cod_municipio_tse"].nunique()),
        "zonas": int(master[["cod_municipio_tse", "zona"]].drop_duplicates().shape[0]),
        "secoes": int(master[["cod_municipio_tse", "zona", "secao"]].drop_duplicates().shape[0]),
        "coverage_local_votacao": round(float(master["local_votacao"].astype(str).str.len().gt(0).mean()), 6),
        "coverage_ibge": 0.0,
        "coverage_setor": 0.0,
        "mean_join_confidence": round(float(pd.to_numeric(master["join_confidence"], errors="coerce").mean()), 6),
        "limitations": [
            "candidate_id is aggregated in this section master; candidate-section fact should be materialized separately for candidate-level vote analysis.",
            "cod_municipio_ibge is not present in the provided TSE ZIPs and requires a governed TSE-IBGE crosswalk.",
            "cd_setor is not present in TSE ZIPs and requires geocoding or explicit local-to-sector crosswalk.",
        ],
    }
    manifest = {
        "created_at_utc": utc_now_iso(),
        "sources": sources,
        "parquet_path": str(parquet_path),
        "coverage_path": str(coverage_path),
        "schema": {column: str(dtype) for column, dtype in master.dtypes.items()},
        "coverage": coverage,
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    coverage_path.write_text(json.dumps(coverage, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"parquet_path": str(parquet_path), "manifest_path": str(manifest_path), "coverage": coverage}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build real TSE section-level master index from local ZIP files.")
    parser.add_argument("--votacao-zip", type=Path, required=True)
    parser.add_argument("--perfil-zip", type=Path, required=True)
    parser.add_argument("--locais-zip", type=Path, required=True)
    parser.add_argument("--dataset-version", default="tse2024_sp_secao_real")
    parser.add_argument("--uf", default="SP")
    parser.add_argument("--chunksize", type=int, default=350000)
    parser.add_argument("--output-dir", type=Path, default=Path("lake/gold/territorial_electoral_master_index"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    votacao = aggregate_votacao(args.votacao_zip, chunksize=args.chunksize)
    perfil = aggregate_perfil(args.perfil_zip, chunksize=args.chunksize)
    locais = aggregate_locais(args.locais_zip, chunksize=args.chunksize, uf=args.uf.upper())
    master = build_master(votacao=votacao, perfil=perfil, locais=locais)
    result = write_outputs(
        master,
        args.output_dir,
        args.dataset_version,
        sources={
            "votacao_secao": str(args.votacao_zip),
            "perfil_eleitor_secao": str(args.perfil_zip),
            "eleitorado_local_votacao": str(args.locais_zip),
        },
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
