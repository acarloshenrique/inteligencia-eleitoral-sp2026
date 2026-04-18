# ruff: noqa: S608,I001

from __future__ import annotations

import argparse
import difflib
import json
import re
import shutil
import sys
import unicodedata
from datetime import UTC, datetime
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.build_tse_section_master_index import close_reader, pad, read_zip_chunks


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def normalize_name(value: object) -> str:
    text = "" if pd.isna(value) else str(value)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = re.sub(r"[^A-Za-z0-9]+", " ", text.upper())
    return re.sub(r"\s+", " ", text).strip()


def duckdb_safe_path(path: Path) -> str:
    resolved = str(path).replace("\\", "/")
    if "'" in resolved:
        raise ValueError(f"Path not safe for DuckDB SQL literal: {path}")
    return resolved


def read_ibge_municipios(path: Path) -> pd.DataFrame:
    records = json.loads(path.read_text(encoding="utf-8"))
    return pd.DataFrame(
        {
            "cod_municipio_ibge": [str(item["id"]) for item in records],
            "municipio_ibge_nome": [str(item["nome"]) for item in records],
            "municipio_norm": [normalize_name(item["nome"]) for item in records],
            "ibge_source": str(path),
        }
    )


def build_tse_ibge_crosswalk(master: pd.DataFrame, ibge_municipios: pd.DataFrame) -> pd.DataFrame:
    tse = (
        master[["uf", "cod_municipio_tse", "municipio_nome"]]
        .drop_duplicates()
        .assign(municipio_norm=lambda df: df["municipio_nome"].map(normalize_name))
    )
    crosswalk = tse.merge(ibge_municipios, on="municipio_norm", how="left")
    ibge_by_norm = {str(row["municipio_norm"]): row for _, row in ibge_municipios.iterrows()}
    missing = crosswalk["cod_municipio_ibge"].isna() | crosswalk["cod_municipio_ibge"].astype(str).str.strip().eq("")
    for index, row in crosswalk[missing].iterrows():
        candidates = difflib.get_close_matches(str(row["municipio_norm"]), list(ibge_by_norm), n=1, cutoff=0.92)
        if not candidates:
            continue
        match = ibge_by_norm[candidates[0]]
        crosswalk.loc[index, "cod_municipio_ibge"] = str(match["cod_municipio_ibge"])
        crosswalk.loc[index, "municipio_ibge_nome"] = str(match["municipio_ibge_nome"])
        crosswalk.loc[index, "ibge_source"] = str(match["ibge_source"])
    matched = crosswalk["cod_municipio_ibge"].notna() & crosswalk["cod_municipio_ibge"].astype(str).str.strip().ne("")
    exact = matched & crosswalk["municipio_norm"].eq(crosswalk["municipio_ibge_nome"].map(normalize_name))
    crosswalk["municipio_join_strategy"] = "not_matched"
    crosswalk.loc[exact, "municipio_join_strategy"] = "exact_normalized_name_tse_ibge"
    crosswalk.loc[matched & ~exact, "municipio_join_strategy"] = "fuzzy_normalized_name_tse_ibge"
    crosswalk["municipio_join_confidence"] = 0.0
    crosswalk.loc[exact, "municipio_join_confidence"] = 1.0
    crosswalk.loc[matched & ~exact, "municipio_join_confidence"] = 0.94
    crosswalk["municipio_join_notes"] = crosswalk.apply(
        lambda row: (
            ""
            if pd.notna(row.get("cod_municipio_ibge")) and str(row.get("cod_municipio_ibge", "")).strip()
            else f"Sem correspondencia exata normalizada para {row['municipio_nome']}"
        ),
        axis=1,
    )
    return crosswalk.sort_values(["uf", "municipio_nome"]).reset_index(drop=True)


def load_sector_crosswalk(path: Path | None) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame()
    if path.suffix.lower() == ".parquet":
        frame = pd.read_parquet(path)
    elif path.suffix.lower() == ".json":
        frame = pd.read_json(path)
    else:
        frame = pd.read_csv(path, dtype=str)
    required = {"cod_municipio_tse", "zona", "secao", "cd_setor"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"Sector crosswalk missing required columns: {sorted(missing)}")
    out = frame.copy()
    out["cod_municipio_tse"] = out["cod_municipio_tse"].astype(str).str.zfill(5)
    out["zona"] = out["zona"].astype(str).str.zfill(4)
    out["secao"] = out["secao"].astype(str).str.zfill(4)
    out["cd_setor"] = out["cd_setor"].astype(str).str.strip()
    return out.drop_duplicates(["cod_municipio_tse", "zona", "secao"])


def enrich_master(
    *,
    master_path: Path,
    ibge_municipios_path: Path,
    output_dir: Path,
    dataset_version: str,
    sector_crosswalk_path: Path | None = None,
) -> dict[str, object]:
    master = pd.read_parquet(master_path)
    ibge_municipios = read_ibge_municipios(ibge_municipios_path)
    crosswalk = build_tse_ibge_crosswalk(master, ibge_municipios)
    crosswalk_path = output_dir / f"crosswalk_tse_ibge_municipios_sp_{dataset_version}.parquet"
    crosswalk_manifest_path = output_dir / f"crosswalk_tse_ibge_municipios_sp_{dataset_version}_manifest.json"
    output_dir.mkdir(parents=True, exist_ok=True)
    crosswalk.to_parquet(crosswalk_path, index=False)

    enriched = master.drop(columns=["cod_municipio_ibge"], errors="ignore").merge(
        crosswalk[
            [
                "uf",
                "cod_municipio_tse",
                "cod_municipio_ibge",
                "municipio_ibge_nome",
                "municipio_join_strategy",
                "municipio_join_confidence",
                "municipio_join_notes",
            ]
        ],
        on=["uf", "cod_municipio_tse"],
        how="left",
    )
    sector_crosswalk = load_sector_crosswalk(sector_crosswalk_path)
    if sector_crosswalk.empty:
        enriched["cd_setor"] = enriched.get("cd_setor", "").fillna("").astype(str)
        enriched["setor_join_strategy"] = "not_available_requires_explicit_local_to_sector_crosswalk"
        enriched["setor_join_confidence"] = 0.0
    else:
        enriched = enriched.drop(columns=["cd_setor"], errors="ignore").merge(
            sector_crosswalk[["cod_municipio_tse", "zona", "secao", "cd_setor"]],
            on=["cod_municipio_tse", "zona", "secao"],
            how="left",
        )
        enriched["cd_setor"] = enriched["cd_setor"].fillna("").astype(str)
        enriched["setor_join_strategy"] = enriched["cd_setor"].map(
            lambda value: "exact_section_sector_crosswalk" if str(value).strip() else "not_matched"
        )
        enriched["setor_join_confidence"] = enriched["cd_setor"].map(lambda value: 1.0 if str(value).strip() else 0.0)

    enriched["municipio_join_confidence"] = pd.to_numeric(
        enriched["municipio_join_confidence"], errors="coerce"
    ).fillna(0.0)
    original_confidence = pd.to_numeric(enriched["join_confidence"], errors="coerce").fillna(0.0)
    enriched["join_confidence"] = (0.85 * original_confidence + 0.15 * enriched["municipio_join_confidence"]).round(6)
    enriched["source_coverage_score"] = enriched["join_confidence"]
    enriched["join_strategy"] = (
        enriched["join_strategy"].astype(str)
        + ";"
        + enriched["municipio_join_strategy"].astype(str)
        + ";"
        + enriched["setor_join_strategy"].astype(str)
    )
    enriched["master_record_id"] = enriched.apply(
        lambda row: str(
            uuid5(
                NAMESPACE_URL,
                "tse-section-master-enriched:"
                + "|".join(
                    str(row.get(column, ""))
                    for column in [
                        "ano_eleicao",
                        "uf",
                        "cod_municipio_tse",
                        "cod_municipio_ibge",
                        "zona",
                        "secao",
                        "local_votacao",
                        "candidate_id",
                        "cd_setor",
                    ]
                ),
            )
        ),
        axis=1,
    )
    parquet_path = output_dir / f"gold_territorial_electoral_master_index_{dataset_version}.parquet"
    manifest_path = output_dir / f"gold_territorial_electoral_master_index_{dataset_version}_manifest.json"
    coverage_path = output_dir / f"gold_territorial_electoral_master_index_{dataset_version}_coverage.json"
    enriched.to_parquet(parquet_path, index=False)
    coverage = master_coverage(enriched, dataset_version=dataset_version)
    coverage_path.write_text(json.dumps(coverage, ensure_ascii=False, indent=2), encoding="utf-8")
    manifest = {
        "created_at_utc": utc_now_iso(),
        "dataset_id": "gold_territorial_electoral_master_index",
        "dataset_version": dataset_version,
        "source_master_path": str(master_path),
        "ibge_municipios_path": str(ibge_municipios_path),
        "sector_crosswalk_path": str(sector_crosswalk_path) if sector_crosswalk_path else "",
        "parquet_path": str(parquet_path),
        "coverage_path": str(coverage_path),
        "schema": {column: str(dtype) for column, dtype in enriched.dtypes.items()},
        "coverage": coverage,
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    crosswalk_manifest = {
        "created_at_utc": utc_now_iso(),
        "dataset_id": "crosswalk_tse_ibge_municipios_sp",
        "dataset_version": dataset_version,
        "source_master_path": str(master_path),
        "ibge_municipios_path": str(ibge_municipios_path),
        "parquet_path": str(crosswalk_path),
        "rows": int(len(crosswalk)),
        "coverage_ibge": round(float(crosswalk["cod_municipio_ibge"].astype(str).str.len().gt(0).mean()), 6),
        "schema": {column: str(dtype) for column, dtype in crosswalk.dtypes.items()},
    }
    crosswalk_manifest_path.write_text(json.dumps(crosswalk_manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "master_parquet_path": str(parquet_path),
        "master_manifest_path": str(manifest_path),
        "crosswalk_path": str(crosswalk_path),
        "crosswalk_manifest_path": str(crosswalk_manifest_path),
        "coverage": coverage,
    }


def master_coverage(master: pd.DataFrame, *, dataset_version: str) -> dict[str, object]:
    coverage_ibge = round(float(master["cod_municipio_ibge"].fillna("").astype(str).str.len().gt(0).mean()), 6)
    coverage_setor = round(float(master["cd_setor"].fillna("").astype(str).str.len().gt(0).mean()), 6)
    limitations = []
    if coverage_setor == 0:
        limitations.append(
            "cd_setor permanece nao inferido: os ZIPs TSE nao contem setor censitario; preencher exige malha/setores e geocoding ou crosswalk local-secao-setor explicito."
        )
    return {
        "dataset_id": "gold_territorial_electoral_master_index",
        "dataset_version": dataset_version,
        "rows": int(len(master)),
        "municipios_tse": int(master["cod_municipio_tse"].nunique()),
        "municipios_ibge": int(master["cod_municipio_ibge"].fillna("").astype(str).replace("", pd.NA).nunique()),
        "zonas": int(master[["cod_municipio_tse", "zona"]].drop_duplicates().shape[0]),
        "secoes": int(master[["cod_municipio_tse", "zona", "secao"]].drop_duplicates().shape[0]),
        "coverage_local_votacao": round(float(master["local_votacao"].astype(str).str.len().gt(0).mean()), 6),
        "coverage_ibge": coverage_ibge,
        "coverage_setor": coverage_setor,
        "mean_join_confidence": round(float(pd.to_numeric(master["join_confidence"], errors="coerce").mean()), 6),
        "municipio_join_strategies": master["municipio_join_strategy"].value_counts(dropna=False).to_dict(),
        "setor_join_strategies": master["setor_join_strategy"].value_counts(dropna=False).to_dict(),
        "limitations": limitations,
    }


def canonical_vote_chunk(chunk: pd.DataFrame, ibge_lookup: dict[str, str]) -> pd.DataFrame:
    out = pd.DataFrame(index=chunk.index)
    out["ano_eleicao"] = pd.to_numeric(chunk["ANO_ELEICAO"], errors="coerce").fillna(0).astype("int64")
    out["turno"] = pd.to_numeric(chunk["NR_TURNO"], errors="coerce").fillna(0).astype("int64")
    out["uf"] = chunk["SG_UF"].astype(str).str.upper().str.strip()
    out["cod_municipio_tse"] = chunk["CD_MUNICIPIO"].map(lambda value: pad(value, 5))
    out["cod_municipio_ibge"] = out["cod_municipio_tse"].map(ibge_lookup).fillna("").astype(str)
    out.loc[out["cod_municipio_ibge"].str.lower().isin(["none", "nan", "<na>"]), "cod_municipio_ibge"] = ""
    out["municipio_nome"] = chunk["NM_MUNICIPIO"].astype(str).str.strip()
    out["zona"] = chunk["NR_ZONA"].map(lambda value: pad(value, 4))
    out["secao"] = chunk["NR_SECAO"].map(lambda value: pad(value, 4))
    out["nr_local_votacao"] = chunk["NR_LOCAL_VOTACAO"].map(lambda value: pad(value, 4))
    out["local_votacao"] = chunk["NM_LOCAL_VOTACAO"].astype(str).str.strip()
    out["cargo_codigo"] = chunk["CD_CARGO"].astype(str).str.strip()
    out["cargo"] = chunk["DS_CARGO"].astype(str).str.strip()
    out["candidate_id"] = chunk["SQ_CANDIDATO"].astype(str).str.strip()
    out["numero_candidato"] = chunk["NR_VOTAVEL"].astype(str).str.strip()
    out["nome_votavel"] = chunk["NM_VOTAVEL"].astype(str).str.strip()
    out["votos"] = pd.to_numeric(chunk["QT_VOTOS"], errors="coerce").fillna(0).astype("int64")
    out["candidate_id"] = out.apply(
        lambda row: (
            f"special:{row['numero_candidato']}:{normalize_name(row['nome_votavel'])}"
            if row["candidate_id"] in {"", "-1", "-3"}
            else row["candidate_id"]
        ),
        axis=1,
    )
    out["territorio_id"] = (
        out["ano_eleicao"].astype(str)
        + ":"
        + out["uf"]
        + ":"
        + out["cod_municipio_tse"]
        + ":Z"
        + out["zona"]
        + ":S"
        + out["secao"]
    )
    out["candidate_section_record_id"] = out.apply(
        lambda row: str(
            uuid5(
                NAMESPACE_URL,
                "tse-candidate-section:"
                + "|".join(
                    str(row[column])
                    for column in [
                        "ano_eleicao",
                        "turno",
                        "uf",
                        "cod_municipio_tse",
                        "zona",
                        "secao",
                        "cargo_codigo",
                        "candidate_id",
                        "numero_candidato",
                    ]
                ),
            )
        ),
        axis=1,
    )
    out["join_strategy"] = "exact_tse_section_vote;exact_tse_ibge_crosswalk"
    out["join_confidence"] = out["cod_municipio_ibge"].map(lambda value: 1.0 if str(value).strip() else 0.85)
    out["source_dataset"] = "votacao_secao_2024_SP"
    return out


def build_candidate_section_fact(
    *,
    votacao_zip: Path,
    crosswalk_path: Path,
    output_dir: Path,
    dataset_version: str,
    chunksize: int,
    uf: str,
) -> dict[str, object]:
    crosswalk = pd.read_parquet(crosswalk_path)
    ibge_lookup = dict(
        zip(crosswalk["cod_municipio_tse"].astype(str), crosswalk["cod_municipio_ibge"].astype(str), strict=True)
    )
    usecols = [
        "ANO_ELEICAO",
        "NR_TURNO",
        "SG_UF",
        "CD_MUNICIPIO",
        "NM_MUNICIPIO",
        "NR_ZONA",
        "NR_SECAO",
        "CD_CARGO",
        "DS_CARGO",
        "NR_VOTAVEL",
        "NM_VOTAVEL",
        "QT_VOTOS",
        "NR_LOCAL_VOTACAO",
        "SQ_CANDIDATO",
        "NM_LOCAL_VOTACAO",
    ]
    temp_dir = output_dir / f"_parts_{dataset_version}"
    existing_parts = sorted(temp_dir.glob("part_*.parquet")) if temp_dir.exists() else []
    part_paths: list[Path] = existing_parts.copy()
    if temp_dir.exists() and not existing_parts:
        shutil.rmtree(temp_dir)
    if not part_paths:
        temp_dir.mkdir(parents=True, exist_ok=True)
        reader = read_zip_chunks(votacao_zip, usecols=usecols, chunksize=chunksize)
        try:
            for index, chunk in enumerate(reader):
                chunk = chunk[chunk["SG_UF"].astype(str).str.upper().eq(uf)]
                if chunk.empty:
                    continue
                canonical = canonical_vote_chunk(chunk, ibge_lookup)
                grouped = (
                    canonical.groupby(
                        [
                            "candidate_section_record_id",
                            "ano_eleicao",
                            "turno",
                            "uf",
                            "cod_municipio_tse",
                            "cod_municipio_ibge",
                            "municipio_nome",
                            "zona",
                            "secao",
                            "nr_local_votacao",
                            "local_votacao",
                            "cargo_codigo",
                            "cargo",
                            "candidate_id",
                            "numero_candidato",
                            "nome_votavel",
                            "territorio_id",
                            "join_strategy",
                            "join_confidence",
                            "source_dataset",
                        ],
                        dropna=False,
                    )
                    .agg(votos=("votos", "sum"))
                    .reset_index()
                )
                part_path = temp_dir / f"part_{index:05d}.parquet"
                grouped.to_parquet(part_path, index=False)
                part_paths.append(part_path)
        finally:
            close_reader(reader)
    output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = output_dir / f"fact_electoral_results_section_{dataset_version}.parquet"
    manifest_path = output_dir / f"fact_electoral_results_section_{dataset_version}_manifest.json"
    coverage_path = output_dir / f"fact_electoral_results_section_{dataset_version}_coverage.json"
    schema: dict[str, str] = {}
    con = duckdb.connect()
    try:
        parts_glob = duckdb_safe_path(temp_dir / "*.parquet")
        out_path = duckdb_safe_path(parquet_path)
        con.execute(f"CREATE OR REPLACE TEMP VIEW vote_parts AS SELECT * FROM read_parquet('{parts_glob}')")
        if not parquet_path.exists():
            con.execute(
                f"""
                COPY (
                    SELECT
                        candidate_section_record_id,
                        ano_eleicao,
                        turno,
                        uf,
                        cod_municipio_tse,
                        cod_municipio_ibge,
                        municipio_nome,
                        zona,
                        secao,
                        nr_local_votacao,
                        local_votacao,
                        cargo_codigo,
                        cargo,
                        candidate_id,
                        numero_candidato,
                        nome_votavel,
                        territorio_id,
                        join_strategy,
                        AVG(join_confidence) AS join_confidence,
                        source_dataset,
                        SUM(votos) AS votos
                    FROM vote_parts
                    GROUP BY ALL
                ) TO '{out_path}' (FORMAT PARQUET)
                """
            )
        coverage = con.execute(
            f"""
            SELECT
                COUNT(*) AS rows,
                COUNT(DISTINCT cod_municipio_tse) AS municipios,
                COUNT(DISTINCT cod_municipio_tse || ':' || zona) AS zonas,
                COUNT(DISTINCT territorio_id) AS secoes,
                COUNT(DISTINCT candidate_id) AS candidatos_votaveis,
                SUM(votos) AS votos,
                AVG(join_confidence) AS mean_join_confidence,
                AVG(CASE WHEN cod_municipio_ibge <> '' THEN 1 ELSE 0 END) AS coverage_ibge
            FROM read_parquet('{out_path}')
            """
        ).fetchdf()
        schema_rows = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{out_path}')").fetchall()
        schema = {str(row[0]): str(row[1]) for row in schema_rows}
    finally:
        con.close()
    coverage_payload = {
        "dataset_id": "fact_electoral_results_section",
        "dataset_version": dataset_version,
        "source_zip": str(votacao_zip),
        "rows": int(coverage.loc[0, "rows"]),
        "municipios": int(coverage.loc[0, "municipios"]),
        "zonas": int(coverage.loc[0, "zonas"]),
        "secoes": int(coverage.loc[0, "secoes"]),
        "candidatos_votaveis": int(coverage.loc[0, "candidatos_votaveis"]),
        "votos": int(coverage.loc[0, "votos"]),
        "coverage_ibge": round(float(coverage.loc[0, "coverage_ibge"]), 6),
        "mean_join_confidence": round(float(coverage.loc[0, "mean_join_confidence"]), 6),
        "lgpd_classification": "public_open_data_aggregated",
        "limitations": [
            "Nao contem CPF ou dados pessoais de eleitores.",
            "Partido nao esta no arquivo votacao_secao; deve ser enriquecido por cadastro de candidaturas quando necessario.",
        ],
    }
    coverage_path.write_text(json.dumps(coverage_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    manifest = {
        "created_at_utc": utc_now_iso(),
        "dataset_id": "fact_electoral_results_section",
        "dataset_version": dataset_version,
        "parquet_path": str(parquet_path),
        "coverage_path": str(coverage_path),
        "source_zip": str(votacao_zip),
        "crosswalk_path": str(crosswalk_path),
        "schema": schema,
        "coverage": coverage_payload,
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "parquet_path": str(parquet_path),
        "manifest_path": str(manifest_path),
        "coverage": coverage_payload,
        "part_files": len(part_paths),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enrich TSE section master with IBGE and candidate-section fact.")
    parser.add_argument("--master-path", type=Path, required=True)
    parser.add_argument("--votacao-zip", type=Path, required=True)
    parser.add_argument("--ibge-municipios-json", type=Path, required=True)
    parser.add_argument("--sector-crosswalk", type=Path)
    parser.add_argument("--dataset-version", default="tse2024_sp_secao_real_ibge")
    parser.add_argument("--uf", default="SP")
    parser.add_argument("--chunksize", type=int, default=450000)
    parser.add_argument("--master-output-dir", type=Path, default=Path("lake/gold/territorial_electoral_master_index"))
    parser.add_argument("--fact-output-dir", type=Path, default=Path("lake/gold/electoral_results_section"))
    parser.add_argument("--skip-candidate-fact", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    master_result = enrich_master(
        master_path=args.master_path,
        ibge_municipios_path=args.ibge_municipios_json,
        output_dir=args.master_output_dir,
        dataset_version=args.dataset_version,
        sector_crosswalk_path=args.sector_crosswalk,
    )
    fact_result: dict[str, object] = {"status": "skipped"}
    if not args.skip_candidate_fact:
        fact_result = build_candidate_section_fact(
            votacao_zip=args.votacao_zip,
            crosswalk_path=Path(str(master_result["crosswalk_path"])),
            output_dir=args.fact_output_dir,
            dataset_version=args.dataset_version,
            chunksize=args.chunksize,
            uf=args.uf.upper(),
        )
    print(json.dumps({"master": master_result, "candidate_section_fact": fact_result}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
