from __future__ import annotations

import zipfile
from datetime import datetime
from io import BytesIO
from typing import Mapping
from xml.sax.saxutils import escape

import pandas as pd

_COMPONENT_LABELS = {
    "score_potencial_eleitoral": "potencial eleitoral",
    "score_oportunidade": "oportunidade",
    "score_eficiencia_midia": "eficiencia de midia",
    "score_custo": "baixo custo relativo",
    "score_risco": "risco controlado",
}
_COMPONENT_COLUMNS = tuple(_COMPONENT_LABELS.keys())


def _as_percent(value: object) -> str:
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(number):
        return "n/d"
    return f"{float(number) * 100:.0f}%"


def _municipio_label(row: pd.Series) -> str:
    for column in ("municipio", "NM_MUNICIPIO", "nome_municipio", "municipio_id_ibge7"):
        if column in row and pd.notna(row[column]) and str(row[column]).strip():
            return str(row[column]).strip()
    return "territorio sem nome"


def _confidence(row: pd.Series) -> float:
    candidates = ["data_quality_score", "join_confidence", "coverage", "quality_score"]
    values = []
    for column in candidates:
        if column in row and pd.notna(row[column]):
            values.append(float(pd.to_numeric(pd.Series([row[column]]), errors="coerce").fillna(0.0).iloc[0]))
    if not values:
        return 0.0
    return max(0.0, min(1.0, sum(values) / len(values)))


def _top_drivers(row: pd.Series) -> list[str]:
    pairs: list[tuple[str, float]] = []
    for column, label in _COMPONENT_LABELS.items():
        raw = row.get(column, 0.0)
        value = float(pd.to_numeric(pd.Series([raw]), errors="coerce").fillna(0.0).iloc[0])
        if column == "score_risco":
            value = 1.0 - value
        pairs.append((label, value))
    pairs.sort(key=lambda item: item[1], reverse=True)
    return [f"{label} ({value:.2f})" for label, value in pairs[:3]]


def build_explainability_frame(scores: pd.DataFrame, recommendations: pd.DataFrame | None = None) -> pd.DataFrame:
    """Create client-facing reasons for each ranked territory."""
    if scores.empty:
        return pd.DataFrame(
            columns=[
                "ranking",
                "municipio_id_ibge7",
                "municipio",
                "score_alocacao",
                "confiabilidade",
                "principais_variaveis",
                "por_que_municipio_esta_alto",
            ]
        )

    base = scores.copy()
    if recommendations is not None and not recommendations.empty and "municipio_id_ibge7" in recommendations.columns:
        keep = [
            col
            for col in ["municipio_id_ibge7", "canal_ideal", "mensagem_ideal", "verba_sugerida", "justificativa"]
            if col in recommendations.columns
        ]
        base = base.merge(
            recommendations[keep].drop_duplicates("municipio_id_ibge7"), on="municipio_id_ibge7", how="left"
        )

    rows: list[dict[str, object]] = []
    for _, row in base.iterrows():
        drivers = _top_drivers(row)
        confidence = _confidence(row)
        score = float(pd.to_numeric(pd.Series([row.get("score_alocacao", 0.0)]), errors="coerce").fillna(0.0).iloc[0])
        canal = str(row.get("canal_ideal", "canal com melhor desempenho disponivel"))
        mensagem = str(row.get("mensagem_ideal", "mensagem com melhor desempenho territorial"))
        why = (
            f"{_municipio_label(row)} combina {', '.join(drivers)}; "
            f"usar {canal} com '{mensagem}'. Confiabilidade {_as_percent(confidence)}."
        )
        rows.append(
            {
                "ranking": int(row.get("ranking", len(rows) + 1)),
                "municipio_id_ibge7": str(row.get("municipio_id_ibge7", "")),
                "municipio": _municipio_label(row),
                "score_alocacao": round(score, 2),
                "confiabilidade": round(confidence, 4),
                "principais_variaveis": "; ".join(drivers),
                "por_que_municipio_esta_alto": why,
                "canal_ideal": canal,
                "mensagem_ideal": mensagem,
            }
        )
    return pd.DataFrame(rows).sort_values(["ranking", "score_alocacao"], ascending=[True, False]).reset_index(drop=True)


def build_ranking_snapshot(scores: pd.DataFrame, recommendations: pd.DataFrame | None = None) -> pd.DataFrame:
    explain = build_explainability_frame(scores, recommendations)
    if explain.empty:
        return explain
    columns = [
        "ranking",
        "municipio_id_ibge7",
        "municipio",
        "score_alocacao",
        "confiabilidade",
        "canal_ideal",
        "mensagem_ideal",
        "principais_variaveis",
    ]
    return explain[[col for col in columns if col in explain.columns]].copy()


def _sheet_name(raw_name: str) -> str:
    return "".join(ch for ch in str(raw_name) if ch.isalnum() or ch in " _-")[:31] or "dados"


def _column_letter(index: int) -> str:
    letters = ""
    while index:
        index, rem = divmod(index - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


def _worksheet_xml(df: pd.DataFrame) -> str:
    rows = []
    values = [list(df.columns)] + df.fillna("").astype(str).values.tolist()
    for row_idx, row in enumerate(values, start=1):
        cells = []
        for col_idx, value in enumerate(row, start=1):
            ref = f"{_column_letter(col_idx)}{row_idx}"
            cells.append(f'<c r="{ref}" t="inlineStr"><is><t>{escape(str(value))}</t></is></c>')
        rows.append(f'<row r="{row_idx}">{"".join(cells)}</row>')
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>'
        + "".join(rows)
        + "</sheetData></worksheet>"
    )


def _minimal_xlsx_bytes(tables: Mapping[str, pd.DataFrame]) -> bytes:
    usable = [(name, df) for name, df in tables.items() if df is not None and not df.empty]
    if not usable:
        usable = [("status", pd.DataFrame([{"status": "Not found in repo"}]))]
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(
            "[Content_Types].xml",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/></Types>',
        )
        zf.writestr(
            "_rels/.rels",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>',
        )
        sheet_entries = []
        rel_entries = []
        for idx, (raw_name, df) in enumerate(usable, start=1):
            sheet_entries.append(f'<sheet name="{escape(_sheet_name(raw_name))}" sheetId="{idx}" r:id="rId{idx}"/>')
            rel_entries.append(
                f'<Relationship Id="rId{idx}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{idx}.xml"/>'
            )
            zf.writestr(f"xl/worksheets/sheet{idx}.xml", _worksheet_xml(df))
        zf.writestr(
            "xl/workbook.xml",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets>'
            + "".join(sheet_entries)
            + "</sheets></workbook>",
        )
        zf.writestr(
            "xl/_rels/workbook.xml.rels",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            + "".join(rel_entries)
            + "</Relationships>",
        )
    return buf.getvalue()


def build_operational_workbook_bytes(tables: Mapping[str, pd.DataFrame]) -> bytes:
    try:
        buf = BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            wrote = False
            for raw_name, df in tables.items():
                if df is None or df.empty:
                    continue
                df.to_excel(writer, sheet_name=_sheet_name(raw_name), index=False)
                wrote = True
            if not wrote:
                pd.DataFrame([{"status": "Not found in repo"}]).to_excel(writer, sheet_name="status", index=False)
        return buf.getvalue()
    except ModuleNotFoundError:
        return _minimal_xlsx_bytes(tables)


def _escape_pdf_text(value: object) -> str:
    text = str(value).replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
    return text.encode("latin-1", errors="replace").decode("latin-1")


def _pdf_text_stream(lines: list[str]) -> str:
    y = 800
    commands = ["BT", "/F1 16 Tf", f"72 {y} Td", f"({_escape_pdf_text(lines[0])}) Tj"]
    y_step = 16
    for line in lines[1:]:
        commands.append(f"0 -{y_step} Td")
        commands.append(f"({_escape_pdf_text(line)}) Tj")
    commands.append("ET")
    return "\n".join(commands)


def _simple_pdf(lines: list[str]) -> bytes:
    content = _pdf_text_stream(lines).encode("latin-1", errors="replace")
    objects = [
        b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n",
        b"3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>\nendobj\n",
        b"4 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\nendobj\n",
        b"5 0 obj\n<< /Length "
        + str(len(content)).encode("ascii")
        + b" >>\nstream\n"
        + content
        + b"\nendstream\nendobj\n",
    ]
    out = BytesIO()
    out.write(b"%PDF-1.4\n")
    offsets = [0]
    for obj in objects:
        offsets.append(out.tell())
        out.write(obj)
    xref = out.tell()
    out.write(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
    out.write(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        out.write(f"{offset:010d} 00000 n \n".encode("ascii"))
    out.write(f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF".encode("ascii"))
    return out.getvalue()


def build_executive_pdf_bytes(
    *,
    scores: pd.DataFrame,
    recommendations: pd.DataFrame | None = None,
    simulations: pd.DataFrame | None = None,
    generated_at: datetime | None = None,
) -> bytes:
    generated_at = generated_at or datetime.now()
    ranking = build_ranking_snapshot(scores, recommendations).head(8)
    total_budget = 0.0
    if recommendations is not None and not recommendations.empty and "verba_sugerida" in recommendations.columns:
        total_budget = float(pd.to_numeric(recommendations["verba_sugerida"], errors="coerce").fillna(0.0).sum())
    elif simulations is not None and not simulations.empty and "verba_simulada" in simulations.columns:
        total_budget = float(pd.to_numeric(simulations["verba_simulada"], errors="coerce").fillna(0.0).sum())

    lines = [
        "Inteligencia Eleitoral SP 2026 - Relatorio Executivo",
        f"Gerado em {generated_at.strftime('%d/%m/%Y %H:%M')}",
        f"Territorios ranqueados: {len(scores)}",
        f"Verba sugerida/simulada: R$ {total_budget:,.2f}",
        "",
        "Top prioridades territoriais",
    ]
    if ranking.empty:
        lines.append("Not found in repo: mart_score_alocacao_modular / mart_recomendacao_alocacao")
    else:
        for _, row in ranking.iterrows():
            lines.append(
                f"#{row['ranking']} {row['municipio']} | score {row['score_alocacao']:.1f} | "
                f"conf {_as_percent(row['confiabilidade'])} | {row.get('canal_ideal', 'n/d')}"
            )
    lines = lines[:45]
    return _simple_pdf(lines)


def build_product_exports(
    *,
    scores: pd.DataFrame,
    recommendations: pd.DataFrame | None = None,
    simulations: pd.DataFrame | None = None,
    media: pd.DataFrame | None = None,
    messages: pd.DataFrame | None = None,
) -> dict[str, object]:
    explainability = build_explainability_frame(scores, recommendations)
    ranking = build_ranking_snapshot(scores, recommendations)
    workbook = build_operational_workbook_bytes(
        {
            "ranking_atualizado": ranking,
            "recomendacoes": recommendations if recommendations is not None else pd.DataFrame(),
            "simulacao": simulations if simulations is not None else pd.DataFrame(),
            "midia_performance": media if media is not None else pd.DataFrame(),
            "mensagens": messages if messages is not None else pd.DataFrame(),
            "explicabilidade": explainability,
        }
    )
    return {
        "pdf_bytes": build_executive_pdf_bytes(scores=scores, recommendations=recommendations, simulations=simulations),
        "xlsx_bytes": workbook,
        "ranking_csv_bytes": ranking.to_csv(index=False).encode("utf-8"),
        "ranking": ranking,
        "explainability": explainability,
    }
