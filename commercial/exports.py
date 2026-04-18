from __future__ import annotations

import json
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Any, Mapping
from xml.sax.saxutils import escape

import pandas as pd

from commercial.marts import utc_now_iso
from commercial.models import CommercialExportManifest
from commercial.strategy import competitive_dataset_ranking


def _write_json(path: Path, payload: dict[str, Any] | list[dict[str, Any]]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _sheet_name(value: str) -> str:
    return "".join(character if character.isalnum() else "_" for character in value)[:31] or "sheet"


def _column_name(index: int) -> str:
    name = ""
    current = index
    while current:
        current, remainder = divmod(current - 1, 26)
        name = chr(65 + remainder) + name
    return name


def _minimal_sheet_xml(rows: list[list[object]]) -> str:
    xml_rows = []
    for row_index, row in enumerate(rows, start=1):
        cells = []
        for column_index, value in enumerate(row, start=1):
            ref = f"{_column_name(column_index)}{row_index}"
            text = escape(str(value))
            cells.append(f'<c r="{ref}" t="inlineStr"><is><t>{text}</t></is></c>')
        xml_rows.append(f'<row r="{row_index}">{"".join(cells)}</row>')
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f"<sheetData>{''.join(xml_rows)}</sheetData>"
        "</worksheet>"
    )


def _minimal_workbook_bytes(tables: Mapping[str, pd.DataFrame]) -> bytes:
    rows: list[list[object]] = []
    for name, frame in tables.items():
        rows.append([name])
        if frame.empty:
            rows.append(["status", "Not found in repo"])
            continue
        rows.append(list(frame.columns))
        rows.extend(frame.fillna("").astype(str).values.tolist())
        rows.append([])
    if not rows:
        rows = [["status"], ["Not found in repo"]]

    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as workbook:
        workbook.writestr(
            "[Content_Types].xml",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
            '<Default Extension="xml" ContentType="application/xml"/>'
            '<Override PartName="/xl/workbook.xml" '
            'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
            '<Override PartName="/xl/worksheets/sheet1.xml" '
            'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
            "</Types>",
        )
        workbook.writestr(
            "_rels/.rels",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" '
            'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
            'Target="xl/workbook.xml"/>'
            "</Relationships>",
        )
        workbook.writestr(
            "xl/workbook.xml",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
            'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
            '<sheets><sheet name="report" sheetId="1" r:id="rId1"/></sheets>'
            "</workbook>",
        )
        workbook.writestr(
            "xl/_rels/workbook.xml.rels",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" '
            'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
            'Target="worksheets/sheet1.xml"/>'
            "</Relationships>",
        )
        workbook.writestr("xl/worksheets/sheet1.xml", _minimal_sheet_xml(rows))
    return buffer.getvalue()


def _workbook_bytes(tables: Mapping[str, pd.DataFrame]) -> bytes:
    buffer = BytesIO()
    try:
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            wrote = False
            for name, frame in tables.items():
                if frame.empty:
                    continue
                frame.to_excel(writer, sheet_name=_sheet_name(name), index=False)
                wrote = True
            if not wrote:
                pd.DataFrame([{"status": "Not found in repo"}]).to_excel(writer, sheet_name="status", index=False)
        return buffer.getvalue()
    except ModuleNotFoundError:
        return _minimal_workbook_bytes(tables)


def _markdown_table(df: pd.DataFrame, *, max_rows: int = 20) -> str:
    if df.empty:
        return "_Sem linhas disponíveis._\n"
    view = df.head(max_rows).fillna("")
    header = "| " + " | ".join(str(column) for column in view.columns) + " |"
    separator = "| " + " | ".join("---" for _ in view.columns) + " |"
    rows = ["| " + " | ".join(str(value) for value in row) + " |" for row in view.to_numpy()]
    return "\n".join([header, separator, *rows]) + "\n"


def _pitch_markdown(metrics: pd.DataFrame) -> str:
    if metrics.empty:
        return "## Métricas comerciais\n\n_Not found in repo: commercial_pitch_metrics._\n"
    row = metrics.iloc[0].to_dict()
    return (
        "## Métricas comerciais\n\n"
        f"- Candidatos suportados: {row.get('candidates_supported', 0)}\n"
        f"- Territórios ranqueados: {row.get('territories_ranked', 0)}\n"
        f"- Registros no índice mestre: {row.get('master_records', 0)}\n"
        f"- Recomendações geradas: {row.get('recommendations_generated', 0)}\n"
        f"- Score médio de prioridade: {float(row.get('avg_priority_score', 0.0)):.3f}\n"
        f"- Confiança média de join: {float(row.get('avg_join_confidence', 0.0)):.3f}\n"
        f"- Score de saúde do lake: {float(row.get('lake_quality_score', 0.0)):.3f}\n"
    )


class CommercialExportService:
    """Exports commercial marts into artifacts that sales, product and operators can consume."""

    def export(
        self,
        *,
        marts: dict[str, pd.DataFrame],
        output_dir: Path,
        tenant_id: str,
        campaign_id: str,
        snapshot_id: str,
    ) -> CommercialExportManifest:
        output_dir.mkdir(parents=True, exist_ok=True)
        exported: dict[str, str] = {}
        row_counts: dict[str, int] = {}

        demo = marts.get("commercial_demo_summary", pd.DataFrame())
        premium = marts.get("premium_report_tables", pd.DataFrame())
        pitch = marts.get("commercial_pitch_metrics", pd.DataFrame())

        demo_json = output_dir / "commercial_demo_summary.json"
        _write_json(demo_json, demo.to_dict(orient="records"))
        exported["commercial_demo_summary.json"] = str(demo_json)
        row_counts["commercial_demo_summary"] = int(len(demo))

        demo_md = output_dir / "commercial_demo_summary.md"
        demo_md.write_text(
            "# Demo comercial\n\n"
            "Ranking inicial para demonstrar valor territorial, granularidade e explicabilidade.\n\n"
            + _markdown_table(demo)
            + "\n"
            + _pitch_markdown(pitch),
            encoding="utf-8",
        )
        exported["commercial_demo_summary.md"] = str(demo_md)

        pitch_json = output_dir / "commercial_pitch_metrics.json"
        _write_json(pitch_json, pitch.to_dict(orient="records"))
        exported["commercial_pitch_metrics.json"] = str(pitch_json)
        row_counts["commercial_pitch_metrics"] = int(len(pitch))

        ranking_csv = output_dir / "ranking_operacional.csv"
        demo.to_csv(ranking_csv, index=False, encoding="utf-8")
        exported["ranking_operacional.csv"] = str(ranking_csv)

        allocation_csv = output_dir / "allocation_recommendations.csv"
        premium.to_csv(allocation_csv, index=False, encoding="utf-8")
        exported["allocation_recommendations.csv"] = str(allocation_csv)
        row_counts["premium_report_tables"] = int(len(premium))

        workbook = output_dir / "premium_report_tables.xlsx"
        workbook.write_bytes(
            _workbook_bytes(
                {
                    "ranking": demo,
                    "recommendations": premium,
                    "pitch_metrics": pitch,
                }
            )
        )
        exported["premium_report_tables.xlsx"] = str(workbook)

        assets_md = output_dir / "commercial_assets.md"
        assets_md.write_text(self._assets_markdown(), encoding="utf-8")
        exported["commercial_assets.md"] = str(assets_md)

        manifest = CommercialExportManifest(
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            snapshot_id=snapshot_id,
            exported_files=exported,
            generated_at_utc=utc_now_iso(),
            row_counts=row_counts,
            notes=[
                "Artifacts include tenant_id, campaign_id and snapshot_id for auditability.",
                "No individual voter-level targeting is exported by this layer.",
            ],
        )
        manifest_path = output_dir / "commercial_export_manifest.json"
        manifest_path.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
        manifest.exported_files["commercial_export_manifest.json"] = str(manifest_path)
        return manifest

    def _assets_markdown(self) -> str:
        lines = ["# Ativos comerciais do Data Lake", ""]
        for asset in competitive_dataset_ranking():
            lines.extend(
                [
                    f"## {asset.name}",
                    "",
                    f"- Impacto: {asset.impact}",
                    f"- Datasets fonte: {', '.join(asset.source_tables)}",
                    f"- Saídas: {', '.join(asset.supported_outputs)}",
                    f"- Usos comerciais: {', '.join(asset.commercial_use_cases)}",
                    f"- Pronto para demo: {'sim' if asset.demo_readiness else 'não'}",
                    f"- Pronto para relatório premium: {'sim' if asset.premium_report_ready else 'não'}",
                    "",
                    asset.description,
                    "",
                ]
            )
            if asset.limitations:
                lines.append("Limitações: " + "; ".join(asset.limitations))
                lines.append("")
        return "\n".join(lines)
