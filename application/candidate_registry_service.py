from __future__ import annotations

import json
from dataclasses import asdict

from api.decision_contracts import CandidateProfileSchema
from application.decision_mappers import candidate_profile_to_domain
from config.settings import AppPaths
from domain.decision_models import CandidateProfile


class CandidateRegistryService:
    def __init__(self, paths: AppPaths):
        self.paths = paths
        root = paths.metadata_db_path.parent if paths.metadata_db_path else paths.data_root / "metadata"
        root.mkdir(parents=True, exist_ok=True)
        self.registry_path = root / "candidate_registry.json"

    def upsert(self, candidate: CandidateProfileSchema) -> CandidateProfile:
        domain = candidate_profile_to_domain(candidate)
        payload = self._read()
        payload[domain.candidate_id] = asdict(domain)
        self._write(payload)
        return domain

    def get(self, candidate_id: str) -> CandidateProfile | None:
        payload = self._read().get(candidate_id)
        if payload is None:
            return None
        return CandidateProfile(
            candidate_id=str(payload["candidate_id"]),
            nome_politico=str(payload["nome_politico"]),
            cargo=str(payload["cargo"]),
            partido=str(payload["partido"]),
            idade=payload.get("idade"),
            faixa_etaria=str(payload.get("faixa_etaria", "nao_informada")),
            origem_territorial=str(payload.get("origem_territorial", "")),
            incumbente=bool(payload.get("incumbente", False)),
            biografia_resumida=str(payload.get("biografia_resumida", "")),
            temas_prioritarios=tuple(payload.get("temas_prioritarios", [])),
            temas_secundarios=tuple(payload.get("temas_secundarios", [])),
            historico_eleitoral=tuple(payload.get("historico_eleitoral", [])),
            municipios_base=tuple(payload.get("municipios_base", [])),
            zonas_base=tuple(payload.get("zonas_base", [])),
            observacoes_estrategicas=str(payload.get("observacoes_estrategicas", "")),
        )

    def list(self) -> list[CandidateProfile]:
        candidates: list[CandidateProfile] = []
        for candidate_id in sorted(self._read()):
            candidate = self.get(candidate_id)
            if candidate is not None:
                candidates.append(candidate)
        return candidates

    def _read(self) -> dict[str, dict]:
        if not self.registry_path.exists():
            return {}
        return json.loads(self.registry_path.read_text(encoding="utf-8"))

    def _write(self, payload: dict[str, dict]) -> None:
        self.registry_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
