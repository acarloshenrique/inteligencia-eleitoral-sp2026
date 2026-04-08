from __future__ import annotations

import json
import os
from typing import Any, Protocol

import requests
from requests import RequestException


class SecretProvider(Protocol):
    def get_secret(self, key: str) -> str | None: ...


class EnvSecretProvider:
    def get_secret(self, key: str) -> str | None:
        value = os.environ.get(key)
        if value is None or value.strip() == "":
            return None
        return value


class VaultSecretProvider:
    def __init__(self, *, address: str, token: str, kv_path: str):
        self._address = address.rstrip("/")
        self._token = token
        self._kv_path = kv_path.strip("/")

    def _fetch_all(self) -> dict[str, Any]:
        if not self._address or not self._token or not self._kv_path:
            return {}
        url = f"{self._address}/v1/{self._kv_path}"
        try:
            resp = requests.get(url, headers={"X-Vault-Token": self._token}, timeout=10)
        except RequestException:
            return {}
        if resp.status_code != 200:
            return {}
        payload = resp.json()
        data = payload.get("data", {})
        # suporta KV v1 e v2
        if "data" in data and isinstance(data["data"], dict):
            return data["data"]
        if isinstance(data, dict):
            return data
        return {}

    def get_secret(self, key: str) -> str | None:
        data = self._fetch_all()
        value = data.get(key)
        if value is None:
            return None
        return str(value)


class ChainedSecretProvider:
    def __init__(self, providers: list[SecretProvider]):
        self._providers = providers

    def get_secret(self, key: str) -> str | None:
        for provider in self._providers:
            value = provider.get_secret(key)
            if value is not None and value.strip() != "":
                return value
        return None

    def get_json(self, key: str) -> dict[str, Any]:
        value = self.get_secret(key)
        if not value:
            return {}
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            return {}
        return {}
