from __future__ import annotations

from config.settings import Settings
from infrastructure.secrets import ChainedSecretProvider, EnvSecretProvider, VaultSecretProvider


def build_secret_provider(settings: Settings) -> ChainedSecretProvider:
    providers = []
    if settings.secret_backend.lower() == "vault":
        providers.append(
            VaultSecretProvider(
                address=settings.vault_addr,
                token=settings.vault_token,
                kv_path=settings.vault_kv_path,
            )
        )
    providers.append(EnvSecretProvider())
    return ChainedSecretProvider(providers)
