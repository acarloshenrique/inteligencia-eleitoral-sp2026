# Runbook: Backup e Restore

## Backup

```bash
python scripts/backup_metadata.py
```

- Validar arquivo em `data/backups/metadata/`.
- Para artefatos em S3, garantir versionamento do bucket.

## Restore

```bash
python scripts/restore_metadata.py --from-backup <arquivo.sqlite3>
```

- Reiniciar API/worker após restore.
- Validar consulta de job e trilha de auditoria.
