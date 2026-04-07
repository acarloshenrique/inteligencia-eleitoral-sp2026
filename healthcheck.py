#!/usr/bin/env python3
import argparse
import os
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def resolve_data_root() -> Path:
    env_root = os.environ.get("DATA_ROOT")
    candidates = []
    if env_root:
        candidates.append(Path(env_root))
    candidates.extend([Path("./data"), Path("/app/data"), Path("/content/drive/MyDrive/inteligencia_eleitoral")])
    for p in candidates:
        if p.exists():
            return p.resolve()
    return candidates[0].resolve()


def find_df_mun(base_dir: Path, ts: str) -> Path | None:
    estado = base_dir / "outputs" / "estado_sessao"
    if not estado.exists():
        return None
    fixed = estado / f"df_mun_{ts}.parquet"
    if fixed.exists():
        return fixed
    files = sorted(estado.glob("df_mun_*.parquet"), reverse=True)
    return files[0] if files else None


def check_streamlit() -> tuple[bool, str]:
    port = os.environ.get("PORT", "7860")
    url = f"http://127.0.0.1:{port}/_stcore/health"
    try:
        with urllib.request.urlopen(url, timeout=3) as resp:
            body = resp.read().decode("utf-8", errors="ignore").lower()
            if resp.status == 200 and ("ok" in body or "healthy" in body):
                return True, "streamlit health ok"
            return False, f"unexpected health response: status={resp.status} body={body[:80]}"
    except urllib.error.URLError as e:
        return False, f"health endpoint unreachable: {e}"
    except Exception as e:
        return False, f"health check failed: {e}"


def check_readiness() -> tuple[bool, list[str]]:
    messages = []
    base = resolve_data_root()
    ts = os.environ.get("DF_MUN_TS", "20260316_1855")

    if env_bool("REQUIRE_DATA", default=False):
        if find_df_mun(base, ts) is None:
            messages.append("required dataset missing (df_mun_*.parquet)")

    if env_bool("REQUIRE_GROQ_API_KEY", default=False) and not os.environ.get("GROQ_API_KEY"):
        messages.append("required GROQ_API_KEY is missing")

    runtime_rel = Path(tempfile.gettempdir()) / "inteligencia_eleitoral" / "relatorios"
    try:
        runtime_rel.mkdir(parents=True, exist_ok=True)
        probe = runtime_rel / ".ready_probe"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
    except Exception as e:
        messages.append(f"runtime output path is not writable: {e}")

    return len(messages) == 0, messages


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["live", "ready"], default="live")
    args = parser.parse_args()

    is_live, live_msg = check_streamlit()
    if not is_live:
        print(f"UNHEALTHY: {live_msg}")
        return 1

    if args.mode == "live":
        print("LIVE: ok")
        return 0

    is_ready, ready_msgs = check_readiness()
    if not is_ready:
        print("NOT_READY: " + " | ".join(ready_msgs))
        return 1

    print("READY: ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
