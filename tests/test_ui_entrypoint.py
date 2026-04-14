from pathlib import Path


def test_streamlit_entrypoint_is_canonical():
    entrypoint = Path("web_ui/streamlit_app.py").read_text(encoding="utf-8")
    assert "from presentation.app_main import run_app" in entrypoint
    assert "run_app()" in entrypoint


def test_legacy_app_py_is_deprecated_stub():
    legacy = Path("app.py").read_text(encoding="utf-8")
    assert "Deprecated Streamlit entrypoint" in legacy
    assert "presentation.app_main import run_app" not in legacy
    assert "web_ui/streamlit_app.py" in legacy


def test_runtime_configs_use_canonical_streamlit_entrypoint():
    dockerfile = Path("Dockerfile").read_text(encoding="utf-8")
    ci = Path(".github/workflows/test.yml").read_text(encoding="utf-8")
    deploy = Path(".github/workflows/deploy.yml").read_text(encoding="utf-8")

    assert '"streamlit", "run", "web_ui/streamlit_app.py"' in dockerfile
    assert "python -m py_compile web_ui/streamlit_app.py presentation/app_main.py healthcheck.py" in ci
    assert "cp ../app.py" not in deploy
