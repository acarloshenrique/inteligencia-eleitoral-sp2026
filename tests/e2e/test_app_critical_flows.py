from contextlib import contextmanager
from types import SimpleNamespace

import pandas as pd
import pytest

import presentation.app_main as app_main


class _FakeTab:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeStreamlit:
    def __init__(self):
        self.session_state = {}
        self.errors = []
        self.successes = []
        self.stopped = False

    def set_page_config(self, **kwargs):
        return None

    def error(self, msg):
        self.errors.append(str(msg))

    def stop(self):
        self.stopped = True
        raise RuntimeError("st.stop")

    @contextmanager
    def spinner(self, _msg):
        yield

    def success(self, msg):
        self.successes.append(str(msg))

    def tabs(self, labels):
        return tuple(_FakeTab() for _ in labels)


@pytest.mark.e2e
def test_e2e_bootstrap_error_stops_app(monkeypatch):
    st = _FakeStreamlit()
    monkeypatch.setattr(app_main, "st", st)
    monkeypatch.setattr(
        app_main,
        "initialize_app_environment",
        lambda: SimpleNamespace(bootstrap={"erros": ["x"], "avisos": [], "app_env": "dev"}),
    )

    try:
        app_main.run_app()
        raise AssertionError("Esperava interrupcao por st.stop")
    except RuntimeError:
        assert st.stopped is True
        assert any("Falha de bootstrap" in e for e in st.errors)


@pytest.mark.e2e
def test_e2e_generate_allocation_happy_path(monkeypatch):
    st = _FakeStreamlit()
    monkeypatch.setattr(app_main, "st", st)

    paths = SimpleNamespace(chromadb_path=object())
    runtime = SimpleNamespace(
        bootstrap={"erros": [], "avisos": [], "app_env": "dev"},
        paths=paths,
        df_mun=pd.DataFrame([{"municipio": "Cidade A"}]),
        repository=object(),
        report_store=object(),
    )
    monkeypatch.setattr(app_main, "initialize_app_environment", lambda: SimpleNamespace(bootstrap=runtime.bootstrap))
    monkeypatch.setattr(app_main, "build_app_runtime", lambda _env: runtime)
    monkeypatch.setattr(app_main, "render_sidebar", lambda *_: (100000, "deputado_federal", 20, 0.5, True))
    monkeypatch.setattr(
        app_main,
        "executar_alocacao",
        lambda *_: pd.DataFrame([{"budget": 100000, "cluster": "Diamante", "municipio": "Cidade A"}]),
    )
    monkeypatch.setattr(app_main, "render_tab_prioridade_territorial", lambda *_: None)
    monkeypatch.setattr(app_main, "render_tab_midia_performance", lambda *_: None)
    monkeypatch.setattr(app_main, "render_tab_mensagem", lambda *_: None)
    monkeypatch.setattr(app_main, "render_tab_simulacao", lambda *_: None)
    monkeypatch.setattr(app_main, "render_tab_monitoramento", lambda *_: None)

    app_main.run_app()
    assert "aloc" in st.session_state
    assert any("alocados" in s for s in st.successes)
