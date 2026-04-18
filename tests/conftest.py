from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture
def mock_llm_client(mocker: Any):
    response = mocker.Mock()
    response.choices = [mocker.Mock(message=mocker.Mock(content="Resposta fake"))]
    response.usage = mocker.Mock(total_tokens=123)

    completions = mocker.Mock()
    completions.create.return_value = response

    chat = mocker.Mock()
    chat.completions = completions

    client = mocker.Mock()
    client.chat = chat
    return client
