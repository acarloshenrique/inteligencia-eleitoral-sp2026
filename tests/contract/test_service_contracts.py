import pytest
from pydantic import ValidationError

from domain.service_contracts import CompleteResponse, SearchRelevantResponse


@pytest.mark.contract
def test_contract_search_relevant_response_valid():
    payload = SearchRelevantResponse(municipios=["Cidade A"], fallback_vector=False)
    assert payload.municipios == ["Cidade A"]


@pytest.mark.contract
def test_contract_complete_response_rejects_negative_tokens():
    with pytest.raises(ValidationError):
        CompleteResponse(text="ok", total_tokens=-1, fallback_llm=False)
