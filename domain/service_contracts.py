from pydantic import BaseModel, Field


class SearchRelevantResponse(BaseModel):
    municipios: list[str] = Field(default_factory=list)
    fallback_vector: bool = False


class CompleteResponse(BaseModel):
    text: str = Field(min_length=1)
    total_tokens: int = Field(ge=0)
    fallback_llm: bool = False
