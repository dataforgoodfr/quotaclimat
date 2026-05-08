from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ASRSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ASR_", extra="ignore")
    model_name: str = Field(...)
    api_url: str = Field(...)
    api_token: str = Field(...)

class VLMSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="VLM_", extra="ignore")
    model_name: str = Field(...)
    api_url: str = Field(...)
    api_token: str = Field(...)