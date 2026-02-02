from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


ENV_PATH = Path(__file__).resolve().parents[1] / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(ENV_PATH),
        env_file_encoding="utf-8",
    )

    # Lê exatamente a variável DATABASE_URL do .env
    database_url: str = Field(validation_alias="DATABASE_URL")

    # BCB / Olinda endpoints
    ifdata_odata_base: str = "https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata"
    request_timeout_s: float = 30.0


settings = Settings()
