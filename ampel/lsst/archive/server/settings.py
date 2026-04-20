from pydantic import AnyUrl, Field, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    root_path: str = Field("", validation_alias="ROOT_PATH")
    valkey_url: AnyUrl = Field(..., validation_alias="VALKEY_URL")
    catalog_endpoint_url: HttpUrl = Field(..., validation_alias="CATALOG_ENDPOINT_URL")
    s3_endpoint: str | None = Field(None, validation_alias="S3_ENDPOINT")
    s3_insecure: bool = Field(False, validation_alias="S3_INSECURE")
    allowed_origins: list[str] = Field(
        [],
        validation_alias="ALLOWED_ORIGINS",
        description="Allowed CORS origins",
    )
    cache_max_age: int = Field(
        300,
        validation_alias="CACHE_MAX_AGE",
        description="Max age for cacheable responses, in seconds",
    )
    enable_profiling: bool = Field(
        False,
        validation_alias="ENABLE_PROFILING",
        description="Enable query profiling",
    )
    model_config = SettingsConfigDict(env_file=".env", validate_assignment=True)


settings = Settings()
