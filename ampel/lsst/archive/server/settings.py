from pydantic import AnyUrl, Field, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    root_path: str = Field("", validation_alias="ROOT_PATH")
    valkey_url: AnyUrl = Field(..., validation_alias="VALKEY_URL")
    catalog_endpoint_url: HttpUrl = Field(..., validation_alias="CATALOG_ENDPOINT_URL")
    schema_repository_url: HttpUrl = Field(
        HttpUrl("https://rubin-alert-schemas.slac.stanford.edu/schema-registry"),
        validation_alias="SCHEMA_REPOSITORY_URL",
        description="URL to the schema repository",
    )
    kafka_bootstrap_servers: str = Field(
        "localhost:9092",
        validation_alias="KAFKA_BOOTSTRAP_SERVERS",
        description="Kafka bootstrap servers",
    )
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
