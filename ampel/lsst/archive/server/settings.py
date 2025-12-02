import secrets

from pydantic import Field, HttpUrl, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    root_path: str = Field("", validation_alias="ROOT_PATH")
    archive_uri: PostgresDsn = Field(
        PostgresDsn("postgresql://localhost:5432/ztfarchive"),
        validation_alias="ARCHIVE_URI",
    )
    default_statement_timeout: int = Field(
        60,
        validation_alias="DEFAULT_STATEMENT_TIMEOUT",
        description="Timeout for synchronous queries, in seconds",
    )
    stream_query_timeout: int = Field(
        8 * 60 * 60,
        validation_alias="STREAM_QUERY_TIMEOUT",
        description="Timeout for asynchronous queries, in seconds",
    )
    stream_chunk_bytes: int = Field(
        5 * 1024 * 1024,
        validation_alias="STREAM_CHUNK_BYTES",
        description="Size of alert chunks when streaming from S3",
    )
    catalog_endpoint_url: HttpUrl = Field(..., validation_alias="CATALOG_ENDPOINT_URL")
    s3_endpoint: str | None = Field(None, validation_alias="S3_ENDPOINT")
    s3_bucket: str = Field("ampel-lsst-cutout-archive", validation_alias="S3_BUCKET")
    s3_insecure: bool = Field(False, validation_alias="S3_INSECURE")
    jwt_secret_key: str = Field(
        secrets.token_urlsafe(64), validation_alias="JWT_SECRET_KEY"
    )
    jwt_algorithm: str = Field("HS256", validation_alias="JWT_ALGORITHM")
    allowed_identities: set[str] = Field(
        {"AmpelProject", "ZwickyTransientFacility"},
        validation_alias="ALLOWED_IDENTITIES",
        description="Usernames, teams, and orgs allowed to create persistent tokens",
    )
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
    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()  # type: ignore[call-arg]
