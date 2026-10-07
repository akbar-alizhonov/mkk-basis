from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    api_key: str = "dev-api-key"

    outbox_poll_interval: float = 1.0
    outbox_batch_size: int = 100

    consumer_prefetch: int = 10
    max_attempts: int = 3
    retry_base_delay: float = 2.0
    webhook_timeout: float = 5.0

    gateway_min_delay: float = 2.0
    gateway_max_delay: float = 5.0
    gateway_success_rate: float = 0.9


@lru_cache
def get_settings() -> Settings:
    return Settings()
