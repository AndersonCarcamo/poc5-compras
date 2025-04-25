from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    database_url: str
    rabbit_host: str
    rabbit_port: int = 5672
    rabbit_user: str = 'guest'
    rabbit_pass: str = 'guest'
    min_threshold: int = 30

    class Config:
        env_file = '.env'

settings = Settings()