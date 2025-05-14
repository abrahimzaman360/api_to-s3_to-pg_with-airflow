from pydantic import BaseModel
from os import getenv 

class Settings(BaseModel):
    # === Postgres ===
    POSTGRES_HOST: str = getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: str  = getenv("POSTGRES_PORT", "5432")
    POSTGRES_DB: str    = getenv("POSTGRES_DB", "postgres")
    POSTGRES_USER: str  = getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD: str = getenv("POSTGRES_PASSWORD", "postgres")
    S3_BUCKET: str = getenv("S3_BUCKET", "git-pg-etl")
    OUTPUT_PATH: str = getenv("OUTPUT_PATH", "data/data.csv")

settings = Settings()
