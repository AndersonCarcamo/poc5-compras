from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from common.settings import settings

engine = create_engine(settings.database_url, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()