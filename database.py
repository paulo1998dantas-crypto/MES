import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

# ======================================================
# CONFIGURAÇÃO DE BANCO DE DADOS
# ======================================================
# Desenvolvimento local usa SQLite em memoria por padrão.
# Em produção, basta definir DATABASE_URL para voltar a usar
# PostgreSQL/Supabase ou outro banco compatível.

SQLALCHEMY_DATABASE_URL = os.getenv("DATABASE_URL", "sqlite://")

engine_kwargs = {}
if SQLALCHEMY_DATABASE_URL == "sqlite://":
    engine_kwargs["connect_args"] = {"check_same_thread": False}
    engine_kwargs["poolclass"] = StaticPool
elif SQLALCHEMY_DATABASE_URL.startswith("sqlite"):
    engine_kwargs["connect_args"] = {"check_same_thread": False}
else:
    engine_kwargs.update(
        {
            "pool_pre_ping": True,
            "pool_size": 5,
            "max_overflow": 10,
        }
    )

engine = create_engine(SQLALCHEMY_DATABASE_URL, **engine_kwargs)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
