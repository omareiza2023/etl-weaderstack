#!/usr/bin/env python3
import os
import logging
from dotenv import load_dotenv

# Cargar variables de entorno PRIMERO
load_dotenv()

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from scripts.models import Base

logger = logging.getLogger(__name__)

def _build_database_url() -> str:
    database_url = os.getenv('DATABASE_URL')
    if database_url:
        return database_url
    host = os.getenv('DB_HOST', 'localhost')
    port = os.getenv('DB_PORT', '5432')
    name = os.getenv('DB_NAME', 'weatherstack')
    user = os.getenv('DB_USER', 'postgres')
    password = os.getenv('DB_PASSWORD', '')
    return "postgresql://postgres:oscar9904@localhost:5432/weatherstack"

DATABASE_URL = _build_database_url()

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    echo=False
)

SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False
)

def crear_tablas():
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("✅ Tablas creadas / verificadas correctamente.")
    except Exception as e:
        logger.error(f"❌ Error al crear tablas: {e}")
        raise

def verificar_conexion():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✅ Conexión a PostgreSQL exitosa.")
        return True
    except Exception as e:
        logger.error(f"❌ No se pudo conectar a PostgreSQL: {e}")
        return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    if verificar_conexion():
        crear_tablas()
        print("✅ Base de datos lista.")
    else:
        print("❌ Revisa las variables de entorno de conexión.")
