#!/usr/bin/env python3
import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

from scripts.models import Base

# Cargar variables de entorno
load_dotenv()

logger = logging.getLogger(__name__)

# ─── Construcción de la URL de conexión ───────────────────────────────────────
def _build_database_url() -> str:
    """
    Construye la URL de conexión a PostgreSQL desde variables de entorno.

    Variables requeridas en .env:
        DB_HOST     → host del servidor (ej. localhost)
        DB_PORT     → puerto          (ej. 5432)
        DB_NAME     → nombre de la BD (ej. weatherstack)
        DB_USER     → usuario         (ej. postgres)
        DB_PASSWORD → contraseña
    
    Opcionalmente puedes definir DATABASE_URL directamente y se usará tal cual.
    """
    database_url = os.getenv('DATABASE_URL')
    if database_url:
        return database_url

    host = os.getenv('DB_HOST', 'localhost')
    port = os.getenv('DB_PORT', '5432')
    name = os.getenv('DB_NAME', 'weatherstack')
    user = os.getenv('DB_USER', 'postgres')
    password = os.getenv('DB_PASSWORD', '')

    return f"postgresql://{user}:{password}@{host}:{port}/{name}"


# ─── Engine y sesión ──────────────────────────────────────────────────────────
DATABASE_URL = _build_database_url()

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,       # verifica la conexión antes de usarla
    pool_size=5,              # conexiones en el pool
    max_overflow=10,          # conexiones extra permitidas
    echo=False                # True para ver el SQL generado (útil en debug)
)

SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False
)


# ─── Creación de tablas ───────────────────────────────────────────────────────
def crear_tablas():
    """Crea todas las tablas definidas en models.py si no existen."""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("✅ Tablas creadas / verificadas correctamente.")
    except Exception as e:
        logger.error(f"❌ Error al crear tablas: {e}")
        raise


def verificar_conexion():
    """Verifica que la conexión a la base de datos sea exitosa."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✅ Conexión a PostgreSQL exitosa.")
        return True
    except Exception as e:
        logger.error(f"❌ No se pudo conectar a PostgreSQL: {e}")
        return False


# ─── Inicialización automática ────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    if verificar_conexion():
        crear_tablas()
        print("✅ Base de datos lista.")
    else:
        print("❌ Revisa las variables de entorno de conexión.")