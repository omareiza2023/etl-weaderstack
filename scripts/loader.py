#!/usr/bin/env python3
"""
loader.py — Carga los datos extraídos del clima en la base de datos PostgreSQL.
Lee desde data/clima_raw.json e inserta en las tablas ciudades, registros_clima y metricas_etl.
"""
import json
import logging
import time
from datetime import datetime
from pathlib import Path
import sys
sys.path.insert(0, '.')

from scripts.database import SessionLocal, crear_tablas, verificar_conexion
from scripts.models import Ciudad, RegistroClima, MetricasETL

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class WeatherstackLoader:
    def __init__(self):
        self.db = SessionLocal()
        self.registros_extraidos = 0
        self.registros_guardados = 0
        self.registros_fallidos = 0
        self.inicio = time.time()

    # ─── Ciudad ───────────────────────────────────────────────────────────────
    def obtener_o_crear_ciudad(self, nombre: str, pais: str) -> Ciudad:
        """Busca la ciudad en la BD; si no existe, la crea."""
        ciudad = self.db.query(Ciudad).filter_by(nombre=nombre).first()
        if not ciudad:
            ciudad = Ciudad(nombre=nombre, pais=pais)
            self.db.add(ciudad)
            self.db.flush()  # obtiene el id sin hacer commit aún
            logger.info(f"🏙️  Ciudad nueva registrada: {nombre} ({pais})")
        return ciudad

    # ─── Registro de clima ────────────────────────────────────────────────────
    def cargar_registro(self, dato: dict) -> bool:
        """Inserta un registro de clima en la BD."""
        try:
            ciudad = self.obtener_o_crear_ciudad(
                nombre=dato.get('ciudad', 'Desconocida'),
                pais=dato.get('pais', 'Desconocido')
            )

            registro = RegistroClima(
                ciudad_id=ciudad.id,
                temperatura=dato.get('temperatura'),
                sensacion_termica=dato.get('sensacion_termica'),
                humedad=dato.get('humedad'),
                velocidad_viento=dato.get('velocidad_viento'),
                descripcion=dato.get('descripcion'),
                fecha_extraccion=datetime.fromisoformat(dato['fecha_extraccion'])
                    if dato.get('fecha_extraccion')
                    else datetime.utcnow()
            )

            self.db.add(registro)
            self.registros_guardados += 1
            logger.info(f"✅ Registro cargado: {ciudad.nombre} — {registro.temperatura}°C")
            return True

        except Exception as e:
            self.registros_fallidos += 1
            logger.error(f"❌ Error cargando registro {dato.get('ciudad')}: {e}")
            return False

    # ─── Métricas ETL ─────────────────────────────────────────────────────────
    def registrar_metrica(self, estado: str):
        """Guarda las métricas de la ejecución en metricas_etl."""
        tiempo_total = round(time.time() - self.inicio, 2)
        metrica = MetricasETL(
            fecha_ejecucion=datetime.utcnow(),
            estado=estado,
            registros_extraidos=self.registros_extraidos,
            registros_guardados=self.registros_guardados,
            registros_fallidos=self.registros_fallidos,
            tiempo_ejecucion_segundos=tiempo_total
        )
        self.db.add(metrica)
        self.db.commit()
        logger.info(
            f"📊 Métricas guardadas — estado: {estado} | "
            f"extraídos: {self.registros_extraidos} | "
            f"guardados: {self.registros_guardados} | "
            f"fallidos: {self.registros_fallidos} | "
            f"tiempo: {tiempo_total}s"
        )

    # ─── Ejecución principal ──────────────────────────────────────────────────
    def ejecutar(self, ruta_json: str = 'data/clima_raw.json'):
        """Lee el JSON extraído y carga todos los registros en la BD."""
        ruta = Path(ruta_json)

        if not ruta.exists():
            logger.error(f"❌ Archivo no encontrado: {ruta_json}")
            logger.error("   Ejecuta primero: python3 scripts/extractor.py")
            return

        try:
            with open(ruta, 'r') as f:
                datos = json.load(f)

            self.registros_extraidos = len(datos)
            logger.info(f"📂 Cargando {self.registros_extraidos} registros desde {ruta_json}...")

            for dato in datos:
                self.cargar_registro(dato)

            # Commit general
            self.db.commit()

            estado = 'exitoso' if self.registros_fallidos == 0 else 'parcial'
            self.registrar_metrica(estado)

            print("\n" + "=" * 55)
            print("  RESUMEN DE CARGA")
            print("=" * 55)
            print(f"  📥 Registros extraídos : {self.registros_extraidos}")
            print(f"  ✅ Registros guardados : {self.registros_guardados}")
            print(f"  ❌ Registros fallidos  : {self.registros_fallidos}")
            print(f"  ⏱️  Tiempo total        : {round(time.time() - self.inicio, 2)}s")
            print(f"  🏁 Estado              : {estado.upper()}")
            print("=" * 55 + "\n")

        except Exception as e:
            self.db.rollback()
            self.registrar_metrica('fallido')
            logger.error(f"❌ Error en la carga: {e}")
            raise

        finally:
            self.db.close()


# ─── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if not verificar_conexion():
        logger.error("No se puede conectar a la BD. Revisa las variables de entorno.")
        sys.exit(1)

    crear_tablas()

    loader = WeatherstackLoader()
    loader.ejecutar()