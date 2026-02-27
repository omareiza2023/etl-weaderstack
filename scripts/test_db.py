#!/usr/bin/env python3
"""
test_db.py — Pruebas de conexión y operaciones sobre la base de datos PostgreSQL.
Verifica: conexión, creación de tablas, CRUD sobre Ciudad, RegistroClima y MetricasETL.
"""
import sys
import logging
from datetime import datetime

sys.path.insert(0, '.')

from scripts.database import SessionLocal, verificar_conexion, crear_tablas
from scripts.models import Ciudad, RegistroClima, MetricasETL

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ─── Helpers ──────────────────────────────────────────────────────────────────
def separador(titulo: str):
    print("\n" + "=" * 55)
    print(f"  {titulo}")
    print("=" * 55)


def ok(msg: str):
    print(f"  ✅ {msg}")


def fallo(msg: str):
    print(f"  ❌ {msg}")


# ─── Tests ────────────────────────────────────────────────────────────────────

def test_conexion():
    separador("TEST 1 — Conexión a PostgreSQL")
    resultado = verificar_conexion()
    if resultado:
        ok("Conexión exitosa")
    else:
        fallo("No se pudo conectar — revisa las variables de entorno")
        sys.exit(1)


def test_crear_tablas():
    separador("TEST 2 — Creación de tablas")
    try:
        crear_tablas()
        ok("Tablas creadas / verificadas: ciudades, registros_clima, metricas_etl")
    except Exception as e:
        fallo(f"Error al crear tablas: {e}")
        sys.exit(1)


def test_crud_ciudad(db):
    separador("TEST 3 — CRUD Ciudad")

    # CREATE
    ciudad = Ciudad(nombre="Bogotá_Test", pais="Colombia")
    db.add(ciudad)
    db.commit()
    db.refresh(ciudad)
    ok(f"Ciudad creada → id={ciudad.id}, nombre='{ciudad.nombre}'")

    # READ
    ciudad_leida = db.query(Ciudad).filter_by(nombre="Bogotá_Test").first()
    assert ciudad_leida is not None, "Ciudad no encontrada"
    ok(f"Ciudad leída  → {ciudad_leida}")

    # UPDATE
    ciudad_leida.pais = "Colombia (actualizado)"
    db.commit()
    db.refresh(ciudad_leida)
    assert ciudad_leida.pais == "Colombia (actualizado)"
    ok(f"Ciudad actualizada → pais='{ciudad_leida.pais}'")

    return ciudad_leida


def test_crud_registro_clima(db, ciudad):
    separador("TEST 4 — CRUD RegistroClima")

    # CREATE
    registro = RegistroClima(
        ciudad_id=ciudad.id,
        temperatura=22.5,
        sensacion_termica=21.0,
        humedad=75.0,
        velocidad_viento=15.0,
        descripcion="Parcialmente nublado",
        fecha_extraccion=datetime.utcnow()
    )
    db.add(registro)
    db.commit()
    db.refresh(registro)
    ok(f"RegistroClima creado → id={registro.id}, temp={registro.temperatura}°C")

    # READ con JOIN
    resultado = db.query(RegistroClima, Ciudad.nombre).join(Ciudad).filter(
        RegistroClima.id == registro.id
    ).first()
    assert resultado is not None, "Registro no encontrado con JOIN"
    ok(f"RegistroClima leído con JOIN → ciudad='{resultado[1]}', temp={resultado[0].temperatura}°C")

    # UPDATE
    registro.temperatura = 25.0
    db.commit()
    db.refresh(registro)
    assert registro.temperatura == 25.0
    ok(f"RegistroClima actualizado → temperatura={registro.temperatura}°C")

    return registro


def test_crud_metricas_etl(db):
    separador("TEST 5 — CRUD MetricasETL")

    # CREATE
    metrica = MetricasETL(
        fecha_ejecucion=datetime.utcnow(),
        estado="exitoso",
        registros_extraidos=5,
        registros_guardados=5,
        registros_fallidos=0,
        tiempo_ejecucion_segundos=2.34
    )
    db.add(metrica)
    db.commit()
    db.refresh(metrica)
    ok(f"MetricasETL creada → id={metrica.id}, estado='{metrica.estado}'")

    # READ
    metrica_leida = db.query(MetricasETL).filter_by(id=metrica.id).first()
    assert metrica_leida is not None
    ok(f"MetricasETL leída → guardados={metrica_leida.registros_guardados}, tiempo={metrica_leida.tiempo_ejecucion_segundos}s")

    return metrica_leida


def test_relacion_ciudad_registros(db, ciudad):
    separador("TEST 6 — Relación Ciudad → RegistroClima")

    ciudad_con_registros = db.query(Ciudad).filter_by(id=ciudad.id).first()
    total = len(ciudad_con_registros.registros)
    ok(f"Ciudad '{ciudad_con_registros.nombre}' tiene {total} registro(s) asociado(s)")
    assert total >= 1, "Se esperaba al menos 1 registro"


def test_limpieza(db, ciudad, registro, metrica):
    separador("TEST 7 — Limpieza de datos de prueba")

    db.delete(registro)
    db.commit()
    ok(f"RegistroClima id={registro.id} eliminado")

    db.delete(ciudad)
    db.commit()
    ok(f"Ciudad id={ciudad.id} eliminada (cascade elimina registros asociados)")

    db.delete(metrica)
    db.commit()
    ok(f"MetricasETL id={metrica.id} eliminada")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "🧪 " * 18)
    print("  SUITE DE PRUEBAS — ETL Weatherstack DB")
    print("🧪 " * 18)

    # Tests independientes de sesión
    test_conexion()
    test_crear_tablas()

    # Tests con sesión activa
    db = SessionLocal()
    try:
        ciudad  = test_crud_ciudad(db)
        registro = test_crud_registro_clima(db, ciudad)
        metrica  = test_crud_metricas_etl(db)
        test_relacion_ciudad_registros(db, ciudad)
        test_limpieza(db, ciudad, registro, metrica)

        separador("RESULTADO FINAL")
        print("  🎉 Todos los tests pasaron correctamente\n")

    except AssertionError as e:
        fallo(f"Aserción fallida: {e}")
        db.rollback()
        sys.exit(1)
    except Exception as e:
        fallo(f"Error inesperado: {e}")
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()