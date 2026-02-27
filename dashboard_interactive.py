#!/usr/bin/env python3
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from sqlalchemy import func, and_
import sys
import os

# --- CONFIGURACIÓN DE RUTAS CRÍTICA ---
# Obtenemos la ruta absoluta de la carpeta donde está este archivo
base_path = os.path.dirname(os.path.abspath(__file__))
scripts_path = os.path.join(base_path, "scripts")

# Añadimos ambas rutas al inicio del sistema de búsqueda de Python
if base_path not in sys.path:
    sys.path.insert(0, base_path)
if scripts_path not in sys.path:
    sys.path.insert(0, scripts_path)

# Ahora importamos directamente de los archivos dentro de /scripts
# Nota: Si el error persiste, asegúrate de que el archivo __init__.py exista en /scripts
try:
    from database import SessionLocal
    from models import Ciudad, RegistroClima
except ImportError:
    # Intento de respaldo si la estructura de paquetes aún se resiste
    from scripts.database import SessionLocal
    from scripts.models import Ciudad, RegistroClima
# --------------------------------------

st.set_page_config(
    page_title="Dashboard Interactivo - Clima",
    page_icon="🎛️",
    layout="wide"
)

# Estilos CSS
st.markdown("""
    <style>
    .main { background-color: #f8f9fa; }
    .stMetric { background-color: #ffffff; padding: 15px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
    </style>
""", unsafe_allow_html=True)

st.title("🎛️ Dashboard Interactivo - ETL Weatherstack")

# Conexión a la base de datos
db = SessionLocal()

try:
    # --- BARRA LATERAL (SIDEBAR) ---
    st.sidebar.header("🔧 Configuración")
    
    ciudades_db = db.query(Ciudad).all()
    nombres_ciudades = [c.nombre for c in ciudades_db]
    
    ciudades_sel = st.sidebar.multiselect(
        "Seleccione Ciudades:",
        options=nombres_ciudades,
        default=nombres_ciudades[:2] if nombres_ciudades else []
    )

    fecha_inicio = st.sidebar.date_input("Desde:", datetime.now() - timedelta(days=7))
    fecha_fin = st.sidebar.date_input("Hasta:", datetime.now())

    temp_min, temp_max = st.sidebar.slider("Rango Temp. (°C):", -20, 50, (-5, 45))

    # --- CONSULTA DE DATOS ---
    query = db.query(
        RegistroClima, 
        Ciudad.nombre.label("ciudad_nombre")
    ).join(Ciudad).filter(
        and_(
            Ciudad.nombre.in_(ciudades_sel),
            RegistroClima.fecha_extraccion >= fecha_inicio,
            RegistroClima.fecha_extraccion <= fecha_fin,
            RegistroClima.temperatura >= temp_min,
            RegistroClima.temperatura <= temp_max
        )
    )

    df = pd.read_sql(query.statement, db.bind)

    if not df.empty:
        # Convertir fecha a formato legible
        df['fecha_extraccion'] = pd.to_datetime(df['fecha_extraccion'])

        # --- KPIs ---
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Máxima", f"{df['temperatura'].max()}°C")
        col2.metric("Mínima", f"{df['temperatura'].min()}°C")
        col3.metric("Humedad Prom.", f"{int(df['humedad'].mean())}%")
        col4.metric("Registros", len(df))

        st.markdown("---")

        # --- GRÁFICAS ---
        c1, c2 = st.columns(2)
        
        with c1:
            fig_temp = px.line(df, x='fecha_extraccion', y='temperatura', color='ciudad_nombre',
                             title="Evolución de Temperatura", markers=True)
            st.plotly_chart(fig_temp, use_container_width=True)
            
        with c2:
            fig_hum = px.bar(df.groupby('ciudad_nombre')['humedad'].mean().reset_index(), 
                           x='ciudad_nombre', y='humedad', title="Humedad Promedio por Ciudad")
            st.plotly_chart(fig_hum, use_container_width=True)

        # --- TABLA ---
        st.subheader("📋 Detalle de Datos")
        st.dataframe(df[['ciudad_nombre', 'temperatura', 'humedad', 'descripcion', 'fecha_extraccion']], 
                     use_container_width=True)

    else:
        st.info("No se encontraron datos con los filtros seleccionados.")

except Exception as e:
    st.error(f"Error en la aplicación: {e}")
    st.info("Asegúrate de que la base de datos esté accesible desde Streamlit Cloud.")

finally:
    db.close()