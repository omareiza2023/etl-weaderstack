#!/usr/bin/env python3
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from sqlalchemy import func, and_
import sys
import os

# --- SOLUCIÓN DE RUTAS PARA STREAMLIT CLOUD ---
# Esto asegura que Python encuentre la carpeta 'scripts' sin importar dónde se ejecute
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)
# ----------------------------------------------

from scripts.database import SessionLocal
from scripts.models import Ciudad, RegistroClima

st.set_page_config(
    page_title="Dashboard Interactivo - Clima",
    page_icon="🎛️",
    layout="wide"
)

# CSS personalizado para mejorar la estética
st.markdown("""
    <style>
    .metric-box {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
    </style>
""", unsafe_allow_html=True)

st.title("🎛️ Dashboard Interactivo - ETL Weatherstack")

# Inicializar conexión a la base de datos
db = SessionLocal()

try:
    # Sidebar con controles
    st.sidebar.markdown("### 🔧 Controles de Filtrado")

    # Selector de ciudades desde la DB
    ciudades_db = db.query(Ciudad).all()
    ciudades_disponibles = [c.nombre for c in ciudades_db]
    
    ciudades_seleccionadas = st.sidebar.multiselect(
        "🏙️ Seleccionar Ciudades",
        options=ciudades_disponibles,
        default=ciudades_disponibles[:2] if ciudades_disponibles else []
    )

    # Rango de fechas
    st.sidebar.markdown("---")
    fecha_inicio = st.sidebar.date_input(
        "📅 Fecha Inicio:",
        value=datetime.now() - timedelta(days=30)
    )
    fecha_fin = st.sidebar.date_input(
        "📅 Fecha Fin:",
        value=datetime.now()
    )

    # Filtros de temperatura
    st.sidebar.markdown("---")
    temp_min, temp_max = st.sidebar.select_slider(
        "🌡️ Rango de Temperatura (°C):",
        options=list(range(-50, 51)),
        value=(-10, 40)
    )

    # Consulta a la base de datos con filtros
    query = db.query(
        RegistroClima,
        Ciudad.nombre.label("ciudad_nombre"),
        Ciudad.pais.label("pais")
    ).join(Ciudad).filter(
        and_(
            Ciudad.nombre.in_(ciudades_seleccionadas),
            RegistroClima.fecha_extraccion >= fecha_inicio,
            RegistroClima.fecha_extraccion <= fecha_fin,
            RegistroClima.temperatura >= temp_min,
            RegistroClima.temperatura <= temp_max
        )
    )

    registros_filtrados = query.all()

    # Construir DataFrame
    data = []
    for registro, ciudad_nombre, pais in registros_filtrados:
        data.append({
            'Ciudad': ciudad_nombre,
            'País': pais,
            'Temperatura': registro.temperatura,
            'Sensación': registro.sensacion_termica,
            'Humedad': registro.humedad,
            'Viento': registro.velocidad_viento,
            'Descripción': registro.descripcion,
            'Fecha': registro.fecha_extraccion
        })

    df = pd.DataFrame(data) if data else pd.DataFrame()

    if not df.empty:
        # --- KPIs ---
        st.markdown("### 📊 Indicadores Clave")
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric("🌡️ Temp Max", f"{df['Temperatura'].max():.1f}°C")
        with col2:
            st.metric("🌡️ Temp Min", f"{df['Temperatura'].min():.1f}°C")
        with col3:
            st.metric("🌡️ Temp Prom", f"{df['Temperatura'].mean():.1f}°C")
        with col4:
            st.metric("💧 Humedad Prom", f"{df['Humedad'].mean():.1f}%")
        with col5:
            st.metric("💨 Viento Max", f"{df['Viento'].max():.1f} km/h")
        
        st.markdown("---")
        
        # --- Gráficas ---
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Comparativa de Temperaturas")
            fig_box = px.box(
                df, x='Ciudad', y='Temperatura', color='Ciudad',
                title='Distribución de Temperaturas por Ciudad'
            )
            st.plotly_chart(fig_box, use_container_width=True)
        
        with col2:
            st.markdown("#### Promedio de Humedad")
            humedad_ciudad = df.groupby('Ciudad')['Humedad'].mean().reset_index()
            fig_bar = px.bar(
                humedad_ciudad, x='Ciudad', y='Humedad', color='Humedad',
                color_continuous_scale='Blues'
            )
            st.plotly_chart(fig_bar, use_container_width=True)
        
        # --- Evolución Temporal ---
        st.markdown("#### 📈 Evolución Temporal")
        df['Fecha'] = pd.to_datetime(df['Fecha'])
        temp_tiempo = df.groupby(['Fecha', 'Ciudad'])['Temperatura'].mean().reset_index()
        fig_line = px.line(
            temp_tiempo, x='Fecha', y='Temperatura', color='Ciudad',
            markers=True, title='Histórico de Temperaturas'
        )
        st.plotly_chart(fig_line, use_container_width=True)
        
        # --- Tabla de Datos ---
        st.markdown("---")
        st.markdown("#### 📋 Datos Detallados")
        columnas_mostrar = st.multiselect(
            "Seleccionar columnas:",
            df.columns.tolist(),
            default=['Ciudad', 'Temperatura', 'Humedad', 'Descripción', 'Fecha']
        )
        st.dataframe(df[columnas_mostrar], use_container_width=True)

        # Botón de Descarga
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="⬇️ Descargar CSV",
            data=csv,
            file_name=f"clima_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )
    else:
        st.info("💡 No hay datos para los filtros seleccionados. Intenta ampliar el rango de fechas o seleccionar más ciudades.")

except Exception as e:
    st.error(f"❌ Error al conectar o procesar los datos: {e}")

finally:
    db.close()