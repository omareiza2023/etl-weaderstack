#!/usr/bin/env python3
from sqlalchemy import (
    Column, Integer, String, Float, DateTime, ForeignKey
)
from sqlalchemy.orm import relationship, declarative_base
from datetime import datetime

Base = declarative_base()


class Ciudad(Base):
    __tablename__ = 'ciudades'

    id = Column(Integer, primary_key=True, autoincrement=True)
    nombre = Column(String(100), nullable=False, unique=True)
    pais = Column(String(100), nullable=True)

    registros = relationship('RegistroClima', back_populates='ciudad', cascade='all, delete-orphan')

    def __repr__(self):
        return f"<Ciudad(id={self.id}, nombre='{self.nombre}', pais='{self.pais}')>"


class RegistroClima(Base):
    __tablename__ = 'registros_clima'

    id = Column(Integer, primary_key=True, autoincrement=True)
    ciudad_id = Column(Integer, ForeignKey('ciudades.id'), nullable=False)
    temperatura = Column(Float, nullable=True)
    sensacion_termica = Column(Float, nullable=True)
    humedad = Column(Float, nullable=True)
    velocidad_viento = Column(Float, nullable=True)
    descripcion = Column(String(255), nullable=True)
    fecha_extraccion = Column(DateTime, default=datetime.utcnow, nullable=False)

    ciudad = relationship('Ciudad', back_populates='registros')

    def __repr__(self):
        return (
            f"<RegistroClima(id={self.id}, ciudad_id={self.ciudad_id}, "
            f"temperatura={self.temperatura}, fecha={self.fecha_extraccion})>"
        )


class MetricasETL(Base):
    __tablename__ = 'metricas_etl'

    id = Column(Integer, primary_key=True, autoincrement=True)
    fecha_ejecucion = Column(DateTime, default=datetime.utcnow, nullable=False)
    estado = Column(String(50), nullable=False)
    registros_extraidos = Column(Integer, default=0)
    registros_guardados = Column(Integer, default=0)
    registros_fallidos = Column(Integer, default=0)
    tiempo_ejecucion_segundos = Column(Float, default=0.0)

    def __repr__(self):
        return (
            f"<MetricasETL(id={self.id}, estado='{self.estado}', "
            f"guardados={self.registros_guardados}, fecha={self.fecha_ejecucion})>"
        )
