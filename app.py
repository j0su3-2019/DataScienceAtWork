import streamlit as st
import pandas as pd
import numpy as np
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

# --- 1. CONFIGURACIÓN (LLENAR CON TUS DATOS) ---
# AZURE BLOB STORAGE
BLOB_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=sttiendavisionjm;AccountKey=bI9zCSBLdQOpL/YfMWsepN1FTNnxGejJDTnv2pZ/Y+qnnHS7UOyBjPwFGDl9h5dcRuO7peuCaENK+AStJek0pA==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "inputs"

# AZURE SQL DATABASE
DB_SERVER = "srv-tiendavisionjm.database.windows.net" 
DB_NAME = "sql-tiendavision-db"
DB_USER = "adminuser"
DB_PASS = "DataScienceAtWork.2025"
DRIVER = "ODBC Driver 17 for SQL Server"

# Crear cadena de conexión SQL segura
DATABASE_URL = f"mssql+pyodbc://{DB_USER}:{DB_PASS}@{DB_SERVER}/{DB_NAME}?driver={DRIVER.replace(' ', '+')}"

# --- 2. FUNCIONES DE LÓGICA DE NEGOCIO ---

def upload_to_blob(file_obj, file_name):
    """Sube el archivo crudo a Azure Blob Storage como backup"""
    try:
        blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=file_name)
        blob_client.upload_blob(file_obj, overwrite=True)
        return True
    except Exception as e:
        st.error(f"Error en Blob Storage: {e}")
        return False

def generate_forecast(df_detail):
    """
    Recibe el detalle de ventas y genera una predicción simple
    Lógica: Promedio de ventas diarias de los últimos 30 días para predecir la próxima semana.
    """
    # Asegurar formato de fecha
    df_detail['Fecha_Venta'] = pd.to_datetime(df_detail['Fecha_Venta'])
    
    # Agrupar por Producto
    # Calculamos el promedio diario de venta por producto
    forecast = df_detail.groupby(['Codigo_Producto', 'Nombre_Producto', 'Categoria'])['Cantidad'].mean().reset_index()
    forecast.rename(columns={'Cantidad': 'Venta_Diaria_Promedio'}, inplace=True)
    
    # Predicción: Lo que venderé en los próximos 7 días
    forecast['Prediccion_Semana_Entrante'] = np.ceil(forecast['Venta_Diaria_Promedio'] * 7) # Redondear hacia arriba
    forecast['Fecha_Prediccion'] = datetime.now()
    
    return forecast

def process_and_save_to_sql(file_obj):
    """Lee el Excel, procesa la data y la guarda en Azure SQL"""
    try:
        # 1. Crear motor SQL
        engine = create_engine(DATABASE_URL)
        
        # 2. Leer Excel
        file_obj.seek(0) 
        df = pd.read_excel(file_obj, sheet_name='Detalle_Ventas_Entrenamiento')
        
        # 3. Generar Predicciones
        with st.spinner('Calculando predicciones...'):
            df_pred = generate_forecast(df)

        # 4. GUARDAR EN SQL CON CHUNKSIZE
        # Usamos engine.connect() para máxima compatibilidad
        with engine.connect() as connection:
            
            with st.spinner(f'Guardando {len(df)} registros históricos...'):
                # chunksize=50 significa: "Sube de 50 en 50 filas". 
                # Esto evita saturar la memoria de Azure SQL.
                df.to_sql('ventas_historicas', con=connection, if_exists='replace', index=False, chunksize=50)
            
            with st.spinner(f'Guardando {len(df_pred)} predicciones...'):
                df_pred.to_sql('predicciones_inventario', con=connection, if_exists='replace', index=False, chunksize=50)
                
            connection.commit()
            
        return True, len(df), len(df_pred)
        
    except Exception as e:
        st.error(f"Error Detallado: {e}")
        return False, 0, 0

# --- 3. INTERFAZ GRÁFICA (FRONTEND) ---
st.set_page_config(page_title="TiendaVisión 2.0", page_icon="📈", layout="centered")

st.image("https://cdn-icons-png.flaticon.com/512/3081/3081559.png", width=100)
st.title("TiendaVisión: Panel de Control")
st.markdown("### Sistema de Predicción de Inventario para Tiendas de Barrio")
st.info("Sube tu reporte de Facturación FEL (Excel) para actualizar el tablero.")

uploaded_file = st.file_uploader("Cargar Reporte Mensual (Excel)", type=["xlsx"])

if uploaded_file is not None:
    st.write(f"📄 Archivo: **{uploaded_file.name}**")
    
    if st.button("🚀 Procesar Datos y Predecir", type="primary"):
        # 1. Subir a Blob (Backup)
        subido_blob = upload_to_blob(uploaded_file, uploaded_file.name)
        
        if subido_blob:
            st.success("✅ Archivo respaldado en la nube (Azure Blob).")
            
            # 2. Procesar SQL
            exito_sql, filas_hist, filas_pred = process_and_save_to_sql(uploaded_file)
            
            if exito_sql:
                st.balloons()
                st.success("✅ ¡Proceso Terminado!")
                
                # Métricas Resumen
                col1, col2 = st.columns(2)
                col1.metric("Ventas Procesadas", f"{filas_hist:,}")
                col2.metric("Predicciones Generadas", f"{filas_pred:,}")
                
                st.markdown("---")
                st.markdown("👉 **Ahora puedes actualizar tu Power BI.** Los datos ya están en la base de datos.")
            
        else:
            st.error("Falló la subida al Storage. Revisa tu conexión.")

st.markdown("---")
st.caption("Maestría en Business Intelligence - Proyecto Data Science at Work")