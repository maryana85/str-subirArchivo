import streamlit as st
import pandas as pd
import os
from io import BytesIO
from utils.funcionesV3 import histMuebles, unionFinal, detectarFormatoFecha


# ✅ Expected columns (in any order)
EXPECTED_COLUMNS = {
    "fecha", 
    "tipo",
    "ubicacionactual",
    "fechaenrutada",
    "jaula",
    "ciudad",
    "ruta",
    "zona"
}

# App title
st.title("🚚 Subidor de CSV/XLSX con Validación de Esquema")

# File uploader
uploaded_file = st.file_uploader("Sube un archivo CSV o Excel", type=["csv", "xlsx"])

if uploaded_file is not None:
    filename = uploaded_file.name
    file_extension = os.path.splitext(filename)[1].lower()

    try:
        # --- Read depending on extension ---
        if file_extension == ".csv":
            df = pd.read_csv(uploaded_file)
            
        elif file_extension == ".xlsx":
            df = pd.read_excel(uploaded_file)
            
        else:
            st.error("❌ Tipo de archivo inválido. Solo se permiten archivos .csv o .xlsx.")
            st.stop()
            df = None

        formfecha = detectarFormatoFecha(df)
        st.write(formfecha)
        # df_proc = histMuebles(df, formfecha)
        df_proc = unionFinal(df, formfecha)

        uploaded_columns = set(df.columns)

        # --- Validate schema ---
        missing_columns = EXPECTED_COLUMNS - uploaded_columns
        extra_columns = uploaded_columns - EXPECTED_COLUMNS

        if not missing_columns:
            st.success(f"✅ '{filename}' cargado correctamente con el esquema esperado.")
            st.write("**Columnas encontradas:**", list(df.columns))
            st.dataframe(df.head())

            if extra_columns:
                st.warning(f"⚠️ Columnas adicionales encontradas: {list(extra_columns)}")

            # --- Example processing ---
            st.info("📊 Procesamiento de ejemplo: conteo de filas y columnas.")
            st.write(f"Total de filas: {len(df)}")
            st.write(f"Total de columnas: {len(df.columns)}")

            st.divider()
            st.dataframe(df_proc)
            # st.write(df_proc)

            # Crear resultado simple (puedes cambiarlo a una operación más compleja)
            result_df = pd.DataFrame({
                "Total_Filas": [len(df)],
                "Total_Columnas": [len(df.columns)]
            })

            # Guardar resultado en memoria
            buffer = BytesIO()
            result_df.to_csv(buffer, index=False)
            buffer.seek(0)

            # Botón de descarga
            st.download_button(
                label="⬇️ Descargar archivo de resultado (resultado.csv)",
                data=buffer,
                file_name="resultado.csv",
                mime="text/csv",
            )

        else:
            st.error("❌ ¡Esquema inválido detectado!")
            st.write("**Columnas faltantes:**", list(missing_columns))
            st.write("**Columnas esperadas:**", list(EXPECTED_COLUMNS))
            st.write("**Columnas encontradas:**", list(df.columns))

    except Exception as e:
        st.error(f"⚠️ Error al leer o procesar el archivo: {e}")

else:
    st.info("Por favor, sube un archivo CSV o XLSX con las columnas requeridas:")
    st.code("\n".join(EXPECTED_COLUMNS))


# streamlit run mainGPTv2.py
