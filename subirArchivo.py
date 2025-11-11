import streamlit as st
import pandas as pd
import os
from io import BytesIO
from datetime import date
from funciones import histMuebles, unionFinal, detectarFormatoFecha

# Expected columns
EXPECTED_COLUMNS = {"fecha",
                    "Tipo_Art",
                    "ubicacionactual",
                    "fechaenrutada",
                    "jaula" ,
                    "ciudad",
                    "ruta",
                    "zona"}

# App title
st.title("📋 Herramienta para procesar el archivo de Históricos Muebles (Entregas)")
st.write("  ")
st.write("  ")

# File uploader
uploaded_file = st.file_uploader("Upload a CSV file", type=None)

if uploaded_file is not None:
    filename = uploaded_file.name
    file_extension = os.path.splitext(filename)[1].lower()

    # Validate extension
    if file_extension == ".csv" or file_extension == ".xlsx":
        try:
            # Read CSV
            df = histMuebles(filename, detectarFormatoFecha(filename) )
            # df = pd.read_csv(uploaded_file)
            uploaded_columns = set(df.columns)

            # Validate schema
            missing_columns = EXPECTED_COLUMNS - uploaded_columns
            extra_columns = uploaded_columns - EXPECTED_COLUMNS

            if not missing_columns:
                st.success(f"✅ '{filename}' se cargó exitosamente con el siguiente esquema:")
                st.write("  ")
                st.write("**Columnas encontradas:**", list(df.columns))
                st.write("  ")
                # st.dataframe(df.head())

                if extra_columns:
                    pass
                    # st.warning(f"⚠️ Extra columns found: {list(extra_columns)}")
                    

                # --- PROCESSING SECTION ---
                try:
                    # Procesar el dataframe con los demás catálogos
                    df_final = unionFinal(df)

                    # Ensure Age is numeric
                    # df["Age"] = pd.to_numeric(df["Age"], errors="coerce")

                    # Compute sum of Age
                    # age_sum = df["Age"].sum()

                    # st.success(f"🧮 The sum of all values in 'Age' column is: {age_sum}")
                    st.success(f"🧮 Se han procesado corretamente los datos, en un momento podrás descargar el archivo final...")

                    
                    # Save result to in-memory buffer
                    buffer = BytesIO()
                    df_final.toPandas().to_csv(buffer, index = False)
                    # result_df.to_csv(buffer, index=False)
                    buffer.seek(0)
                    fecha = date.today().strftime("%d %b %Y")

                    # Download button
                    st.divider()
                    st.download_button(
                        label= f"⬇️ Descarga el archivo resultante (HistóricoEntregasMueblesAA - {fecha}.csv)",
                        data= buffer,
                        file_name= f"HistóricoEntregasMueblesAA - {fecha}.csv", 
                        mime= "text/csv",
                    )
                except Exception as e:
                    st.error(f"⚠️ Las columnas necesarias para procesar el archivo no fueron encontradas: {e}")                

            else:
                st.error("❌ Se detectó un esquema inválido!")
                st.write("**Faltan columnas:**", list(missing_columns))
                st.write("**Columnas:**", list(EXPECTED_COLUMNS))
                st.write("**Columnas encontradas:**", list(df.columns))
                
            st.divider()

        except Exception as e:
            st.error(f"⚠️ Error reading CSV: {e}")
        

    else:
        st.error("❌ Tipo de archivo inválido! Por favor carga un archivo con extensión '.csv' unicamente.")
else:
    st.info("Por favor carga un archivo CSV con las siguientes columnas: fecha, Tipo_Art, ubicacionactual, fechaenrutada, jaula,ciudad, ruta, zona")

# streamlit run subirArchivo.py