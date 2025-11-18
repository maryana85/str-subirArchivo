import streamlit as st
import pandas as pd
import os
from io import BytesIO
from utils.funcionesV3 import histMuebles, unionFinal, detectarFormatoFecha, pivoteVal, pivoteVal_2


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
# 🚚
st.title("📋 Herramienta para procesar el archivo de Históricos Muebles (Entregas)")


# Create two tabs
tab1, tab2 = st.tabs(["📚 Documentación", "📁 Procesar Archivo"])


# ---------------------- TAB 1 ----------------------
with tab1:
    st.header("Documentación 📚")
    st.write("""
    Usa la pestaña **'Procesar Archivo'** para subir tu archivo CSV o Excel,
    validar el esquema y generar un archivo procesado.
    """)

    
    with st.expander("Entradas"):
        data = {
            "Archivo": ["Códigos Postales por cluster ce.xlsx",
                        "6.15 Catalogo Nacional Rutas Zonas.xlsx",
                        "7.1 Maestro Productos Muebles.xlsx",
                        "7.2 Medidas peso por dcf.xlsx",
                        "Historico Entregas Express_izp_AA_v1.xlsx",
                        "archivos de Histórico Muebles (Entregas)"],
            "Tipo": ["Catálogo", "Catálogo","Catálogo","Catálogo","Catálogo", "Entrada"],
            "Alias": ["clusters_df", "rutas_df", "codigosM_df", "pesos_df", "hist_exp_df", "entregas_df"]
        }
        df = pd.DataFrame(data)
        df = df.set_axis(range(1, len(df) + 1), axis=0)

        st.write('''
            Los archivos usados como entradas son los siguientes:
        ''')
        st.table(df)

    st.write("")    

    with st.expander("Limpieza"):
        
        st.markdown('''<ol>
                    <li>Códigos Postales por cluster ce.xlsx</li>
                        <ul>
                            <li>Homologación en el nombre de algunos clusters:
                                <blockquote>homolog_map = {
                                    "6CHL": "6 CHALCO",
                                    "CHM-2": "4 CHIMALHUACAN",
                                }</blockquote></li>
                            <li>Eliminación de CP's repetidos asignados a más de 1 clúster</li>
                            <li>Manejo de CP's como string de 5 caracteres de largo</li>
                            <li>Se agregó columna <code><strong>["has_cluster_ce"]</strong></code> </li>
                        </ul><br>
                    <li>6.15 Catalogo Nacional Rutas Zonas.xlsx:</li>
                    <ul>
                        <li>Eliminación de duplicados en columna <code><strong>["ZONA VENTA"]</strong></code></li>
                        <li>Manejo de CP's como string de 5 caracteres de largo</li>
                    </ul><br>
                    <li>7.1 Maestro Productos Muebles.xlsx</li>
                    <ul>
                        <li>Eliminación de duplicados en la columna <code><strong>["Codigo"]</strong></code></li>
                    </ul><br>
                    <li>7.2 Medidas peso por dcf.xlsx</li><br>
                    <li>Historico Entregas Express_izp_AA_v1.xlsx</li>
                    <ul>
                        <li>Manejo de CP's como string de 5 caracteres de largo</li>
                        <li>Eliminación de duplicados en la columna <code><strong>["Codigo postal"]</strong></code></li>
                    </ul>
                    </ol>                    
                    

                    
                    ''',
                        unsafe_allow_html=True)

        st.markdown('''''',    unsafe_allow_html=True)
        
    st.write("")

    with st.expander("Filtros y Normalización"):
        st.markdown('''<p>En la columna <code><strong>["jaula"]</strong></code> si el string contiene una <strong>“R”</strong></p>
        <p>En la columna <code><strong>["tipo"]</strong></code> si los valores están en <strong>["VB", "VS"]</strong></p>
        <p>En la columna <code><strong>["ubicacionactual"]</strong></code> si el valor es igual a  <strong>"30011"</strong></p>''',    unsafe_allow_html=True)
        
    st.write("")

    with st.expander("Uniones"):  
        data = {
            "Tablas": ["hist_exp_df", 
                        "clusters_df",
                        "rutas_df",
                        "entregas_df",
                        "codigosM_df",
                        "pesos_df"],
            "1":["","","","","", "Seccion"],
            "2":["","","","","DCF", "DCF"],
            "3":["","","","Codigo","Codigo", ""],
            "4":["","","zona","zona","", ""],
            "5":["Código_postal","Código_postal","Código_postal","","", ""],
        }
        df = pd.DataFrame(data)
        styled_df = df.style.applymap(
                lambda x: "background-color: lightgreen" if x != "" else "",
                subset=["1", "2", "3", "4", "5"]
                )
        
#         styled_df = df.style.set_table_styles([
#     {'selector': 'th', 'props': [('font-weight', 'bold')]}
# ])
        
        # styled_df = df.style.hide(axis='columns')
        
        st.table(styled_df)
        st.write('''  ''')

    st.write("")

    with st.expander("Salidas"):  

        st.markdown("<h3><code>HistóricoEntregasMueblesAA - <strong>{fecha}</strong>.csv</code></h3>",    unsafe_allow_html=True)


# ---------------------- TAB 2 ----------------------
with tab2:

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
            # df_proc = histMuebles(df, formfecha)
            df_proc = unionFinal(df, formfecha)

            uploaded_columns = set(df.columns)

            # --- Validate schema ---
            missing_columns = EXPECTED_COLUMNS - uploaded_columns
            extra_columns = uploaded_columns - EXPECTED_COLUMNS

            if not missing_columns:
                st.success(f"✅ '{filename}' cargado correctamente con el esquema esperado.")
                st.write("**Columnas encontradas:**", list(df.columns))
                # st.dataframe(df.head())

                if extra_columns:
                    st.warning(f"⚠️ Columnas adicionales encontradas: {list(extra_columns)}")

                # --- Example processing ---
                st.info("📊 El Procesamiento ha terminado.")
                # st.write(f"Total de filas: {len(df)}")
                # st.write(f"Total de columnas: {len(df.columns)}")

                st.divider()
                st.markdown("<h3 style='text-align: center;'>Tabla Final</h3>", unsafe_allow_html=True)

                st.dataframe(df_proc)
                st.divider()
                # st.header("Tablas Pivote")
                st.write()
                st.write()
                st.markdown("<h3 style='text-align: center;'>Tablas Pivote</h3>", unsafe_allow_html=True)
                

                col1, col2 = st.columns(2)
                with col1:
                    data = [["IS_RAC", "1"],
                            ["COBERTURA_CE", "CON_COBERTURA"]]
                    tabla = pd.DataFrame(data, columns=["1", "2"]) \
                            .style.hide(axis='columns') \
                            .hide(axis="index") \
                            .set_properties(**{'background-color': "#f0f5ff", 'color': 'black'}) \
                            .set_properties(**{'width': '200px'})

                    # st.table(tabla.style.hide_columns())
                    st.write(tabla.to_html(), unsafe_allow_html=True)

                    pivot_df = df_proc[ ( df_proc['IS_RAC'] == 1 )  &  (df_proc['COBERTURA_CE'] == "CON_COBERTURA") ]
                    opc_fecha = st.selectbox("Fecha en Rutada", options = pivot_df['fechaenrutada'].unique().tolist(),  key="ID1")

                    st.dataframe(pivoteVal(df_proc, opc_fecha))

                with col2:
                    data = [["IS_RAC", "1"],
                            ["COBERTURA_CE", "(Multiple Items)"]]
                    tabla = pd.DataFrame(data, columns=["1", "2"]) \
                            .style.hide(axis='columns') \
                            .hide(axis="index") \
                            .set_properties(**{'background-color': "#f0f5ff", 'color': 'black'}) \
                            .set_properties(**{'width': '200px'})

                    # st.table(tabla.style.hide_columns())
                    st.write(tabla.to_html(), unsafe_allow_html=True)

                    pivot2_df = df_proc[ ( df_proc['IS_RAC'] == 1 ) ]

                    opc_fecha2 = st.selectbox("Fecha en Rutada", options = pivot2_df['fechaenrutada'].unique().tolist(), key="ID2")

                    st.dataframe(pivoteVal_2(df_proc, opc_fecha2))
                
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
                st.write()
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


    # streamlit run mainGPTv5.py
