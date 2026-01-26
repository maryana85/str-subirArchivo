import streamlit as st
import pandas as pd
import os
import plotly.express as px
from io import BytesIO
from utils.funcionesV5 import histMuebles, unionFinal, detectarFormatoFecha, leerArchivo, nombreCEDIS, tablasAggregadas, saveResultMem, segundoFiltrado, filtroVarios


# Esta es una versión de prueba, con las modificaciones de la versión del proceso en dónde ya se incluye la parte de centros de nómina y los cambios a los catálogos iniciales (rutas) ...

YELLOW = '\033[33m'
PINK = '\033[95m'
RED = "\x1b[31m"
GREEN = "\x1b[32m"
CYAN = "\x1b[36m"
RESET = "\x1b[0m" # Resets the color and style

class TablaFinal:
    """create a python singleton class that auto run 1 method. This method will define one of its attributes:
    """
    _instance = None
    _initialized = False

    def __new__(cls, df):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, df):
        
        if self.__class__._initialized:
            return
        # ATRIBUTOS DE INSTANCIA
        # self.df     →  
        # self.totalFilas  
        
        # ATRIBUTOS DE INSTANCIA
        self.df = df        # → es el df ya procesado!!!
        # Auto-Run methods
        self._TotalFilas()
        self._EntregasRAC()
        self._EntregasNoRAC()        
        self.prtjRAC = f"{(self.entregasRAC / self.totalFilas):.0%}"
        self.prtjNoRAC = f"{(self.entregasNoRAC / self.totalFilas):.0%}"
        # self.totalHasClusterCE = self.df['has_cluster_ce'].sum()
        # self.totalHasHistCE = self.df['has_hist_ce'].sum()
        # self.totalCPs = len(self.df['Código_postal'].unique().tolist())
        # self.israc_cero = len(self.df[self.df['IS_RAC'] == 0])
        # self.israc_uno =len(self.df[self.df['IS_RAC'] == 1])
        # self.totalConCobertura = len(self.df[self.df['COBERTURA_CE'] == "CON_COBERTURA"])
        # self.totalSinCobertura = len(self.df[self.df['COBERTURA_CE'] == "SIN_COBERTURA"])
        # self.totalNORAC = len(self.df[self.df['COBERTURA_CE'] == "NORAC"])
        # self.totalCon_RAC = len(self.df[self.df['CON_RAC'] == "RAC"])        
        # self.totalIs_RAC_Izt = len(self.df[self.df['IS_RAC_Izt'] == 1])
        # self.valoresIs_RAC_Izt = self.df['IS_RAC_Izt'].unique().tolist()
        # self.tieneR = self.df['TieneR'].unique().tolist()


        self.__class__._initialized = True

    def _TotalFilas(self):
        self.totalFilas = len(self.df)

    def _EntregasRAC(self):
        self.entregasRAC = len( self.df[ self.df['IS_RAC'] == 1 ] )

    def _EntregasNoRAC(self):
        self.entregasNoRAC = len( self.df[ self.df['IS_RAC'] == 0 ] )



        

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


# ---------------------- Streamlit UI -----------------------

# App title
# 🚚
st.set_page_config(page_title="Herramienta Coppel", page_icon="📈", layout="wide")
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
        styled_df = df.style.map(
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

    # with st.container(width= "content")

    # File uploader
    uploaded_file = st.file_uploader("Sube un archivo CSV o Excel", type=["csv", "xlsx"])    

    if uploaded_file is not None:
        
        filename = uploaded_file.name
        # file_extension = os.path.splitext(filename)[1].lower()

        try:            
            df = leerArchivo(uploaded_file)
            
            # --- Read depending on extension ---
            # if file_extension == ".csv":
            #     df = pd.read_csv(uploaded_file)           
                
            # elif file_extension == ".xlsx":
            #     df = pd.read_excel(uploaded_file)
                
            # else:
            #     st.error("❌ Tipo de archivo inválido. Solo se permiten archivos .csv o .xlsx.")
            #     st.stop()
            #     df = None

            # formfecha = detectarFormatoFecha(df)           
            

            uploaded_columns = set(df.columns)

            # --- Validate schema ---
            missing_columns = EXPECTED_COLUMNS - uploaded_columns
            extra_columns = uploaded_columns - EXPECTED_COLUMNS
            

            if not missing_columns:
                st.success(f"✅ '{filename}' cargado correctamente con el esquema esperado.")
                st.write("**Columnas encontradas:**", list(df.columns))
                # st.dataframe(df.head())

                # ---------------------------------
                # DATA PREP
                # ---------------------------------
                # df["fecha"] = pd.to_datetime(df["fechaenrutada"], errors="coerce")                

                min_date = df["fechaenrutada"].min().date()
                max_date = df["fechaenrutada"].max().date()

                dictio_CEDIS = nombreCEDIS()                

                ubicaciones = sorted(df["ubicacionactual"].dropna().unique().tolist())

                # Intersecta la lista completa de CEDIS con sólo las claves que trae el archivo a procesar
                ubicaciones_dict = {key: dictio_CEDIS[key] for key in ubicaciones if key in dictio_CEDIS}

                                
                # ---------------------------------

                # ---------------------------------
                # SESSION STATE INIT
                # ---------------------------------
                if "fecha_inicial" not in st.session_state:
                    st.session_state.fecha_inicial = min_date
                if "fecha_final" not in st.session_state:
                    st.session_state.fecha_final = max_date
                if "ubicaciones" not in st.session_state:
                    st.session_state.ubicaciones = list( ubicaciones_dict.values() )

                # ---------------------------------
                # RESET ALL FILTERS
                # ---------------------------------
                def reset_filters():
                    st.session_state.fecha_inicial = min_date
                    st.session_state.fecha_final = max_date
                    st.session_state.ubicaciones = list( ubicaciones_dict.values() )

                st.sidebar.header("Filtros")
                st.sidebar.button("🔄 Resetear Filtros", on_click=reset_filters)

                # ---------------------------------
                # DATE FILTERS
                # ---------------------------------
                fecha_inicial = st.sidebar.date_input(
                    "Fecha Enrutada Inicial",
                    value=st.session_state.fecha_inicial,
                    min_value=min_date,
                    max_value=max_date,
                    key="fecha_inicial",
                    format="DD/MM/YYYY"
                )

                fecha_final = st.sidebar.date_input(
                    "Fecha Enrutada Final",
                    value=st.session_state.fecha_final,
                    min_value=min_date,
                    max_value=max_date,
                    key="fecha_final",
                    format="DD/MM/YYYY"
                )

                # ---------------------------------
                # UBICACION FILTER
                # ---------------------------------
                selected_ubicaciones = st.sidebar.multiselect(
                    "Ubicación actual (CEDIS)",
                    options= list(ubicaciones_dict.values()),
                    default=st.session_state.ubicaciones,
                    key="ubicaciones"
                )
                # selected_ubicaciones: será una lista con todos las valores del diccinario de ubicaciones_dict

                # ---------------------------------
                # FILTER BADGES (WITH ❌)
                # ---------------------------------
                st.sidebar.markdown("---")
                st.sidebar.subheader("Filtros Activos")

                # Fecha badge
                col1, col2 = st.sidebar.columns([8, 1])
                col1.markdown(f"📅 **Fecha:** {fecha_inicial} →\n {fecha_final}")
                if col2.button("❌", key="clear_fecha"):
                    st.session_state.fecha_inicial = min_date
                    st.session_state.fecha_final = max_date
                    st.rerun()

                # Ubicacion badge
                if len(selected_ubicaciones) != len(list( ubicaciones_dict.values() )):
                    
                    col1, col2 = st.sidebar.columns([8, 1])
                    col1.markdown(f"📍 **Ubicación:** {', '.join(selected_ubicaciones)}")
                    if col2.button("❌", key="clear_ubicacion"):
                        st.session_state.ubicaciones = selected_ubicaciones # ubicaciones.copy()
                        st.rerun()

                # ---------------------------------
                # APPLY FILTERS
                # ---------------------------------
                filtered_df = df.copy()

                # Voy a mandar todos los filtros al módulo de funciones para que ahí se haga todo eso
                
                df_proc = unionFinal(filtered_df, fecha_inicial, fecha_final, selected_ubicaciones)  # ← aquí se procesa el dataframe final

                # AQUÍ inicializo mi objeto
                t = TablaFinal(df_proc)
                # -----------------------------------------------


                if extra_columns:
                    st.warning(f"⚠️ Columnas adicionales encontradas: {list(extra_columns)}")

                # --- Example processing ---
                st.info("📊 El Procesamiento ha terminado.")
                # st.write(f"Total de filas: {len(df)}")
                # st.write(f"Total de columnas: {len(df.columns)}")

                st.divider()

                with st.expander("Indicadores"):
                    saule1, saule2, saule3 = st.columns(3) 

                    with saule1:
                        st.metric(label= "Total Entregas", value=  f'{t.totalFilas:,}' )

                    with saule2:
                        st.metric(label= "Entregas RAC", value= f'{t.entregasRAC:,}' ,
                                  delta= t.prtjRAC,
                                  delta_arrow = "off",
                                  delta_color = "off")

                    with saule3:
                        st.metric(label= "Entregas No RAC", value= f'{t.entregasNoRAC:,}', 
                                  delta= t.prtjNoRAC,
                                  delta_arrow = "off",
                                  delta_color = "off"
                                  )
                    

                st.markdown("<h3 style='text-align: center;'>Tabla Final</h3>", unsafe_allow_html=True)
                
                st.dataframe(df_proc,                             
                            column_config = {'fechaenrutada': st.column_config.DateColumn( format="DD-MM-YYYY"),
                                             'fecha': st.column_config.DateColumn( format="DD-MM-YYYY"),
                                             'Fecha_New': st.column_config.DateColumn( format="DD-MM-YYYY") ,
                                             'Fecha_en_Ruta_New': st.column_config.DateColumn( format="DD-MM-YYYY")
                            },
                            column_order=['tipo', 'folio', 'fecha', 'codigo', "articulo", "marca", "modelo", 'zona', 'jaula', 'ruta', 'fechaenrutada', 'ubicacionactual', 'NombreCEDIS', 'Fecha_New', 'Fecha_en_Ruta_New', 'IS_RAC', 'ID_RUTA', 'DCF', 'Seccion', 'Código_postal', 'Cluster', 'has_cluster_ce', 'has_hist_ce', 'COBERTURA_CE']
                            )
                
                columnas = st.columns(2)
                with columnas[0]:
                    st.write(f"Total filas = **{len(df_proc)}**")  
                    # st.write(f"Has Cluster CE = {t.totalHasClusterCE}")
                    # st.write(f"Has Hist CE = {t.totalHasHistCE}")
                    # st.write(f"CP's diferentes = {t.totalCPs}")
                    # st.write("")
                    # st.write(f"con RAC = {t.totalCon_RAC}")
                    # st.write("")                    
                    # st.write(f"IS RAC Izt = {t.totalIs_RAC_Izt}")
                    # st.write(f"Valores en IS RAC Izt = {t.valoresIs_RAC_Izt}")
                    # st.write(f"Valores con R en Jaula = {t.tieneR}")

                    # st.write("")
                    # st.write(f"IS_RAC 0 = {t.israc_cero}")
                    # st.write(f"IS_RAC 1 = {t.israc_uno}")
                    # st.write("")
                    # st.write(f"Con Cobertura = {t.totalConCobertura}")
                    # st.write(f"Sin Cobertura = {t.totalSinCobertura}")
                    # st.write(f"NORAC = {t.totalNORAC}")
                    
                    

                # Botón de descarga
                with columnas[1]:
                    
                    with st.container(horizontal=True, horizontal_alignment="right"):
                        st.download_button(
                            label="⬇️ Descargar archivo de resultado (resultado.csv)",
                            data= saveResultMem(df_proc),
                            file_name="resultado.csv",
                            mime="text/csv",
                        )

                
                st.divider()

                st.subheader("Filtros para las Tablas Pivote")
                colin1, colin2, colin3 = st.columns([3, 2, 3], gap = "medium")

                with colin1:
                    # ----------------- 1° Filtro ---------------------
                    cedis_selected = st.multiselect("CEDIS", options = selected_ubicaciones,  key="ID5", default = selected_ubicaciones) # opción de todas   
                  
                    # opciones_cluster= sorted(df_proc[df_proc["NombreCEDIS"] == cedis_selected]["Cluster"].unique().tolist())
                                          
                    opciones_cluster = sorted(
                        df_proc[
                            (df_proc["NombreCEDIS"].isin(cedis_selected)) &
                            (df_proc['fechaenrutada'].dt.date >= fecha_inicial) & 
                            (df_proc['fechaenrutada'].dt.date <= fecha_final)
                            ]["Cluster"].unique().tolist())
                        
                    # df_proc ya está filtrado con los filtros primarios, hay que quitar eso ↑

                    # (2°) Filtro 
                    cluster_selected = st.multiselect("Clusters", options =  opciones_cluster,  key="ID4", default = opciones_cluster) # en este filtro que sólo se pueda escoger 1 cluster 
                    
                    opciones_fecha = filtroVarios(df_proc, cedis_selected, cluster_selected, tipo = "fecha")

                    # (3°) filtro
                    fecha_selected = st.multiselect("Fecha Enrutada", options = opciones_fecha ,  key="ID6", default = opciones_fecha) # fecha en específico o todas

                    opciones_jaula = filtroVarios(df_proc, cedis_selected, cluster_selected, fecha_selected, tipo = "jaula")

                    # (4°) filtro
                    jaula_selected = st.multiselect("Jaula", options =  opciones_jaula,  key="ID7", default= opciones_jaula) 

                    opciones_RAC = filtroVarios(df_proc, cedis_selected, cluster_selected, fecha_selected, jaula_selected, tipo = "rac")
                    
                    # (5°) filtro
                    rac_selected = st.multiselect("RAC", options = opciones_RAC ,  key="ID8", default= opciones_RAC)
                    
                    opciones_Cobertura = filtroVarios(df_proc, cedis_selected, cluster_selected, fecha_selected, jaula_selected, rac_selected, tipo = "cobertura")

                    # (6°) filtro
                    cobertura_selected = st.multiselect("Cobertura", options = opciones_Cobertura,  key="ID9", default= opciones_Cobertura)

                with colin3:
                    st.write("")
                    # st.write(cedis_selected)
                    # st.write(cluster_selected)
                    # st.write(fecha_selected)
                    # st.write(jaula_selected)
                    # st.write(rac_selected)
                    # st.write(cobertura_selected)


                #---------------------------------------------------
                
                df_proc_filt = segundoFiltrado(df_proc, cedis_selected, cluster_selected, fecha_selected, jaula_selected, rac_selected, cobertura_selected)           
                

                # Aquí poner la tabla pivote
                aggCluster_df, aggJaula_df = tablasAggregadas(df_proc_filt)

                st.space("small")
                with st.container(border=True, width = 1500):

                    columnas = st.columns([3, 5])

                    with columnas[0]:                
                        st.dataframe(aggCluster_df.style \
                                    .set_properties(subset=['Total'],**{'font-weight': 'bold'}),
                                    width = 450,
                            column_config = {'CON_COBERTURA': st.column_config.NumberColumn(format="localized"),
                                "NORAC": st.column_config.NumberColumn(format="localized"),
                                "SIN_COBERTURA": st.column_config.NumberColumn(format="localized"),
                                "Total": st.column_config.NumberColumn(format="localized"),
                                },
                            column_order = ("CON_COBERTURA", "SIN_COBERTURA", "NORAC", "Total")
                            )
                        with st.container(horizontal= True, horizontal_alignment="right"):                     
                            st.download_button(
                                label="⬇️ Descargar tabla de Resultados 1 (tabla_Clusters_agg.csv)",
                                data= saveResultMem(aggCluster_df.reset_index()),
                                file_name="tabla_Clusters_agg.csv",
                                mime="text/csv",
                            )

                    with columnas[1]:
                        # hay que resetear el dataframe de aggClusters y borrar la última fila de Totales para poder graficar
                        fig_C = px.pie(aggCluster_df.reset_index().iloc[:-1], values= 'Total', names='Cluster',
                                        title=f'Gráfico × Clusters',
                                        height=450, width=300)
                        fig_C.update_layout(margin=dict(l=20, r=20, t=30, b=0),)
                        st.plotly_chart(fig_C, use_container_width=True)

                st.space("small")

                with st.container(border=True):

                    columnas = st.columns([3, 5])

                    with columnas[0]:
                        st.dataframe(aggJaula_df.style \
                                    .set_properties(subset=['Total'],**{'font-weight': 'bold'}),
                                     width = 450,
                            column_config = {
                                'CON_COBERTURA': st.column_config.NumberColumn(format="localized"),
                                "NORAC": st.column_config.NumberColumn(format="localized"),
                                "SIN_COBERTURA": st.column_config.NumberColumn(format="localized"),
                                "Total": st.column_config.NumberColumn(format="localized"),
                                },
                            column_order = ("CON_COBERTURA", "SIN_COBERTURA", "NORAC",  "Total")
                                )
                        with st.container(horizontal= True, horizontal_alignment="right"):                     
                            st.download_button(
                                label="⬇️ Descargar tabla de Resultados 2 (tabla_Jaula_agg.csv)",
                                data= saveResultMem(aggJaula_df.reset_index()),
                                file_name="tabla_Jaula_agg.csv",
                                mime="text/csv",
                            )
                    with columnas[1]:
                        fig_J = px.pie(aggJaula_df.reset_index().iloc[:-1], values= 'Total', names='Jaula',
                                        title=f'Gráfico × Jaulas',
                                        height=450, width=300)

                        # fig_J = px.bar(aggJaula_df.reset_index().iloc[:-1], y = 'Total', x ='Jaula',
                        #                 title=f'Gráfico × Jaulas',
                        #                 height=450, width=300, ) # color = 'Jaula'

                        fig_J.update_layout(margin=dict(l=20, r=20, t=30, b=0),)
                        st.plotly_chart(fig_J, use_container_width=True)

                        # st.bar_chart(aggJaula_df, x = 'Jaula', y = 'Total', color=["#0000FF"], height=450, width=300)


                # st.divider()
                # st.header("Tablas Pivote")
                st.write()
                st.write()
                # st.markdown("<h3 style='text-align: center;'>Tablas Pivote</h3>", unsafe_allow_html=True)
                

                # col1, col2 = st.columns(2)
                # with col1:
                #     data = [["IS_RAC", "1"],
                #             ["COBERTURA_CE", "CON_COBERTURA"]]
                #     tabla = pd.DataFrame(data, columns=["1", "2"]) \
                #             .style.hide(axis='columns') \
                #             .hide(axis="index") \
                #             .set_properties(**{'background-color': "#f0f5ff", 'color': 'black'}) \
                #             .set_properties(**{'width': '200px'})

                #     # st.table(tabla.style.hide_columns())
                #     st.write(tabla.to_html(), unsafe_allow_html=True)

                #     pivot_df = df_proc[ ( df_proc['IS_RAC'] == 1 )  &  (df_proc['COBERTURA_CE'] == "CON_COBERTURA") ]
                    

                #     etwas =  ['Todas las fechas'] + [d.astype('datetime64[D]').item() for d in pivot_df['fechaenrutada'].unique() ]

                #     # print(GREEN + f"\n\tEsto es etwas: {etwas}"   + RESET)

                #     opc_fecha = st.selectbox("Fecha en Rutada", options = etwas,  key="ID1") 

                #     st.dataframe(pivoteVal(df_proc, opc_fecha) )

                # with col2:
                #     data = [["IS_RAC", "1"],
                #             ["COBERTURA_CE", "(Multiple Items)"]]
                #     tabla = pd.DataFrame(data, columns=["1", "2"]) \
                #             .style.hide(axis='columns') \
                #             .hide(axis="index") \
                #             .set_properties(**{'background-color': "#f0f5ff", 'color': 'black'}) \
                #             .set_properties(**{'width': '200px'})

                #     # st.table(tabla.style.hide_columns())
                #     st.write(tabla.to_html(), unsafe_allow_html=True)

                #     pivot2_df = df_proc[ ( df_proc['IS_RAC'] == 1 ) ]

                #     etwas2 = ['Todas las fechas'] + [ d.astype('datetime64[D]').item() for d in pivot2_df['fechaenrutada'].unique() ] 

                #     opc_fecha2 = st.selectbox("Fecha en Rutada", options = etwas2, key="ID2")

                #     st.dataframe(pivoteVal_2(df_proc, opc_fecha2))
                
                # # st.write(df_proc)

                # # Crear resultado simple (puedes cambiarlo a una operación más compleja)
                # result_df = pd.DataFrame({
                #     "Total_Filas": [len(df)],
                #     "Total_Columnas": [len(df.columns)]
                # })                

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
