import pandas as pd
from pandas import DataFrame
from datetime import date
import os
import glob
from pprint import pprint
import pickle
from io import StringIO
import streamlit as st
import numpy as np
import csv
import io
from io import BytesIO
from datetime import datetime

"""Esta versión es sin SPARK y es la versión a Enero del 2026, a la par del archivo Entregas Muebles - cleaning v4.ipynb"""

YELLOW = '\033[33m'
PINK = '\033[95m'
RED = "\x1b[31m"
GREEN = "\x1b[32m"
CYAN = "\x1b[36m"
RESET = "\x1b[0m" # Resets the color and style

def detectarFormatoFecha(df: pd.DataFrame) -> str:
    # para saber el formato de fecha
    try:
        # df.select(to_date(col('fecha'), "yyyy-MM-dd"))        
        df['prueba']= pd.to_datetime(df['fecha'], format='%Y-%m-%d')
        formato =  '%Y-%m-%d'


    except:
        try:
            # df.select(to_date(col('fecha'), "dd-MM-yyy"))
            df['prueba']= pd.to_datetime(df['fecha'], format='%d-%m-%Y')
            formato =  '%d-%m-%Y'
        except:
            print("Ninguna coincidencia")

    # print("\n\n\n\n", formato,"\n\n\n\n")

    return formato

def _desencurtir() -> tuple:
    """Desencurte los archivos pkl que contienen los dataframes que están en pandas y los transforma en un dataframe de pandas
    Regresa 6 variables"""    
    
    archivos = ["centrosNomina_df.pkl",
                "cedis_dictio.pkl",
                "clustersV3_df.pkl", 
                "rutas_df.pkl", 
                "pesos_codigosM_df.pkl",
                "hisExprNacional_df.pkl"
               ]
        
    # "hist_exp_df.pkl",       → Out

    listaFinal = []

    for a in archivos:
        with open(a, 'rb') as file:
            data = pickle.load(file)
        
        listaFinal.append(data)

    return tuple(listaFinal)


# (6) Cargar el Catálogo de los archivos de Histórico Muebles (Entregas) → este es el que se va a procesar!!!
def histMuebles(df, fecha_inicial: date, fecha_final: date, selected_ubicaciones: list) -> DataFrame:
    """Cargar el Catálogo de los archivos de Histórico Muebles (Entregas) → este es el que se va a procesar!!!
    Para usar esta función las columnas de fecha y fechaenrutada ya deben de venir como tipo fecha
    Args:
        df: Al ingestar este df, unos archivo viene con coma y otros con pipe, CUIDADO!!!
        fecha_inicial:
        fecha_final: 
        selected_ubicaciones: lista de strings        
    """  

    columnasMantener = ["fecha","tipo", "ubicacionactual", "fechaenrutada", "jaula", "ruta", "zona",  "folio", "codigo",'Fecha_New', 'Fecha_en_Ruta_New', 'IS_RAC', 'ID_RUTA', "num_centronomina", "IS_RAC_Izt", "Cluster", "articulo", "marca", "modelo"] # "cantidad",  "mododeentrega", "cliente", "ciudad", "tienda"
        
    pre_df_ent = df
    pre_df_ent['ubicacionactual'] = pre_df_ent['ubicacionactual'].astype('string')
    pre_df_ent['num_centronomina'] = pre_df_ent['num_centronomina'].astype('string')
    pre_df_ent['codigo'] = pre_df_ent['codigo'].astype('string')
    

    # Filtros que vienen de la App de Streamlit
    # (1) - Filtro de Fechas
    if fecha_inicial <= fecha_final:
                    pre_df_ent = pre_df_ent[
                        (pre_df_ent["fechaenrutada"] >= pd.to_datetime(fecha_inicial)) &
                        (pre_df_ent["fechaenrutada"] <= pd.to_datetime(fecha_final))
                    ]
    # (2) - Filtro de ubicacionactual
    if selected_ubicaciones:
        ubicaciones_dict = nombreCEDIS()                    
        lista_codigos_ubi = [k for k, v in ubicaciones_dict.items() if v in selected_ubicaciones]
        #  saca sus keys en base a los values que son los que están guardados en la lista de selected_ubicaciones

        print(PINK + f"\n\nUbicaciones Seleccionadas: {selected_ubicaciones}\n\n" )         
        pprint(lista_codigos_ubi)
        print(RESET)

        pre_df_ent = pre_df_ent[
            pre_df_ent["ubicacionactual"].isin(lista_codigos_ubi)
            ]

    # (3) - Filtro de tipo
    tipo_keep = ["VB", "VS"]
    pre_df_ent = pre_df_ent[
                    (pre_df_ent['tipo'].isin(tipo_keep))                            
                ]
    
    
    pre_df_ent['Fecha_New'] = df['fecha']
    pre_df_ent['Fecha_en_Ruta_New'] = df['fechaenrutada']
    pre_df_ent.rename(columns={'Tipo_Art': 'tipo'}, inplace=True)

    # pre_df_ent['jaula'].astype(str).str.contains("R").astype(int)     
    

    pre_df_ent['IS_RAC_Izt'] =  np.nan

    cond_1 = pre_df_ent['ubicacionactual'] == '30011'
    cond_2 = pre_df_ent['jaula'].str.contains("R") 

    pre_df_ent['TieneR'] = pre_df_ent['jaula'].str.contains("R") 

    # Use df.loc to set the value where both conditions are true
    pre_df_ent.loc[cond_1 & cond_2, 'IS_RAC_Izt'] = 1

    
    pre_df_ent['zona'] = pre_df_ent['zona'].astype('string').str.zfill(width= 7)
    pre_df_ent['ID_RUTA'] = pre_df_ent['ciudad'].astype(str) + '-' + pre_df_ent['ruta'].astype(str) + '-' +  pre_df_ent['jaula'].astype(str)

    # Eliminar columnas innecesarias
    todas = pre_df_ent.columns.tolist()
    colsTirar = [i for i in todas if i not in columnasMantener]

    pre_df_ent.drop(colsTirar, axis=1, inplace=True)

    entregas_df = pre_df_ent
    
    print(f"\nLargo de pre_df_ent = {len(pre_df_ent)}\n")     

    return  entregas_df      # Dataframe

def _coberturaFunc(row):

    # print(type(row["IS_RAC"]))

    if row["IS_RAC"] == 0:
        return "NORAC"
    else:
        if ( pd.notna(row["has_hist_ce"]) ) or (pd.notna(row['has_cluster_ce']) ):        
            return "CON_COBERTURA"
        else: 
            return "SIN_COBERTURA"
  
def _ISRACFunc(row):

    # .withColumn("IS_RAC",
    #                     when(col("IS_RAC_Izt").isNotNull() , col("IS_RAC_Izt")
    #                          ).otherwise(
    #                         when(col("CON_RAC") == "RAC", 
    #                             lit(1)
    #                             ).otherwise(lit(0)))
    #                  ) \
     
    if pd.notna(row["IS_RAC_Izt"]):
        return row["IS_RAC_Izt"]
    
    else:        
        if row["CON_RAC"] == "RAC":     # la columna CON_RAC viene del de centros de nómina
            return 1
        else:
            return 0
    

# (7) Unión de los 5 dataframes para poder asignarles un clúster
def unionFinal(df, fecha_inicial, fecha_final, selected_ubicaciones) -> DataFrame:
    """los catálogos se jalan usando la función de desencurtir"""
    # lista_CPs = entregas_df.select('Código_postal').rdd.flatMap(lambda x: x).collect()

    entregas_df = histMuebles(df, fecha_inicial, fecha_final, selected_ubicaciones)       # 1
    # print("\n\nYA PROCESÓ ENTREGAS MUEBLES!!!!\n\n")
    
    cnomina_df, cedis_dictio, clusters_df, rutas_df, pesos_codigosM_df, hist_cps_norm = _desencurtir()
    
    # importante para que no tome los números del CP como números sino como stirngs.
    hist_cps_norm['Código_postal'] = hist_cps_norm['Código_postal'].astype('string').str.zfill(width= 5)
    clusters_df['Código_postal'] = clusters_df['Código_postal'].astype('string').str.zfill(width= 5)
    rutas_df['Código_postal'] = rutas_df['Código_postal'].astype('string').str.zfill(width= 5)
    rutas_df['zona'] = rutas_df['zona'].astype('string').str.zfill(width= 7)
    pesos_codigosM_df['codigo'] = pesos_codigosM_df['codigo'].astype('string')
    cnomina_df['num_centronomina'] = cnomina_df['num_centronomina'].astype('string')

    listaClus = rutas_df['Código_postal'].unique().tolist()

    print(YELLOW)
    print("\n\n", len(listaClus))
    print(RESET)

    
    cedis_df = pd.DataFrame(list(cedis_dictio.items()), columns=  ['ubicacionactual', 'NombreCEDIS'])

    cedis_df['ubicacionactual'] = cedis_df['ubicacionactual'].astype(str)

    # rutas_df['zona'] = rutas_df['zona'].astype(str).str.zfill(width= 7)

        
    # final_df = pd.merge(entregas_df, pesos_codigosM_df, on='codigo', how='left')
    final_df = entregas_df.merge(cnomina_df, on= "num_centronomina", how= 'left') \
                        .merge(cedis_df, on= 'ubicacionactual', how= 'left') \
                        .merge(pesos_codigosM_df, on='codigo', how='left') \
                        .merge(rutas_df, on="zona", how="left") \
                        .merge(clusters_df, on= "Código_postal", how = 'left') \
                        .merge(hist_cps_norm, on = "Código_postal", how= "left") \
                        
                        # .merge(cedis_df, on= 'ubicacionactual', how= 'left')
    
    print(YELLOW + "\nTipos de datos en columnas\n" + RESET)
    # diferentes = final_df["has_hist_ce"].unique().tolist()
    # print(diferentes)
    # dif = final_df["has_cluster_ce"].unique().tolist()
    

    final_df["IS_RAC"] = final_df.apply(_ISRACFunc, axis= 1).astype(int)   # ← nueva función!!!, 
    

    final_df["COBERTURA_CE"] = final_df.apply(_coberturaFunc, axis=1)    

    final_df['cluster'].fillna("SIN_CLUSTER", inplace= True)    

    final_df.rename(columns={'Almacen_key': 'Origen_Express', 'cluster':'Cluster'}, inplace=True)
        
    final_df.drop(columns = ["has_hist_ce", "has_cluster_ce"], inplace= True)
    
    # print("\nLa unión final ha finalizado\n")

    return final_df


def tablasAggregadas(df: pd.DataFrame) -> pd.DataFrame:

    print("\nYa entró a la función\n")
    
    print(RED + f"\n{df.columns}\n" + RESET)

    # agg_cluster_df = df.groupby(['NombreCEDIS', 'Cluster'])['Cluster'].size() \
    #                     .reset_index(name='Count') \
    #                     .rename(columns={'Count': 'Total'})


    # checar de dónde salió la columna Fila....  
    agg_cluster_df = df.pivot_table(values = 'folio', 
                                    index= ['NombreCEDIS', 'Cluster'],
                                    columns= 'COBERTURA_CE',
                                    aggfunc= 'count',
                                    margins= True,
                                    margins_name='Total'
                                    )   

        
    # agg_jaula_df = df.groupby(['NombreCEDIS', 'jaula'])['jaula'].size() \
    #                     .reset_index(name='Count') \
    #                     .rename(columns={'jaula': 'Jaula', 'Count': 'Total' })
    

    agg_jaula_df = df.rename(columns={'jaula': 'Jaula'}) \
                        .pivot_table(values = 'folio', 
                                    index= ['NombreCEDIS', 'Jaula'],
                                    columns= 'COBERTURA_CE',
                                    aggfunc= 'count',
                                    margins= True,
                                    margins_name='Total'
                                    ) \
                                
 

    return agg_cluster_df, agg_jaula_df

def filtroVarios(df, cedis, cluster, fecha = None, jaula = None, rac = None, tipo = None) -> list:
    """Genera las opciones para 4 de los filtros secundarios.
    El df_proc ya tiene los filtros primarios de cedis y fecha
    cedis y cluster son obligatorios, los demás no
    """

    # (3)
    if tipo =='fecha':
        code_fecha = df[
            (df['NombreCEDIS'].isin(cedis)) &
            (df['Cluster'].isin(cluster)) 
           ]["fechaenrutada"].dt.date.unique().tolist()
        
        code_fecha.sort()
        
        return code_fecha
    
    # (4)
    if tipo =='jaula':
        code_jaula = df[
            (df['NombreCEDIS'].isin(cedis)) &
            (df['Cluster'].isin(cluster)) &
            (df['fechaenrutada'].isin(fecha))
           ]["jaula"].unique().tolist()
        
        return code_jaula
    
    # (5)
    if tipo =='rac':
        code_rac = df[
            (df['NombreCEDIS'].isin(cedis)) &
            (df['Cluster'].isin(cluster)) &
            (df['fechaenrutada'].isin(fecha)) &
            (df['jaula'].isin(jaula))
           ]["IS_RAC"].unique().tolist()
        
        return code_rac

    if tipo =='cobertura':
        code_rac = df[
            (df['NombreCEDIS'].isin(cedis)) &
            (df['Cluster'].isin(cluster)) &
            (df['fechaenrutada'].isin(fecha)) &
            (df['jaula'].isin(jaula)) &
            (df['IS_RAC'].isin(rac))
           ]["COBERTURA_CE"].unique().tolist()
        
        return code_rac
    
    return

def segundoFiltrado(df, cedis, cluster, fecha, jaula, rac, cobertura)  -> pd.DataFrame :
    """Genera una string ad hoc para hacer el segundo filtrado y genera un nuevo dataframe evaluando esa string, porque debía de tener alguna forma de poder manejar cuando es sólo 1 opción o cuando son todas las opciones...
    El df_proc ya tiene los filtros primarios de cedis y fecha""" 

    # print(CYAN + "\n\nEstos son las variables para el filtrado de las tablas Agregadas:")
    # print("cedis: ",  cedis)
    # print("cluster: ", cluster)
    # print("fechas: ", fecha)
    # print("jaula: ", jaula)
    # print("rac: ", rac)
    # print("cobertura: ", cobertura)
    # print(RESET)


    # si paso variables en blanco, entonces que automaticamente sean igual a todas las opciones
    if cedis == []:
        cedis = df['NombreCEDIS'].unique().tolist()

    if cluster == []:
        cluster = df['Cluster'].unique().tolist()

    if fecha == []:
        fecha = df['fechaenrutada'].unique().tolist()

    if jaula == []:
        jaula = df['jaula'].unique().tolist()

    if rac == []:
        rac = df['IS_RAC'].unique().tolist()

    if cobertura == []:
        cobertura = df['COBERTURA_CE'].unique().tolist()
   
    df_filtrado = df[
                    (df["NombreCEDIS"].isin(cedis)) &
                    (df['Cluster'].isin(cluster)) &
                    (df['fechaenrutada'].isin(fecha)) &
                    (df['jaula'].isin(jaula)) &
                    (df['IS_RAC'].isin(rac)) &
                    (df['COBERTURA_CE'].isin(cobertura))
                ]   

    return df_filtrado
    
def saveResultMem(df: pd.DataFrame):
    """Guarda el resultado en memoria para facilitar la descarga de archivos"""

    # Guardar resultado en memoria
    buffer = BytesIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    return buffer



# (8) Escritura final del DataFrame
def _escrituraFinal(final_df):
    
    # fecha = date.today().strftime("%d-%m-%Y")
    fecha = date.today().strftime("%d %b %Y")

    # nombre_final = "HistóricoEntregasMueblesAA - Oct 2025 v3.csv"
    nombre_final = f"HistóricoEntregasMueblesAA - {fecha}.csv"

    # final_df.toPandas().to_csv("..\\Output\\" + nombre_final, index = False)
    final_df.toPandas().to_csv(nombre_final, index = False)


    return

def get_key_from_value(dictio):
    
    # Get all keys that have a value equal to target_value
    keys_as_list = [key for key, value in dictio.items()]

    return keys_as_list


def nombreCEDIS():
    """Esta función abre el archivo encurtido del diccionario que tiene los números de Bodega o CEDIS con sus nombres"""
    with open("cedis_dictio.pkl", 'rb') as file:
            dictio = pickle.load(file)

    return dictio

def _separador(archivo):
    """Sirve para deterctar el separador de columnas de un archivo csv"""  
   
    sample = archivo.getvalue().decode("utf-8")     # forma original, pero lee todo el archivo 👎
    # Wrap bytes in BytesIO for text-like reading
    buffer = io.BytesIO(archivo.getvalue())
    otro = ""
    
    # Display the first few lines    
    for i, line in enumerate(buffer):
        if i < 5:
            otro += line.decode('utf-8').strip()            
        else:
            break

    dialect = csv.Sniffer().sniff(otro)       
    

    return dialect.delimiter

def leerArchivo(uploaded_file):
    """Lee el archivo subido y detecta la extensión del mismo para leerlo con el método adecuado,
    Convierte la columna fechaenrutada en tipo fecha
    Quita las filas que tiene fecha menor al año 2000
    Args:
        uploaded_file: es de tipo: streamlit.runtime.uploaded_file_manager.UploadedFile"""

    if uploaded_file is not None:
        filename = uploaded_file.name
        file_extension = os.path.splitext(filename)[1].lower()    
    
    if file_extension == ".csv":        
        
        df = pd.read_csv(uploaded_file, sep= _separador(uploaded_file))
                
        
    elif file_extension == ".xlsx":
        df = pd.read_excel(uploaded_file)
        print(YELLOW + "\nELIF\n" + RESET)
        
    else:
        st.error("❌ Tipo de archivo inválido. Solo se permiten archivos .csv o .xlsx.")
        st.stop()
        df = None    

    if df is not None:

        formfecha = detectarFormatoFecha(df)
        df['fechaenrutada'] = pd.to_datetime(df['fechaenrutada'], format= formfecha)
        df['fecha'] = pd.to_datetime(df['fecha'], format= formfecha)
        df['ubicacionactual'] = df['ubicacionactual'].astype('string') 
        df['num_centronomina']  =df['num_centronomina'].astype('string')



        df_filtrado =  df[df['fechaenrutada'].dt.year >= 2020 ]


    return df_filtrado


"""este módulo contiene las funciones que habrán de ser usadas en el script principal"""

if __name__ == "__main__":
     # probar aquí las funciones    
    
    archivo ="arenaiztp.xlsx"
    
    # formato = detectarFormatoFecha("Datos_eahistoria.csv")
    # formato = detectarFormatoFecha(archivo)

    # print(f"\n\n\n\n\n{formato}\n\n\n\n\n")

    # df = histMuebles(archivo)

    # unionFinal(df)

    # escrituraFinal(unionFinal(df))

    # pprint(nombreCEDIS())
    cedis_dictio, clusters_df, rutas_df, pesos_codigosM_df, hist_exp_df = _desencurtir()
    print()
    
    
    print(df.head())




