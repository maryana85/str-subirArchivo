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

    columnasMantener = ["fecha","tipo", "ubicacionactual", "fechaenrutada", "jaula", "ruta", "zona",  "folio", "codigo",'Fecha_New', 'Fecha_en_Ruta_New', 'IS_RAC', 'ID_RUTA', "num_centronomina", "IS_RAC_Izt", "Cluster"] # "articulo", "marca", "modelo", "cantidad",  "mododeentrega", "cliente", "ciudad", "tienda"
        
    pre_df_ent = df

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

        # print(PINK + f"\n\nUbicaciones Seleccionadas: {selected_ubicaciones}\n\n" + RESET)
        # print(PINK, lista_codigos_ubi,  RESET)

        pre_df_ent = pre_df_ent[
            pre_df_ent["ubicacionactual"].isin(lista_codigos_ubi)
            ]

    # (3) - Filtro de tipo
    tipo_keep = ["VB", "VS"]
    pre_df_ent = pre_df_ent[
                    (pre_df_ent['tipo'].isin(tipo_keep))                            
                ]
    
    # "yyyy-MM-dd"

    pre_df_ent['codigo'] = pre_df_ent['codigo'].astype(str)
    # pre_df_ent['Fecha_New'] = pd.to_datetime(df['fecha'], format= formatoFecha)
    # pre_df_ent['Fecha_en_Ruta_New'] = pd.to_datetime(df['fechaenrutada'], format= formatoFecha)
    pre_df_ent['Fecha_New'] = df['fecha']
    pre_df_ent['Fecha_en_Ruta_New'] = df['fechaenrutada']
    pre_df_ent.rename(columns={'Tipo_Art': 'tipo'}, inplace=True)    
    pre_df_ent['jaula'].astype(str).str.contains("R").astype(int)      
    # pre_df_ent['IS_RAC_Izt'] = np.where(
    #                 (pre_df_ent['ubicacionactual'] == '30011') &
    #                 (pre_df_ent['jaula'].str.contains('R', na=False)),
    #                 1,
    #                 None  # o 0 para valores numéricos
    #             )
    
    pre_df_ent['IS_RAC_Izt'] = pre_df_ent.apply(
                                lambda row: 1 if (row['ubicacionactual'] == '30011' and 
                                                'R' in str(row['jaula'])) else 0,
                                axis=1
                            )    

    
    pre_df_ent['zona'] = pre_df_ent['zona'].astype(str).str.zfill(width= 7)    
    pre_df_ent['ID_RUTA'] = pre_df_ent['ciudad'].astype(str) + '-' + pre_df_ent['ruta'].astype(str) + '-' +  pre_df_ent['jaula'].astype(str)

    # Eliminar columnas innecesarias
    todas = pre_df_ent.columns.tolist()
    colsTirar = [i for i in todas if i not in columnasMantener]

    pre_df_ent.drop(colsTirar, axis=1, inplace=True)

    entregas_df = pre_df_ent
    
    print(f"\nLargo de pre_df_ent = {len(pre_df_ent)}\n")    

    return  entregas_df      # Dataframe

def _coberturaFunc(row):       
    if row["IS_RAC"] == 0:
        return "NORAC"
    else:
        if (row["has_hist_ce"] is not None) or (row['has_cluster_ce'] is not None):
            return "CON_COBERTURA"            
        else: 
            return "SIN_COBERTURA"            
  
def _ISRACFunc(row):

    if row["IS_RAC_Izt"] is not None:        
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
    print("\n\nYA PROCESÓ ENTREGAS MUEBLES!!!!\n\n")
    
    cnomina_df, cedis_dictio, clusters_df, rutas_df, pesos_codigosM_df, hist_cps_norm = _desencurtir()
    
    cedis_df = pd.DataFrame(list(cedis_dictio.items()), columns=  ['ubicacionactual', 'NombreCEDIS'])

    # rutas_df['zona'] = rutas_df['zona'].astype(str).str.zfill(width= 7)

        
    # final_df = pd.merge(entregas_df, pesos_codigosM_df, on='codigo', how='left')
    final_df = entregas_df.merge(cnomina_df, on= "num_centronomina", how= 'left') \
                        .merge(cedis_df, on= 'ubicacionactual', how= 'left') \
                        .merge(pesos_codigosM_df, on='codigo', how='left') \
                        .merge(rutas_df, on="zona", how="left") \
                        .merge(clusters_df, on= "Código_postal", how = 'left') \
                        .merge(hist_cps_norm, on = "Código_postal", how= "left") \
                        
                        # .merge(cedis_df, on= 'ubicacionactual', how= 'left')
    
    
    final_df["IS_RAC"] = final_df.apply(_ISRACFunc, axis= 1)    # ← nueva función!!!, el pedo está aquí

    final_df["COBERTURA_CE"] = final_df.apply(_coberturaFunc, axis=1)    

    final_df['cluster'].fillna("SIN_CLUSTER", inplace= True)    

    final_df.rename(columns={'Almacen_key': 'Origen_Express', 'cluster':'Cluster'}, inplace=True)
        
    final_df.drop(columns = ["has_hist_ce", "has_cluster_ce", "IS_RAC_Izt"], inplace= True)
    
    print("\nLa unión final ha finalizado\n")

    return final_df

def resumenClusters(df: pd.DataFrame) -> pd.DataFrame:

    print("\nYa entró a la función\n")

    print(df.columns)

    # agg_df = df.groupby('Cluster').sum()

    agg_df = df.groupby('Cluster').agg({'Cluster': 'count'})


    return agg_df


def pivoteVal(df, fecha = None):

    if fecha is not None:
        filter_df = df[ ( df['IS_RAC'] == 1 )  &  
                       (df['COBERTURA_CE'] == "CON_COBERTURA") &
                       (df['fechaenrutada'] == fecha)
                       ]

    else:
        filter_df = df[ ( df['IS_RAC'] == 1 )  &  (df['COBERTURA_CE'] == "CON_COBERTURA") ]

        # num_rows = len(filter_df)
        # filter_df["Cluster"]
    
    pivote_df = filter_df.groupby('Cluster')['folio'].count()   


    return pivote_df

def pivoteVal_2(df, fecha = None):

    if fecha is not None:
        filter_df = df[ 
                        ( df['IS_RAC'] == 1 ) &
                        (df['fechaenrutada'] == fecha)
                       ]

    else:
        filter_df = df[ ( df['IS_RAC'] == 1 ) ]
        # num_rows = len(filter_df)
        # filter_df["Cluster"]


    pivote_df = filter_df.groupby('Cluster')['folio'].count()    

    return pivote_df

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

    # pprint(df.columns)

    # unionFinal(df)

    # escrituraFinal(unionFinal(df))

    # pprint(nombreCEDIS())
    cedis_dictio, clusters_df, rutas_df, pesos_codigosM_df, hist_exp_df = _desencurtir()
    print()
    
    
    print(df.head())




