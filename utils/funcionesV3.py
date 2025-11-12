import pandas as pd
from pandas import DataFrame
from datetime import date
import os
import glob
from pprint import pprint
import pickle
from io import StringIO



"""Esta versión es sin SPARK"""

def detectarFormatoFecha(df) -> str:
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

    print("\n\n\n\n", formato,"\n\n\n\n")

    return formato

def _isRACFun(row):
    """función para el apply en un dataframe"""
    try:
        if row['jaula'].str.contains('R'):
            res =  1

    except:         
        res = 0

    return


def _desencurtir() -> tuple:
    """Desencurte los archivos pkl que contienen los dataframes que están en pandas y los transforma un un dataframe de pyspark"""    
    
    archivos = [
                'pesos_codigosM_df.pkl', 
               ]
    
    listaFinal = []

    for a in archivos:
        with open(a, 'rb') as file:
            data = pickle.load(file)
        
        listaFinal.append(data)

    return tuple(listaFinal)

# (6) Cargar el Catálogo de los archivos de Histórico Muebles (Entregas) → este es el que se va a procesar!!!
def histMuebles(df, formatoFecha: str) -> DataFrame:
    """Cargar el Catálogo de los archivos de Histórico Muebles (Entregas) → este es el que se va a procesar!!!"""  

    columnasMantener = ["fecha","tipo", "ubicacionactual", "fechaenrutada", "jaula", "ciudad", "ruta", "zona", "tienda", "folio", "codigo", "articulo", "marca", "modelo", "cantidad", "mododeentrega", "cliente", 'Fecha_New', 'Fecha_en_Ruta_New', 'IS_RAC', 'ID_RUTA']
        
    pre_df_ent = df

    tipo_keep = ["VB", "VS"]
    ubicacion_keep = "30011"
    # "yyyy-MM-dd"

    pre_df_ent['codigo'] = pre_df_ent['codigo'].astype(str)
    pre_df_ent['Fecha_New'] = pd.to_datetime(df['fecha'], format= formatoFecha)
    pre_df_ent['Fecha_en_Ruta_New'] = pd.to_datetime(df['fechaenrutada'], format= formatoFecha)
    pre_df_ent.rename(columns={'Tipo_Art': 'tipo'}, inplace=True)    
    pre_df_ent['IS_RAC'] = pre_df_ent.apply(_isRACFun, axis=1)
    pre_df_ent['ID_RUTA'] = pre_df_ent['ciudad'].astype(str) + '-' + pre_df_ent['ruta'].astype(str) + '-' + pre_df_ent['jaula'].astype(str)

    todas = pre_df_ent.columns.tolist()
    colsTirar = [i for i in todas if i not in columnasMantener]

    pre_df_ent.drop(colsTirar, axis=1, inplace=True)

    entregas_df = pre_df_ent[
                            (pre_df_ent['tipo'].isin(tipo_keep))  &
                            (pre_df_ent['ubicacionactual'] == ubicacion_keep)
                            ]

    entregas_df = pre_df_ent

    return  entregas_df      # Dataframe

# (7) Unión de los 5 dataframes para poder asignarles un clúster
def unionFinal(df, formatoFecha) -> DataFrame:
    """los catálogos se jalan usando la función de desencurtir"""
    # lista_CPs = entregas_df.select('Código_postal').rdd.flatMap(lambda x: x).collect()

    entregas_df = histMuebles(df, formatoFecha)       # 1
    print("\n\nYA PROCESO ENTREGAS MUEBLES!!!!\n\n")

    algo = list(set(entregas_df['codigo'].str.len().tolist()))
    print("Lista de longitudes en entregas: ", algo)
    

    pesos_codigosM_df = _desencurtir()[0]  # 2

    otro = list(set(pesos_codigosM_df['codigo'].str.len().tolist()))
    print("Lista de longitudes en el otro: ", otro)

        
    # final_df = pd.merge(entregas_df, pesos_codigosM_df, on='codigo', how='left')
    final_df = entregas_df.merge(pesos_codigosM_df, on='codigo', how='left')
    
    print("\nLa unión final ha finalizado\n")
    return final_df

# (8) Escritura final del DataFrame
def _escrituraFinal(final_df):
    
    # fecha = date.today().strftime("%d-%m-%Y")
    fecha = date.today().strftime("%d %b %Y")

    # nombre_final = "HistóricoEntregasMueblesAA - Oct 2025 v3.csv"
    nombre_final = f"HistóricoEntregasMueblesAA - {fecha}.csv"

    # final_df.toPandas().to_csv("..\\Output\\" + nombre_final, index = False)
    final_df.toPandas().to_csv(nombre_final, index = False)


    return


"""este módulo contiene las funciones que habrán de ser usadas en el script principal"""

if __name__ == "__main__":
     # probar aquí las funciones    
    
    archivo ="arenaiztp.xlsx"
    
    # formato = detectarFormatoFecha("Datos_eahistoria.csv")
    formato = detectarFormatoFecha(archivo)

    print(f"\n\n\n\n\n{formato}\n\n\n\n\n")

    df = histMuebles(archivo, formato)

    # pprint(df.columns)

    # unionFinal(df)

    # escrituraFinal(unionFinal(df))

