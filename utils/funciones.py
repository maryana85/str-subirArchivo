import pandas as pd
from datetime import date
import os
import glob
from pyspark.sql import SparkSession, DataFrame
from pprint import pprint
from pyspark.sql.functions import col, to_date, length, concat, lit, when, count,  concat_ws, row_number
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
import pickle


spark = (
    SparkSession.builder
    .appName("CoppelExpress_Historico_clean")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.driver.memory", "10g")
    .config("spark.driver.maxResultSize", "2g")
    .config("spark.sql.shuffle.partitions", "64")
    .getOrCreate()
)

def detectarFormatoFecha(nombreArchivo: str) -> str:

    # filename = uploaded_file.name
    file_extension = os.path.splitext(nombreArchivo)[1].lower()

    print("\n\n\n", file_extension, "\n\n\n")

    # Para leer el dataframe:
    if file_extension == ".csv":   
        df = spark.read.csv(nombreArchivo,  header=True)    
    
    elif file_extension == ".xlsx":
        df = spark.createDataFrame( pd.read_excel(nombreArchivo , engine="openpyxl", dtype=str) )
    
    # para saber el formato de fecha
    try:
        df.select(to_date(col('fecha'), "yyyy-MM-dd"))
        formato =  "yyyy-MM-dd"

    except:
        try:
            df.select(to_date(col('fecha'), "dd-MM-yyy"))
            formato =  "dd-MM-yyy"
        except:
            print("Ninguna coincidencia")

    print("\n\n\n\n", formato,"\n\n\n\n")

    return formato


def desencurtir() -> tuple:
    """Desencurte los archivos pkl que contienen los dataframes que están en pandas y los transforma un un dataframe de pyspark"""

    archivos = [
                'pesos_codigosM_df.pkl'
                ]
    
    listaFinal = []

    for a in archivos:
        with open(a, 'rb') as file:
            data = pickle.load(file)
        # spark.createDataFrame(data)
        listaFinal.append(spark.createDataFrame(data))

    return tuple(listaFinal)

# (6) Cargar el Catálogo de los archivos de Histórico Muebles (Entregas) → este es el que se va a procesar!!!
def histMuebles(nombreArchivo: str, formatoFecha: str) -> DataFrame:
    """Cargar el Catálogo de los archivos de Histórico Muebles (Entregas) → este es el que se va a procesar!!!"""
    # his24 = "C:\\Users\\marya\\Documentos\\Arena\\Emmanuel\\AnalisisCoppel-main\\Catálogos\\8.1_historico_entregas_2024_muebles.xlsx"
    
    rootDir = r"C:\Users\marya\Documentos\Arena\Emmanuel\AnalisisCoppel-main\Entregas Muebles\\"
    # name = "arenaiztp.xlsx"
    # name = "marioiztp.xlsx"
    name = "Datos_eahistoria.csv"

    file_extension = os.path.splitext(nombreArchivo)[1].lower()

    if file_extension == ".csv":
        # pre_df_ent = spark.read.csv(rootDir + name , header=True)
        pre_df_ent = spark.read.csv(nombreArchivo,  header=True)
    elif file_extension == ".xlsx":        
        pre_df_ent = spark.createDataFrame( pd.read_excel(nombreArchivo , engine="openpyxl", dtype=str) ) 
    

    tipo_keep = ["VB", "VS"]
    ubicacion_keep = "30011"
    # "yyyy-MM-dd"

    entregas_df = pre_df_ent.withColumn("Fecha_New",  to_date(col('fecha'), formatoFecha)) \
            .drop('num_empleadochofer', 'num_empleadoayudante', 'num_centronomina', 'MUEBLES - LIMPIEZA - SOLO IZP', 'num_subcausa', 'flagcelactivado', 'num_minutoscodigo', 'num_minutosruta', 'num_equivalencia', 'num_servicioxcontrol', 'clv_mesaregalo', 'tur', 'foliofide', 'flagonline', 'flagcontrolnuevo', 'motivadopor', 'flagta' ) \
            .filter(col("tipo").isin(*tipo_keep)) \
            .filter(col('ubicacionactual') == ubicacion_keep) \
            .withColumn("Fecha_en_Ruta_New",  to_date(col('fechaenrutada'), formatoFecha)) \
            .withColumn("IS_RAC", when(col("jaula").contains("R"), lit(1)).otherwise(lit(0))) \
            .withColumn("ID_RUTA", concat_ws("-", col("ciudad"), col("ruta"),  col("jaula"))) \
            .withColumnRenamed("tipo", "Tipo_Art")

    return entregas_df      # pyspark Dataframe

# (7) Unión de los 5 dataframes para poder asignarles un clúster
def unionFinal(entregas_df) -> DataFrame:
    """los catálogos se jalan usando la función de desencurtir"""
    # lista_CPs = entregas_df.select('Código_postal').rdd.flatMap(lambda x: x).collect()

    pesos_codigosM_df = desencurtir()

    print("\n",type(pesos_codigosM_df), "\n")
    
    
    final_df = entregas_df.join(pesos_codigosM_df, ["codigo"], "left") \
                        # .join(rutas_df, ["zona"], "left") \
                        # .join(clusters_df, ["Código_postal"], "left").fillna("N/A", subset=["Cluster"]) \
                        # .join(hist_exp_df, ["Código_postal"], "left") \
                        # .withColumn("COBERTURA_CE",
                            # when(col("IS_RAC") == 0, lit("NORAC"))
                            # .when((col("has_hist_ce").isNotNull()) | (col("has_cluster_ce").isNotNull()), lit("CON_COBERTURA"))
                            # .otherwise(lit("SIN_COBERTURA"))
                        # ) \
                        # .select("ID_RUTA", "Código_postal", "folio", "fecha", "Seccion", "Tipo_Art", "Cluster", "COBERTURA_CE", "IS_RAC")

    print("\nLa unión final ha finalizado\n")
    return final_df

# (8) Escritura final del DataFrame
def escrituraFinal(final_df):
    
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


