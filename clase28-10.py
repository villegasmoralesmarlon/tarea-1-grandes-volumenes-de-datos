#importar librerias necesarias
# 1. Importar las librerías necesarias de PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, when, date_format, year, month,to_date,min,hour,to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType


#  Crear una sesión de Spark (punto de entrada a Spark)
spark = SparkSession.builder \
    .appName("AnalisisTransacciones") \
    .getOrCreate()

#  Leer el archivo CSV
# Suponemos que el archivo tiene encabezado y el esquema se infiere automáticamente
df = spark.read.option("header", "true") \
               .option("inferSchema", "true") \
               .csv("transacciones_simuladas.csv")

# Contar transacciones por ciudad
print(" Número de transacciones por ciudad:")
df.groupBy("ciudad") \
  .agg(count("*").alias("num_transacciones")) \
  .orderBy(col("num_transacciones").desc()) \
  .show()

#muestra los datos nulos
df.select([sum(when(col(c).isNull(), 1).otherwise(0)).alias(c + "_nulos") for c in df.columns]).show()

# Convertir la columna 'fecha' desde formato 'dd/MM/yyyy'
df = df.withColumn("fecha", to_date(col("fecha"), "dd/MM/yyyy"))

# Ordenar por fecha descendente
df_ordenado = df.orderBy(col("fecha").desc())

df_ordenado.show()

# Calcular el monto mínimo
monto_minimo = df.select(min(col("monto")).alias("monto_minimo")).collect()[0]["monto_minimo"]

# Filtrar las operaciones con ese monto
operaciones_sospechosas = df.filter(col("monto") == monto_minimo)

operaciones_sospechosas.show()

# Filtrar retiros mayores a 40000
retiros_mayores = df.filter((col("tipo_transaccion") == "retiro") & (col("monto") > 40000))
retiros_mayores.show()

#muestra sumatoria de los retiros coloca el alias total_retiros
total_retiros = df.filter(col("tipo_transaccion") == "retiro") \
                  .agg(sum(col("monto")).alias("total_retiros"))

total_retiros.show()

#agrega la columna hora
df = df.withColumn("hora", hour(to_timestamp(col("fecha"), "dd/MM/yyyy HH:mm:ss")))
df.show()


#muestra todas las operaciones realizadas en el extranjero como sospechosas
operaciones_extranjero = df.filter(col("categoria_comercio") == "extranjero")
operaciones_extranjero.show(5)



