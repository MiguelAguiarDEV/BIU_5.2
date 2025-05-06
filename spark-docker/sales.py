# Este archivo contiene funciones para manejar datos de ventas con valores nulos.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean, min, round as pyspark_round, lit

# Crear SparkSession
spark = SparkSession.builder.appName("Ejercicio Sales Nulos").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Leer el fichero VentasNulos.csv (sales)
sales = spark.read.option("header", True).option("inferSchema", True).csv("data/VentasNulos.csv")

# Eliminar filas con al menos 4 nulos
cols = ["Nombre", "Ventas", "Euros", "Ciudad", "Identificador"]
sales = sales.where(
    sum([col(c).isNull().cast("int") for c in cols]) < 4
)

# Sustituir los nombres nulos por 'Empleado'
sales = sales.withColumn("Nombre", when(col("Nombre").isNull(), "Empleado").otherwise(col("Nombre")))

# Sustituir las ventas nulas por la media de las ventas de los compañeros (redondeado a entero)
avg_ventas = sales.select(mean(col("Ventas")).alias("avg_ventas")).collect()[0][0]
sales = sales.withColumn(
    "Ventas",
    when(col("Ventas").isNull(), pyspark_round(lit(avg_ventas)).cast("int")).otherwise(col("Ventas"))
)

# Sustituir los euros nulos por el valor mínimo de euros de los compañeros
min_euros = sales.select(min(col("Euros")).alias("min_euros")).collect()[0][0]
sales = sales.withColumn(
    "Euros",
    when(col("Euros").isNull(), min_euros).otherwise(col("Euros"))
)

# Sustituir la ciudad nula por 'C.V.'
sales = sales.withColumn("Ciudad", when(col("Ciudad").isNull(), "C.V.").otherwise(col("Ciudad")))

# Sustituir el identificador nulo por 'XYZ'
sales = sales.withColumn("Identificador", when(col("Identificador").isNull(), "XYZ").otherwise(col("Identificador")))

sales.show()

spark.stop()