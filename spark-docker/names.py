from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, monotonically_increasing_id

# Crear SparkSession
spark = SparkSession.builder.appName("Ejercicio nombres").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Leer el fichero nombres.json (ajusta la ruta si es necesario)
names = spark.read.json("data/nombres.json")

# Eliminar filas vacías (donde Nombre es null)
names = names.filter(col("Nombre").isNotNull())

# 1. Nueva columna FaltanJubilacion (67 - Edad)
names = names.withColumn("FaltanJubilacion", lit(67) - col("Edad"))

# 2. Nueva columna Apellidos con valor 'XYZ'
names = names.withColumn("Apellidos", lit("XYZ"))

# 3. Eliminar columnas Mayor30 y Apellidos
names = names.drop("Mayor30", "Apellidos")

# 4. Nueva columna AnyoNac (año de nacimiento, usando fecha actual 2025)
names = names.withColumn("AnyoNac", lit(2025) - col("Edad"))

# 5. Añadir id incremental y mostrar columnas en orden solicitado
names = names.withColumn("Id", monotonically_increasing_id())
names = names.select("Id", "Nombre", "Edad", "AnyoNac", "FaltanJubilacion", "Ciudad")

names.show()

spark.stop()
