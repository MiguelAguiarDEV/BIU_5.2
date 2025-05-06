from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Inicializar Spark
spark = SparkSession.builder \
    .appName("Ejemplo UDF") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# DataFrame de ejemplo
datos = [(1, "Ana"), (2, "Luis"), (3, "Marta"), (4, None)]
df = spark.createDataFrame(datos, ["id", "nombre"])

# 1. Definir una función Python
# Esta función invierte el string recibido
def invertir_cadena(s):
    return s[::-1] if s else s

# 2. Registrar la UDF para uso en DataFrame
invertir_udf = udf(invertir_cadena, StringType())

df_udf = df.withColumn("nombre_invertido", invertir_udf(df["nombre"]))
print("--- Uso de UDF en DataFrame ---")
df_udf.show()

# 3. Registrar la UDF para Spark SQL
spark.udf.register("invertir_cadena", invertir_cadena, StringType())
df.createOrReplaceTempView("personas")

print("--- Uso de UDF en Spark SQL ---")
spark.sql("SELECT id, nombre, invertir_cadena(nombre) as nombre_invertido FROM personas").show()

# Parar Spark
spark.stop()
