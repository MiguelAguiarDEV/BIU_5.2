from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, trim, split, regexp_extract, to_timestamp, month, year
import os

def main():
    spark = SparkSession.builder.appName("TechSales").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    base_path = "/opt/spark/data/salesdata/"

    # 1. Lectura infiriendo el esquema
    df_infer = spark.read.option("header", "true").option("inferSchema", "true").csv(base_path + "*.csv")
    print("Schema inferido:")
    df_infer.printSchema()
    df_infer.show(5, truncate=False)

    # 2. Lectura con esquema explícito
    schema = StructType([
        StructField("Order ID", StringType(), True),
        StructField("Product", StringType(), True),
        StructField("Quantity Ordered", IntegerType(), True),
        StructField("Price Each", DoubleType(), True),
        StructField("Order Date", StringType(), True),
        StructField("Purchase Address", StringType(), True)
    ])
    df = spark.read.option("header", "true").schema(schema).csv(base_path + "*.csv")

    # 3. Renombrar columnas para quitar espacios
    for c in df.columns:
        df = df.withColumnRenamed(c, c.replace(" ", ""))

    # 4. Eliminar filas con algún campo nulo
    df = df.dropna()

    # 5. Eliminar filas que sean cabeceras repetidas
    df = df.filter(df.Product != "Product")

    # 6. Extraer City y State de la dirección (regex final)
    df = df.withColumn("City", regexp_extract(col("PurchaseAddress"), r",\s([^,]+),\s([A-Z]{2}) \d{5}$", 1))
    df = df.withColumn("State", regexp_extract(col("PurchaseAddress"), r",\s([^,]+),\s([A-Z]{2}) \d{5}$", 2))

    # 7. Convertir OrderDate a timestamp
    df = df.withColumn("OrderDate", to_timestamp(col("OrderDate"), "MM/dd/yy HH:mm"))

    # 8. Crear columnas Month y Year
    df = df.withColumn("Month", month(col("OrderDate")))
    df = df.withColumn("Year", year(col("OrderDate")))

    print("Schema final y muestra de datos limpios:")
    df.printSchema()
    df.show(5, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
