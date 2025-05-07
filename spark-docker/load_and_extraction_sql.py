from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import matplotlib.pyplot as plt
import pandas as pd
import os

def main():
    # Inicializar Spark
    spark = SparkSession.builder.appName("SalesAnalysisSQL").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Definir rutas absolutas para el entorno Docker
    BASE_PATH = "/opt/spark/data"
    SALES_DATA_PATH = f"{BASE_PATH}/salesdata"
    SALES_OUTPUT_PATH = f"{BASE_PATH}/salesoutput"

    # Leer archivo de muestra para mostrar estructura
    sample_file = f"{SALES_DATA_PATH}/Sales_January_2019.csv"
    df_sample = spark.read.option("header", True).option("inferSchema", True).csv(sample_file)
    print(f"Muestra del archivo {sample_file}:")
    df_sample.show(5, truncate=False)
    print("\nEsquema del archivo:")
    df_sample.printSchema()

    # Revisar fechas
    print("\nEjemplos de fechas en el archivo:")
    df_sample.select("Order Date").distinct().limit(10).show(truncate=False)

    # Leer todos los archivos CSV
    all_files = [f"{SALES_DATA_PATH}/{file}" for file in os.listdir(SALES_DATA_PATH) if file.endswith(".csv")]
    print(f"\nLeyendo {len(all_files)} archivos CSV...")
    df = spark.read.option("header", True).option("inferSchema", True).csv(all_files)
    print(f"Total de registros: {df.count()}")

    # Renombrar columnas
    column_mapping = {col: col.strip().replace(" ", "") for col in df.columns}
    for old_name, new_name in column_mapping.items():
        df = df.withColumnRenamed(old_name, new_name)
    print("\nColumnas después de renombrar:")
    print(df.columns)

    # Limpiar datos
    print(f"\nFilas antes de limpieza: {df.count()}")
    df = df.filter(col("OrderID").isNotNull())
    columnas_criticas = ["OrderID", "Product", "QuantityOrdered", "PriceEach", "OrderDate", "PurchaseAddress"]
    df = df.dropna(subset=columnas_criticas)
    print(f"Filas después de limpieza: {df.count()}")
    df.show(5)

    # Convertir fecha
    df = df.withColumn("OrderDate", to_timestamp(col("OrderDate"), "MM/dd/yy HH:mm"))
    df = df.withColumn("Year", year(col("OrderDate")))
    df = df.withColumn("Month", month(col("OrderDate")))

    # Extraer ciudad y estado correctamente
    df = df.withColumn("City", trim(split(col("PurchaseAddress"), ",")[1]))
    df = df.withColumn("State", split(trim(split(col("PurchaseAddress"), ",")[2]), " ")[0])

    # Convertir tipos
    df = df.withColumn("QuantityOrdered", col("QuantityOrdered").cast("integer"))
    df = df.withColumn("PriceEach", col("PriceEach").cast("double"))

    # Guardar como Parquet
    print("\nGuardando datos particionados...")
    df.write.partitionBy("Year", "Month").mode("overwrite").parquet(SALES_OUTPUT_PATH)

    # Verificar particiones
    if os.path.exists(SALES_OUTPUT_PATH):
        print("\nEstructura de directorios creada:")
        for item in os.listdir(SALES_OUTPUT_PATH):
            if item.startswith("Year="):
                print(f"- {item}")
                year_dir = os.path.join(SALES_OUTPUT_PATH, item)
                subfolders = os.listdir(year_dir)
                print(f"  Contiene {len(subfolders)} subdirectorios")

    # Leer datos del año 2019
    year_path = f"{SALES_OUTPUT_PATH}/Year=2019"
    if os.path.exists(year_path):
        print("\nLeyendo datos del año 2019...")
        df2019 = spark.read.parquet(year_path)
        df2019.createOrReplaceTempView("ventas2019")

        # Mostrar estados detectados
        print("\nEstados detectados en los datos de ventas2019:")
        spark.sql("SELECT DISTINCT State FROM ventas2019").show(100, truncate=False)

        # Ventas por mes
        ventas_mes = spark.sql("""
            SELECT Month, SUM(QuantityOrdered * PriceEach) AS Total
            FROM ventas2019
            GROUP BY Month
            ORDER BY Month
        """)
        pd_mes = ventas_mes.toPandas()
        plt.figure(figsize=(10, 6))
        plt.bar(pd_mes["Month"], pd_mes["Total"], color=plt.cm.tab20(range(len(pd_mes))))
        plt.title("Ventas totales por mes")
        plt.xlabel("Mes")
        plt.ylabel("Ventas Totales")
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        plt.tight_layout()
        plt.savefig("ventas_por_mes.png")
        print("Gráfico guardado como 'ventas_por_mes.png'")

        # Top 10 ciudades
        top_ciudades = spark.sql("""
            SELECT City, SUM(QuantityOrdered) AS Unidades
            FROM ventas2019
            GROUP BY City
            ORDER BY Unidades DESC
            LIMIT 10
        """)
        pd_ciudades = top_ciudades.toPandas()
        pd_ciudades.plot(kind="bar", x="City", y="Unidades", legend=False, figsize=(10, 6))
        plt.title("Top 10 Ciudades por Unidades Vendidas")
        plt.xlabel("Ciudad")
        plt.ylabel("Unidades Vendidas")
        plt.tight_layout()
        plt.savefig("top_ciudades.png")
        print("Gráfico guardado como 'top_ciudades.png'")

        # Pedidos por hora
        df2019 = df2019.withColumn("Hora", hour("OrderDate"))
        df2019.createOrReplaceTempView("ventas_con_hora")
        horas = spark.sql("""
            SELECT Hora, COUNT(DISTINCT OrderID) AS TotalPedidos
            FROM (
                SELECT OrderID, Hora
                FROM ventas_con_hora
                GROUP BY OrderID, Hora
                HAVING COUNT(*) >= 2
            ) pedidos
            GROUP BY Hora
            ORDER BY Hora
        """)
        pd_horas = horas.toPandas()
        plt.figure(figsize=(10, 6))
        plt.bar(pd_horas["Hora"], pd_horas["TotalPedidos"])
        plt.title("Pedidos por Hora (al menos 2 productos)")
        plt.xlabel("Hora")
        plt.ylabel("Pedidos")
        plt.tight_layout()
        plt.savefig("pedidos_por_hora.png")
        print("Gráfico guardado como 'pedidos_por_hora.png'")

        # Productos comprados juntos en NY
        ny = spark.sql("""
        SELECT OrderID, collect_set(Product) AS Productos
        FROM ventas2019
        WHERE State = 'NY' AND Product IS NOT NULL
        GROUP BY OrderID
        HAVING size(Productos) > 1
        """)
        print("Productos comprados juntos en NY:")
        ny.show(truncate=False)
    else:
        print(f"ERROR: No se encontró el directorio {year_path}")

    spark.stop()

if __name__ == "__main__":
    main()
