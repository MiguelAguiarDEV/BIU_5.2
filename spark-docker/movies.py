from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, desc, row_number
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

def main():
    spark = SparkSession.builder.appName("MovieAnalysis").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    base_path = "/opt/spark/data/"

    # Ajusta los nombres de columnas según el contenido real de los archivos
    movies_schema = ["actor", "title", "year"]
    ratings_schema = ["rating", "title", "year"]

    movies_df = spark.read.option("delimiter", "\t").option("header", "false").option("inferSchema", "true").csv(base_path + "movies.tsv").toDF(*movies_schema)
    ratings_df = spark.read.option("delimiter", "\t").option("header", "false").option("inferSchema", "true").csv(base_path + "movie-ratings.tsv").toDF(*ratings_schema)

    print("Schema for movies_df:")
    movies_df.printSchema()
    print("Sample data from movies_df:")
    movies_df.show(5, truncate=False)

    print("Schema for ratings_df:")
    ratings_df.printSchema()
    print("Sample data from ratings_df:")
    ratings_df.show(5, truncate=False)

    # Join por title y year
    join_cols = ["title", "year"]
    joined_df = movies_df.join(ratings_df, join_cols, "inner")
    print("Schema for joined_df:")
    joined_df.printSchema()
    print("Sample data from joined_df after join:")
    joined_df.show(5, truncate=False)

    # Persistencia
    joined_df.persist(StorageLevel.MEMORY_AND_DISK)
    count = joined_df.count()
    print(f"El DataFrame unido tiene {count} filas.")
    print("El DataFrame 'joined_df' ha sido persistido. Por favor, comprueba la Spark UI (pestaña 'Storage') para ver su tamaño.")
    print("Puedes acceder a la Spark UI normalmente en http://localhost:4040 si ejecutas Spark localmente.")

    # Procesamiento adicional
    processed_df = joined_df.withColumn("year", col("year").cast("integer")) \
                             .withColumn("rating", col("rating").cast("float"))
    processed_df = processed_df.filter(col("year").isNotNull() & col("rating").isNotNull())
    processed_df = processed_df.filter(col("actor").isNotNull() & (col("actor") != ""))

    print("Schema for processed_df (after type casting and filtering):")
    processed_df.printSchema()
    print("Sample data from processed_df:")
    processed_df.show(5, truncate=False)

    # 4. Película con mayor puntuación por año
    window_spec_year_rating = Window.partitionBy("year").orderBy(desc("rating"))
    top_movie_per_year_df = processed_df.withColumn("rank", row_number().over(window_spec_year_rating)) \
                                      .filter(col("rank") == 1) \
                                      .select("year", col("title").alias("titulo_pelicula"), "rating", "actor")
    print("--- Película con mayor puntuación por año ---")
    top_movie_per_year_df.show(truncate=False)

    # 5. Lista de intérpretes (en este caso, solo hay un actor por fila)
    top_movie_per_year_with_cast_list_df = top_movie_per_year_df.withColumn("lista_interpretes", split(col("actor"), ","))
    print("--- Película con mayor puntuación por año (con lista de intérpretes) ---")
    top_movie_per_year_with_cast_list_df.select("year", "titulo_pelicula", "rating", "lista_interpretes").show(truncate=False)

    # 6. Tres parejas de intérpretes que más han trabajado juntos
    # Si hay más de un actor por película, separa por coma. Si no, esto dará vacío.
    movies_with_actors_df = processed_df.withColumn("interprete", explode(split(col("actor"), ",")))
    movies_with_actors_df = movies_with_actors_df.select("title", "year", "interprete")
    df1 = movies_with_actors_df.alias("df1")
    df2 = movies_with_actors_df.alias("df2")
    actor_pairs_df = df1.join(df2, (col("df1.title") == col("df2.title")) & (col("df1.year") == col("df2.year"))) \
                        .where(col("df1.interprete") < col("df2.interprete")) \
                        .groupBy(col("df1.interprete").alias("interprete1"), col("df2.interprete").alias("interprete2")) \
                        .count() \
                        .orderBy(desc("count")) \
                        .withColumnRenamed("count", "cantidad")
    print("--- Tres parejas de intérpretes que más han trabajado juntos ---")
    actor_pairs_df.show(3, truncate=False)

    # Limpiar la persistencia
    joined_df.unpersist()
    spark.stop()

if __name__ == "__main__":
    main()
