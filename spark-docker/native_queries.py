from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, desc, row_number, max as spark_max, collect_set, collect_list, explode, countDistinct, date_add, current_date, lit, array_contains
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff
from pyspark.sql.functions import count as spark_count
from pyspark.sql.functions import to_date

# Inicializa SparkSession
spark = SparkSession.builder.appName("Native Spark Queries").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
# Conexión JDBC
jdbc_url = "jdbc:mysql://mysql-moviebind:3306/moviebind"
props = {"user": "user", "password": "1234", "driver": "com.mysql.cj.jdbc.Driver"}

tables = [
    "users", "profiles", "contract_types", "contracts", "movies", "actors", "movie_actors",
    "genres", "movie_genres", "keywords", "movie_keywords", "views", "ratings"
]
df = {t: spark.read.jdbc(url=jdbc_url, table=t, properties=props) for t in tables}

# 1. Protagonistas de películas que se enmarcan, al menos, en los géneros ‘comedy’ y ‘drama’ al mismo tiempo.
comedy_drama_movies = df["movie_genres"].filter(col("genre_name").isin(["Comedy", "Drama"])) \
    .groupBy("movie_title", "movie_director") \
    .agg(collect_set("genre_name").alias("genres")) \
    .filter(array_contains(col("genres"), "Comedy") & array_contains(col("genres"), "Drama"))
protagonistas = comedy_drama_movies \
    .join(df["movie_actors"], ["movie_title", "movie_director"]) \
    .select("movie_title", "movie_director", "actor_name")
print("\n==============================")
print("1. Protagonistas de películas con géneros Comedy y Drama:")
protagonistas.show(truncate=False)
print("==============================\n")

# 2. Usuarios registrados hace más de 6 meses y sin contratos nunca

usuarios_antiguos = df["users"].filter(datediff(current_date(), col("registration_date")) > 180)
usuarios_sin_contrato = usuarios_antiguos.join(df["contracts"], usuarios_antiguos["nickname"] == df["contracts"]["user_nickname"], "left_anti")
print("\n==============================")
print("2. Usuarios registrados hace más de 6 meses y sin contratos nunca:")
usuarios_sin_contrato.select("nickname", "registration_date").show(truncate=False)
print("==============================\n")

# 3. Director cuyas películas reciben más comentarios (de media)
comentarios_por_pelicula = df["ratings"].groupBy("movie_title", "movie_director").agg(count("comment").alias("num_comentarios"))
promedio_por_director = comentarios_por_pelicula.groupBy("movie_director").agg(avg("num_comentarios").alias("media_comentarios"))
director_top = promedio_por_director.orderBy(desc("media_comentarios")).limit(1)
print("\n==============================")
print("3. Director con más comentarios de media:")
director_top.show(truncate=False)
print("==============================\n")

# 4. Top 5 películas con mejores calificaciones IMDb y sus géneros
pelis_top5 = df["movies"].orderBy(desc("imdb_rating")).limit(5)
pelis_top5_generos = pelis_top5.join(df["movie_genres"], [pelis_top5["title"] == df["movie_genres"]["movie_title"], pelis_top5["director"] == df["movie_genres"]["movie_director"]], "left") \
    .select(pelis_top5["title"], pelis_top5["imdb_rating"], df["movie_genres"]["genre_name"])
print("\n==============================")
print("4. Top 5 películas IMDb y sus géneros:")
pelis_top5_generos.orderBy(desc("imdb_rating")).show(truncate=False)
print("==============================\n")

# 5. Directores que han dirigido películas en 3 o más géneros
pelis_generos = df["movie_genres"].groupBy("movie_director", "movie_title").agg(collect_set("genre_name").alias("generos"))
# Solución: primero explotar, luego agrupar y contar géneros distintos
pelis_generos_exploded = pelis_generos.select("movie_director", explode("generos").alias("genero"))
directores_varios_generos = pelis_generos_exploded.groupBy("movie_director").agg(countDistinct("genero").alias("num_generos")).filter(col("num_generos") >= 3)
print("\n==============================")
print("5. Directores con 3 o más géneros:")
directores_varios_generos.show(truncate=False)
print("==============================\n")

# 6. Obtén el actor más popular de cada película (el que más aparece en 'views')

views_actors = df["views"].join(df["movie_actors"], ["movie_title", "movie_director"])
actor_popular = views_actors.groupBy("movie_title", "movie_director", "actor_name") \
    .agg(spark_count("user_nickname").alias("num_vistas"))
window_actor = Window.partitionBy("movie_title", "movie_director").orderBy(desc("num_vistas"))
actor_popular = actor_popular.withColumn("rank", row_number().over(window_actor)).filter(col("rank") == 1)
print("\n==============================")
print("6. Actor más popular de cada película:")
actor_popular.select("movie_title", "movie_director", "actor_name", "num_vistas").show(truncate=False)
print("==============================\n")

# 7. Top 5 usuarios que han visto más películas
user_views = df["views"].groupBy("user_nickname").agg(spark_count("movie_title").alias("num_vistas"))
print("\n==============================")
print("7. Top 5 usuarios que han visto más películas:")
user_views.orderBy(desc("num_vistas")).limit(5).show(truncate=False)
print("==============================\n")

# 8. Calificación media de IMDb de las películas en cada género
movies_genres = df["movies"].join(df["movie_genres"], [df["movies"]["title"] == df["movie_genres"]["movie_title"], df["movies"]["director"] == df["movie_genres"]["movie_director"]])
genre_avg = movies_genres.groupBy("genre_name").agg(avg("imdb_rating").alias("media_imdb"))
print("\n==============================")
print("8. Calificación media de IMDb por género:")
genre_avg.orderBy(desc("media_imdb")).show(truncate=False)
print("==============================\n")

# 9. Palabras clave más comunes en todas las películas
keyword_counts = df["movie_keywords"].groupBy("keyword_word").agg(spark_count("movie_title").alias("num_peliculas"))
print("\n==============================")
print("9. Palabras clave más comunes:")
keyword_counts.orderBy(desc("num_peliculas")).show(truncate=False)
print("==============================\n")

# 10. Usuarios cuyos contratos terminan en los próximos 30 días

contratos_proximos = df["contracts"].filter((col("end_date") >= current_date()) & (col("end_date") <= date_add(current_date(), 30)))
print("\n==============================")
print("10. Usuarios con contratos que terminan en los próximos 30 días:")
contratos_proximos.select("user_nickname", "end_date").show(truncate=False)
print("==============================\n")

# 11. Películas con múltiples géneros junto con sus géneros
multi_genre_movies = df["movie_genres"].groupBy("movie_title", "movie_director").agg(collect_set("genre_name").alias("generos")) \
    .filter(spark_count("genre_name") > 1)
print("\n==============================")
print("11. Películas con múltiples géneros:")
multi_genre_movies.show(truncate=False)
print("==============================\n")

# 12. Géneros de las 10 películas más vistas
pelis_mas_vistas = df["views"].groupBy("movie_title", "movie_director").agg(spark_count("user_nickname").alias("num_vistas")) \
    .orderBy(desc("num_vistas")).limit(10)
generos_10_mas_vistas = pelis_mas_vistas.join(df["movie_genres"], ["movie_title", "movie_director"]).select("movie_title", "movie_director", "genre_name", "num_vistas")
print("\n==============================")
print("12. Géneros de las 10 películas más vistas:")
generos_10_mas_vistas.orderBy(desc("num_vistas")).show(truncate=False)
print("==============================\n")

# 13. Usuarios que han visualizado películas en al menos 3 idiomas
vistas_idiomas = df["views"].join(df["movies"], [df["views"]["movie_title"] == df["movies"]["title"], df["views"]["movie_director"] == df["movies"]["director"]])
usuarios_idiomas = vistas_idiomas.groupBy("user_nickname").agg(countDistinct("language").alias("num_idiomas")) \
    .filter(col("num_idiomas") >= 3)
print("\n==============================")
print("13. Usuarios que han visto películas en al menos 3 idiomas:")
usuarios_idiomas.show(truncate=False)
print("==============================\n")

spark.stop()
