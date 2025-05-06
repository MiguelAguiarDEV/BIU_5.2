from pyspark.sql import SparkSession

# Configura la sesión de Spark
spark = SparkSession.builder \
    .appName("SQL Queries on MySQL Data") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
# Configuración de conexión JDBC a MySQL
jdbc_url = "jdbc:mysql://mysql-moviebind:3306/moviebind"
props = {
    "user": "user",
    "password": "1234",
    "driver": "com.mysql.cj.jdbc.Driver"
}

tables = [
    "users", "profiles", "contract_types", "contracts", "movies", "actors",
    "movie_actors", "genres", "movie_genres", "keywords", "movie_keywords",
    "views", "ratings"
]

# Cargar y registrar vistas temporales
for t in tables:
    df = spark.read.jdbc(url=jdbc_url, table=t, properties=props)
    df.createOrReplaceTempView(t)

# Ejemplo de consultas SQL en Spark
# 1. Número de películas por director
print("Películas por director:")
spark.sql("""
    SELECT director, COUNT(*) AS num_movies
    FROM movies
    GROUP BY director
    ORDER BY num_movies DESC
""").show()

# 2. Usuarios y número de visualizaciones
print("Visualizaciones por usuario:")
spark.sql("""
    SELECT user_nickname, COUNT(*) AS num_views
    FROM views
    GROUP BY user_nickname
    ORDER BY num_views DESC
""").show()

# 3. Media de rating por película
print("Media de rating por película:")
spark.sql("""
    SELECT movie_title, movie_director, ROUND(AVG(rating),2) AS avg_rating
    FROM ratings
    GROUP BY movie_title, movie_director
    ORDER BY avg_rating DESC
""").show()

# 4. Películas vistas por cada usuario
print("Películas vistas por cada usuario:")
spark.sql("""
    SELECT v.user_nickname, m.title, m.director
    FROM views v
    JOIN movies m ON v.movie_title = m.title AND v.movie_director = m.director
    ORDER BY v.user_nickname
""").show()

# 5. Protagonistas de películas que se enmarcan, al menos, en los géneros ‘comedy’ y ‘drama’ al mismo tiempo.
print("5. Protagonistas de películas con géneros 'comedy' y 'drama':")
spark.sql("""
SELECT ma.movie_title, ma.movie_director, ma.actor_name
FROM movie_actors ma
JOIN (
    SELECT mg1.movie_title, mg1.movie_director
    FROM movie_genres mg1
    JOIN movie_genres mg2
      ON mg1.movie_title = mg2.movie_title AND mg1.movie_director = mg2.movie_director
    WHERE LOWER(mg1.genre_name) = 'comedy' AND LOWER(mg2.genre_name) = 'drama'
) mg
ON ma.movie_title = mg.movie_title AND ma.movie_director = mg.movie_director
""").show()

# 6. Usuarios registrados hace más de 6 meses y sin contrato nunca
print("6. Usuarios registrados hace más de 6 meses y sin contrato nunca:")
spark.sql("""
SELECT u.nickname
FROM users u
LEFT JOIN contracts c ON u.nickname = c.user_nickname
WHERE c.user_nickname IS NULL
  AND u.registration_date < DATE_SUB('2025-05-06', 180)
""").show()

# 7. Director cuyas películas reciben más comentarios (de media)
print("7. Director con más comentarios de media en ratings:")
spark.sql("""
SELECT movie_director, ROUND(AVG(num_comments),2) AS avg_comments
FROM (
    SELECT movie_director, movie_title, COUNT(comment) AS num_comments
    FROM ratings
    WHERE comment IS NOT NULL AND comment <> ''
    GROUP BY movie_director, movie_title
) t
GROUP BY movie_director
ORDER BY avg_comments DESC
LIMIT 1
""").show()

# 8. Top 5 películas con mejores calificaciones IMDb y sus géneros
print("8. Top 5 películas por calificación IMDb y sus géneros:")
spark.sql("""
SELECT m.title, m.director, m.imdb_rating, COLLECT_SET(mg.genre_name) AS genres
FROM movies m
JOIN movie_genres mg ON m.title = mg.movie_title AND m.director = mg.movie_director
GROUP BY m.title, m.director, m.imdb_rating
ORDER BY m.imdb_rating DESC
LIMIT 5
""").show()

# 9. Directores con películas en 3 o más géneros
print("9. Directores con películas en 3 o más géneros:")
spark.sql("""
SELECT director, COUNT(DISTINCT genre_name) AS num_genres
FROM movies m
JOIN movie_genres mg ON m.title = mg.movie_title AND m.director = mg.movie_director
GROUP BY director
HAVING num_genres >= 3
""").show()

# 10. Actor más popular de cada película (más visualizaciones)
print("10. Actor más popular de cada película:")
spark.sql("""
SELECT movie_title, movie_director, actor_name, num_views
FROM (
    SELECT ma.movie_title, ma.movie_director, ma.actor_name, COUNT(v.user_nickname) AS num_views,
           ROW_NUMBER() OVER (PARTITION BY ma.movie_title, ma.movie_director ORDER BY COUNT(v.user_nickname) DESC) as rn
    FROM movie_actors ma
    JOIN views v ON ma.movie_title = v.movie_title AND ma.movie_director = v.movie_director
    GROUP BY ma.movie_title, ma.movie_director, ma.actor_name
) t
WHERE rn = 1
""").show()

# 11. Top 5 usuarios que han visto más películas
print("11. Top 5 usuarios con más visualizaciones:")
spark.sql("""
SELECT user_nickname, COUNT(*) AS num_views
FROM views
GROUP BY user_nickname
ORDER BY num_views DESC
LIMIT 5
""").show()

# 12. Calificación media de IMDb por género
print("12. Calificación media de IMDb por género:")
spark.sql("""
SELECT mg.genre_name, ROUND(AVG(m.imdb_rating),2) AS avg_imdb
FROM movies m
JOIN movie_genres mg ON m.title = mg.movie_title AND m.director = mg.movie_director
GROUP BY mg.genre_name
ORDER BY avg_imdb DESC
""").show()

# 13. Palabras clave más comunes en todas las películas
print("13. Palabras clave más comunes:")
spark.sql("""
SELECT keyword_word, COUNT(*) AS freq
FROM movie_keywords
GROUP BY keyword_word
ORDER BY freq DESC
LIMIT 10
""").show()

# 14. Usuarios con contratos que terminan en los próximos 30 días
print("14. Usuarios con contratos que terminan en los próximos 30 días:")
spark.sql("""
SELECT user_nickname, end_date
FROM contracts
WHERE end_date BETWEEN '2025-05-06' AND DATE_ADD('2025-05-06', 30)
""").show()

# 15. Películas con múltiples géneros y sus géneros
print("15. Películas con múltiples géneros:")
spark.sql("""
SELECT movie_title, movie_director, COLLECT_SET(genre_name) AS genres
FROM movie_genres
GROUP BY movie_title, movie_director
HAVING SIZE(COLLECT_SET(genre_name)) > 1
""").show()

# 16. Géneros de las 10 películas más vistas
print("16. Géneros de las 10 películas más vistas:")
spark.sql("""
WITH top_movies AS (
    SELECT movie_title, movie_director, COUNT(*) AS num_views
    FROM views
    GROUP BY movie_title, movie_director
    ORDER BY num_views DESC
    LIMIT 10
)
SELECT t.movie_title, t.movie_director, mg.genre_name
FROM top_movies t
JOIN movie_genres mg ON t.movie_title = mg.movie_title AND t.movie_director = mg.movie_director
""").show()

# 17. Usuarios que han visualizado películas en al menos 3 idiomas
print("17. Usuarios que han visto películas en al menos 3 idiomas:")
spark.sql("""
SELECT v.user_nickname, COUNT(DISTINCT m.language) AS num_languages
FROM views v
JOIN movies m ON v.movie_title = m.title AND v.movie_director = m.director
GROUP BY v.user_nickname
HAVING num_languages >= 3
""").show()

spark.stop()
