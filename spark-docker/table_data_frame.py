from pyspark.sql import SparkSession

# Inicializa la SparkSession
spark = SparkSession.builder.appName("Read MySQL Tables with Spark").getOrCreate()

# Parámetros de conexión JDBC
jdbc_url = "jdbc:mysql://mariadb-moviebind:3306/moviebind"
props = {"user": "user", "password": "1234", "driver": "com.mysql.cj.jdbc.Driver"}

# Error level info
spark.sparkContext.setLogLevel("ERROR")
# Lista fija de tablas según tu init.sql
tables = [
    "users",
    "profiles",
    "contract_types",
    "contracts",
    "movies",
    "actors",
    "movie_actors",
    "genres",
    "movie_genres",
    "keywords",
    "movie_keywords"
]

for t in tables:
    df = spark.read.jdbc(url=jdbc_url, table=t, properties=props)
    df.show(truncate=False)

spark.stop()
