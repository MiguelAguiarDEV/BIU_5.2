from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadMariaDB").getOrCreate()

url = "jdbc:mariadb://mariadb-moviebind:3306/moviebind"
properties = {
    "user": "user",
    "password": "1234",
    "driver": "org.mariadb.jdbc.Driver"
}

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

dataframes = {}
for table in tables:
    df = spark.read.jdbc(url=url, table=table, properties=properties)
    dataframes[table] = df
    df.show()