from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder.appName("ReadMariaDB").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

url = "jdbc:mariadb://mariadb-moviebind:3306/moviebind?useLegacyDatetimeCode=false&serverTimezone=UTC"
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
# for table in tables:
    
#     df = spark.read.jdbc(url=url, table=table, properties=properties)
#     print(f"\nTabla: {table}")
#     df.printSchema()
#     df.show()

df = spark.read \
    .jdbc(url=url, table="users", properties=properties)

df = df.select(to_date("registration_date", "yyyy-MM-dd").alias("formatted_date"))

print(df.show())

