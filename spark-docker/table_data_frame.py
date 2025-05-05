import mariadb
import sys
from pyspark.sql import SparkSession, Row
from datetime import date

spark = SparkSession.builder.appName("ReadMariaDB_NonSparkConnector").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Database connection details
db_host = "mariadb-moviebind"
db_port = 3306
db_name = "moviebind"
db_user = "user"
db_password = "1234"

try:
    # Connect to MariaDB
    conn = mariadb.connect(
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port,
        database=db_name
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

# Get cursor
cur = conn.cursor(dictionary=True) # Fetch as dictionaries

# Get all table names from the database
cur.execute("SHOW TABLES")
table_names = [row[f'Tables_in_{db_name}'] for row in cur.fetchall()]

dataframes = {}

for table in table_names:
    cur.execute(f"SELECT * FROM {table}")
    results = cur.fetchall()
    rows = [Row(**row) for row in results]
    df = spark.createDataFrame(rows)
    dataframes[table] = df
    print(f"Tabla: {table}")
    df.printSchema()
    print(f"Número de filas: {df.count()}")
    try:
        df.show(truncate=False)
    except Exception as e:
        print(f"Error during show() for table {table}: {e}")

# Close connection
conn.close()