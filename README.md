# Práctica 5.2: Spark, MySQL y Hadoop

En esta práctica se integran Spark, MySQL y Hadoop usando Docker. A continuación se detallan los comandos y pasos para cada apartado, siguiendo el mismo estilo que la práctica anterior.

---

## 1. Preparación del entorno

### 1.1. Eliminar contenedores y red anteriores (opcional)
```sh
docker rm -f spark-master-custom hadoop-namenode mysql-moviebind || true
docker network rm spark-network || true
```
_Elimina contenedores y red si existen, para evitar conflictos._

### 1.2. Crear la red Docker
```sh
docker network create spark-network
```
_Crea la red para que los contenedores se comuniquen._

### 1.3. Construir las imágenes personalizadas
```sh
docker build -t spark-custom:3.3.2 ./spark-docker
docker build -t hadoop-custom:3.3.2 ./hadoop-docker
docker build -t my-mysql ./mysql-docker
```
_Compila las imágenes de Spark, Hadoop y MySQL._

### 1.4. Iniciar los contenedores
```sh
docker run -d --name mysql-moviebind --network spark-network -p 3306:3306 my-mysql
docker run -d --name spark-master-custom --network spark-network -p 8080:8080 -p 7077:7077 -v "$(pwd)/spark-docker:/opt/spark/data" spark-custom:3.3.2 tail -f /dev/null
docker run -d --name hadoop-namenode --network spark-network hadoop-custom:3.3.2 tail -f /dev/null
```
_Lanza los contenedores en la red spark-network y monta la carpeta spark-docker en /opt/spark/data del contenedor de Spark._

### 1.5. (Solo la primera vez) Formatear el NameNode de Hadoop
```sh
docker exec -it hadoop-namenode bash -c "/opt/hadoop/bin/hdfs namenode -format"
```
_Inicializa el sistema de archivos HDFS._

### 1.6. Acceder al contenedor Hadoop e iniciar servicios HDFS
```sh
docker exec -it hadoop-namenode bash
# Dentro del contenedor ejecuta:
export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root
/opt/hadoop/sbin/hadoop-daemon.sh start namenode
/opt/hadoop/sbin/hadoop-daemon.sh start datanode
/opt/hadoop/sbin/hadoop-daemon.sh start secondarynamenode
exit
```
_Inicia los servicios de HDFS._

### 1.7. Instalar ping (iputils-ping) en un contenedor (por ejemplo, Spark)
```sh
docker exec -it spark-master-custom bash -c "apt-get update && apt-get install -y iputils-ping"
```
_Esto permite usar el comando ping dentro del contenedor._

### 1.8. Ver logs de los contenedores
```sh
docker logs mysql-moviebind
# Ver logs de MySQL

docker logs spark-master-custom
# Ver logs de Spark

docker logs hadoop-namenode
# Ver logs de Hadoop
```
_Útil para depuración y ver errores de arranque._

---

## 2. Crear un DataFrame con todas las tablas y sus datos de MySQL

### 2.1. Acceder al contenedor de Spark
```sh
docker exec -it spark-master-custom bash
```
_Entra en el contenedor para ejecutar scripts de Spark._

### 2.2. Crear el script para leer las tablas de MySQL
Crea el archivo `read_all_tables.py` en la carpeta `spark-docker` (se verá en `/opt/spark/data` dentro del contenedor):

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadMySQL").getOrCreate()

url = "jdbc:mysql://mysql-moviebind:3306/moviebind"
properties = {
    "user": "user",
    "password": "1234",
    "driver": "com.mysql.cj.jdbc.Driver"
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
```

### 2.3. Ejecutar el script en Spark
```sh
spark-submit /opt/spark/data/read_all_tables.py
```
_Ejecuta el script y verás los datos de cada tabla en la salida._

---

## 3. Ejecución de scripts de Spark y tareas de la práctica

A continuación se explica cómo ejecutar cada uno de los scripts principales de la carpeta `spark-docker` para resolver las tareas de la práctica. Todos los comandos deben ejecutarse dentro del contenedor de Spark:

```sh
docker exec -it spark-master-custom bash
cd /opt/spark/data
```

### 3.1. Consultas SQL sobre MySQL con Spark
Ejecuta las consultas SQL sobre las tablas de MySQL usando Spark SQL:
```sh
spark-submit /opt/spark/data/sql_queries.py
```
_Este script realiza consultas SQL sobre las tablas importadas desde MySQL y muestra los resultados por pantalla._

### 3.2. Consultas nativas con PySpark
Ejecuta las consultas usando solo PySpark (sin SQL):
```sh
spark-submit /opt/spark/data/native_queries.py
```
_Este script resuelve las tareas usando solo funciones nativas de PySpark._

### 3.3. Ejercicio de nombres (nombres.json)
Procesa el fichero `nombres.json` y realiza las transformaciones pedidas:
```sh
spark-submit /opt/spark/data/names.py
```
_Este script lee el archivo JSON de nombres y aplica transformaciones sobre los datos._

### 3.4. Ejercicio de ventas con nulos (VentasNulos.csv)
Procesa el fichero de ventas con nulos y aplica las transformaciones:
```sh
spark-submit /opt/spark/data/sales.py
```
_Este script limpia y transforma los datos de ventas, gestionando los valores nulos según lo solicitado en la práctica._

### 3.5. Mostrar todas las tablas de MySQL como DataFrame
Muestra el contenido de todas las tablas de MySQL como DataFrame:
```sh
spark-submit /opt/spark/data/table_data_frame.py
```
_Este script conecta a MySQL y muestra los datos de todas las tablas principales._

---

Puedes añadir más scripts siguiendo este mismo formato. Si algún script requiere argumentos, indícalo en la línea de ejecución correspondiente.


