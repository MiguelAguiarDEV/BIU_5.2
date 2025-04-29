# Práctica 5.2: Spark, MariaDB y Hadoop

En esta práctica se integran Spark, MariaDB y Hadoop usando Docker. A continuación se detallan los comandos y pasos para cada apartado, siguiendo el mismo estilo que la práctica anterior.

---

## 1. Preparación del entorno

### 1.1. Eliminar contenedores y red anteriores (opcional)
```sh
docker rm -f spark-master-custom hadoop-namenode mariadb-moviebind || true
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
docker build -t my-mariadb ./mariadb-docker
```
_Compila las imágenes de Spark, Hadoop y MariaDB._

### 1.4. Iniciar los contenedores
```sh
docker run -d --name mariadb-moviebind --network spark-network -p 3306:3306 my-mariadb
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
docker logs mariadb-moviebind
# Ver logs de MariaDB

docker logs spark-master-custom
# Ver logs de Spark

docker logs hadoop-namenode
# Ver logs de Hadoop
```
_Útil para depuración y ver errores de arranque._

---

## 2. Crear un DataFrame con todas las tablas y sus datos de MariaDB

### 2.1. Acceder al contenedor de Spark
```sh
docker exec -it spark-master-custom bash
```
_Entra en el contenedor para ejecutar scripts de Spark._

### 2.2. Crear el script para leer las tablas de MariaDB
Crea el archivo `read_all_tables.py` en la carpeta `spark-docker` (se verá en `/opt/spark/data` dentro del contenedor):

```python
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
```

### 2.3. Ejecutar el script en Spark
```sh
spark-submit /opt/spark/data/read_all_tables.py
```
_Ejecuta el script y verás los datos de cada tabla en la salida._

---

# (Añade aquí los siguientes apartados de la práctica 5.2 siguiendo este mismo formato)


