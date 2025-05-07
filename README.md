# Práctica 5.2: Análisis de Ventas y Consultas con Spark, MySQL y Hadoop

Esta práctica integra Spark, MySQL y Hadoop usando Docker. Aquí tienes los pasos y comandos para cada apartado, adaptados a los ejercicios y scripts de esta práctica. Cada sección incluye una breve explicación y cómo ver los resultados.

---

## 1. Preparación del entorno Docker

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
_Lanza los contenedores y monta la carpeta de trabajo en Spark._

### 1.5. (Solo la primera vez) Formatear el NameNode de Hadoop
```sh
docker exec -it hadoop-namenode bash -c "/opt/hadoop/bin/hdfs namenode -format"
```
_Inicializa el sistema de archivos HDFS._

### 1.6. Iniciar servicios HDFS en Hadoop
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

### 1.7. Instalar ping (opcional, para pruebas de red)
```sh
docker exec -it spark-master-custom bash -c "apt-get update && apt-get install -y iputils-ping"
```
_Permite usar ping dentro del contenedor._

---

## 2. Ejecución de scripts

### 2.1. Mostrar todas las tablas de MySQL como DataFrame
```sh
spark-submit /opt/spark/data/table_data_frame.py
```
_Conecta a MySQL y muestra los datos de todas las tablas principales como DataFrame por consola._

### 2.2. Consultas nativas con PySpark
```sh
spark-submit /opt/spark/data/native_queries.py
```
_Resuelve las tareas usando solo funciones nativas de PySpark. Los resultados se muestran por consola._

### 2.3. Consultas SQL sobre MySQL con Spark
```sh
spark-submit /opt/spark/data/sql_queries.py
```
_Realiza consultas SQL sobre las tablas importadas desde MySQL y muestra los resultados por pantalla._

### 2.4. Ejercicio de nombres (nombres.json)
```sh
spark-submit /opt/spark/data/names.py
```
_Lee el archivo JSON de nombres y aplica las transformaciones pedidas. Los resultados se muestran por consola._

### 2.5. Ejercicio de ventas con nulos (VentasNulos.csv)
```sh
spark-submit /opt/spark/data/sales.py
```
_Limpia y transforma los datos de ventas, gestionando los valores nulos según lo solicitado. Los resultados se muestran por consola._

### 2.6. Utilizar UDF en Spark y Spark SQL
```sh
spark-submit /opt/spark/data/user_defined_function.py
```
_Ejecuta ejemplos de funciones definidas por el usuario (UDF) en Spark y Spark SQL. Los resultados se muestran por consola._

### 2.7. Visualización de datos estadísticos con pandas
```sh
spark-submit /opt/spark/data/pandas_script.py
```
_Utiliza pandas para realizar análisis estadísticos y generar visualizaciones. Los gráficos se guardan como archivos PNG en `data/viz/` y los resultados se muestran por consola._

### 2.8. Análisis de películas (movies.py)
```sh
spark-submit /opt/spark/data/movies.py
```
_Analiza datos de películas y calificaciones. Los resultados se muestran por consola._

### 2.9. Análisis de ventas de tienda de tecnología (tech_sales.py)
```sh
spark-submit /opt/spark/data/tech_sales.py
```
_Analiza datos de ventas de una tienda de tecnología. Los resultados se muestran por consola._

### 2.10. Carga y extracción de información con Spark SQL sobre ventas (load_and_extraction_sql.py)
```sh
spark-submit /opt/spark/data/load_and_extraction_sql.py
```
_Procesa todos los archivos de ventas en `salesdata/`, limpia y transforma los datos, guarda los resultados en formato Parquet en `salesoutput/` y genera gráficos en la carpeta de trabajo. Los resultados y gráficos se muestran por consola y se guardan como archivos PNG._

- **Resultados:**
  - Gráficos generados: `ventas_por_mes.png`, `top_ciudades.png`, `pedidos_por_hora.png` (en la carpeta donde ejecutas el script, típicamente `/opt/spark/data/` dentro del contenedor)
  - Estructura de particiones Parquet en `salesoutput/`
  - Listados y tablas por consola (estados, productos comprados juntos, etc.)

---

## 3. Visualización de resultados

- Los gráficos generados por los scripts de ventas se guardan en la carpeta de trabajo (`ventas_por_mes.png`, `top_ciudades.png`, `pedidos_por_hora.png`, etc.).
- Los resultados de las consultas y transformaciones se muestran por consola al ejecutar cada script.
- Los archivos Parquet generados se encuentran en la carpeta `salesoutput/` y pueden ser consultados con Spark si se desea.

---

**Nota:** Todos los scripts deben ejecutarse dentro del contenedor de Spark (`spark-master-custom`) en la ruta `/opt/spark/data`.

Puedes añadir más scripts siguiendo este mismo formato. Si algún script requiere argumentos, indícalo en la línea de ejecución correspondiente.


