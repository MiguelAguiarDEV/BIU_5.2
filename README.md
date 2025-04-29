# 1. Algoritmo de Euclides

El algoritmo de Euclides es un método clásico y muy eficiente para calcular el **máximo común divisor (MCD)** de dos números enteros positivos. Se basa en la siguiente propiedad fundamental:

> **Propiedad.** Para $a, b \in \mathbb{Z}^+$ con $a \ge b$:

> $$
> \gcd(a, b) = \gcd\bigl(b,\,a \bmod b\bigr).
> $$

Iterando este paso (reemplazando $(a, b)$ por $(b,\,a \bmod b)$) hasta que el segundo término sea cero, el primer término restante es el MCD.

---

## ¿Para qué puede usarse?

- **Simplificación de fracciones.** Para reducir $\frac{a}{b}$ a su forma irreducible dividiendo numerador y denominador por $\gcd(a,b)$.
- **Criptografía.** En algoritmos como RSA, para calcular inversos modulares y en pruebas de primalidad.
- **Ecuaciones diofánticas lineales.** Para resolver $ax + by = c$ sólo si $\gcd(a,b)\mid c$.
- **Geometría computacional y gráficos.** Para determinar pasos enteros de un segmento de recta o en rasterización (algoritmo de Bresenham).
- **Teoría de números y análisis algorítmico.** Sirve de bloque básico en muchos teoremas y algoritmos que manipulan divisores.

---

## Ejemplo de ejecución en notación matemática

Calculemos $\gcd(48,18)$ paso a paso:

```latex
\begin{aligned}
48 &= 18 \times 2 \; + \; 12,      &\quad&\gcd(48,18) = \gcd(18,12)\\
18 &= 12 \times 1 \; + \; 6,       &\quad&\gcd(18,12) = \gcd(12,6)\\
12 &= 6  \times 2 \; + \; 0,       &\quad&\gcd(12,6) = 6
\end{aligned}
```

Como el resto llega a cero, el **MCD** es el último divisor distinto de cero:

$$
\boxed{\gcd(48,18) = 6}.
$$

---

## Comandos en `spark-docker`

Dentro del directorio `spark-docker`, ejecuta los siguientes comandos:

1. **Crear la red para Spark:**
   ```bash
   docker network create spark-network
   ```
2. **Construir la imagen personalizada:**
   ```bash
   docker build -t spark-custom:3.3.2 .
   ```
3. **Iniciar el contenedor maestro:**
   ```bash
   docker run -d --name spark-master-custom --network spark-network \
     -p 8080:8080 -p 7077:7077 \
     -v "${PWD}:/opt/spark/data" \
     spark-custom:3.3.2 tail -f /dev/null
   ```
4. **Arrancar el Master manualmente:**
   ```bash
   docker exec -d spark-master-custom bash -c "/opt/spark/bin/spark-class \
     org.apache.spark.deploy.master.Master --host 0.0.0.0 --port 7077 \
     --webui-port 8080"
   ```
5. **Ejecutar el script Euclid y filtrar resultados:**
   ```bash
   docker exec spark-master-custom bash -c "/opt/spark/bin/spark-submit \
     --master local[*] data/euclid.py 2>&1 | grep '^gcd'"
   ```

![Pasted image 20250426015627](https://github.com/user-attachments/assets/16bd195c-f55a-4f97-bf38-12d61dc62165)


![Pasted image 20250426015627](https://github.com/user-attachments/assets/b54a2d85-3ca2-4758-9dc1-0d109ab5a404)


# 2. 

## 2.1. Ejecución movies_each_average

   ```bash
docker exec spark-master-custom bash -c "/opt/spark/bin/spark-submit --master local[*] data/movies_each_average.py 2>/dev/null"
   ```

Nota media de todas las votaciones de cada película. 
![Pasted image 20250426015627](https://github.com/user-attachments/assets/42f7b74d-43ce-4265-9ec4-83b3eb2dac7a)


## 2.2 Ejecución  average_movie_mark

   ```bash
docker exec spark-master-custom bash -c "/opt/spark/bin/spark-submit --master local[*] data/average_movie_mark.py 2>/dev/null"
   ```
Notas medias superiores a 3
![Pasted image 20250426015627](https://github.com/user-attachments/assets/4b2cf32b-3c57-4d4e-8dc3-afb1ec1f65dd)


# 3.

```bash
docker exec spark-master-custom bash -c '/opt/spark/bin/spark-submit --master local[*] data/mapreduce.py'
```

![Pasted image 20250426015627](https://github.com/user-attachments/assets/d22dfa38-6e94-429c-86f1-34509669015c)


# 4.

```bash
docker exec spark-master-custom bash -c "/opt/spark/bin/spark-submit --master local[*] data/movies_rdd.py"
```

![Pasted image 20250426015627](https://github.com/user-attachments/assets/ce440f50-ffb9-4ff6-a470-a4151b5dcfcb)

Muestra muchas películas porque hay pocas o ninguna con menos de 4 letras en el nombre.
Aquí una prueba cambiando 4 por 10
![Pasted image 20250426025448](https://github.com/user-attachments/assets/87b4f4e5-0514-4f31-8dbe-606bf071bf56)


# 5.
## 5.1

```bash
docker exec spark-master-custom bash -c "/opt/spark/bin/spark-submit --master local[*] data/quijote_all_words_list.py"
```
Lista de todas las palabras:
![Pasted image 20250426030534](https://github.com/user-attachments/assets/c1b6f260-ffe4-49a5-990d-ec45277b22c1)


*Extras*
Total de palabras + 50 primeras palabras:
![Pasted image 20250426030621](https://github.com/user-attachments/assets/e133fdba-30fc-47a9-8669-718d20f315bf)


## 5.2

# Práctica 5.2: Spark, MariaDB y Hadoop

En esta práctica se integran Spark, MariaDB y Hadoop usando Docker. A continuación se detallan los comandos y pasos para cada apartado, siguiendo el mismo estilo que la práctica anterior.

---

## 1. Preparación del entorno

### 1.1. Eliminar contenedores y red anteriores (opcional)
```bash
docker rm -f spark-master-custom hadoop-namenode mariadb-moviebind || true
docker network rm spark-network || true
```
_Elimina contenedores y red si existen, para evitar conflictos._

### 1.2. Crear la red Docker
```bash
docker network create spark-network
```
_Crea la red para que los contenedores se comuniquen._

### 1.3. Construir las imágenes personalizadas
```bash
docker build -t spark-custom:3.3.2 ./spark-docker
docker build -t hadoop-custom:3.3.2 ./hadoop-docker
docker build -t my-mariadb ./mariadb-docker
```
_Compila las imágenes de Spark, Hadoop y MariaDB._

### 1.4. Iniciar los contenedores
```bash
docker run -d --name mariadb-moviebind --network spark-network -p 3306:3306 my-mariadb
docker run -d --name spark-master-custom --network spark-network -p 8080:8080 -p 7077:7077 -v "${PWD}:/opt/spark/data" spark-custom:3.3.2 tail -f /dev/null
docker run -d --name hadoop-namenode --network spark-network hadoop-custom:3.3.2 tail -f /dev/null
```
_Lanza los contenedores en la red spark-network._

### 1.5. (Solo la primera vez) Formatear el NameNode de Hadoop
```bash
docker exec -it hadoop-namenode bash -c "/opt/hadoop/bin/hdfs namenode -format"
```
_Inicializa el sistema de archivos HDFS._

### 1.6. Acceder al contenedor Hadoop e iniciar servicios HDFS
```bash
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

---

## 2. Crear un DataFrame con todas las tablas y sus datos de MariaDB

### 2.1. Acceder al contenedor de Spark
```bash
docker exec -it spark-master-custom bash
```
_Entra en el contenedor para ejecutar scripts de Spark._

### 2.2. Crear el script para leer las tablas de MariaDB
Crea el archivo `read_all_tables.py` en tu carpeta local (se verá en `/opt/spark/data` dentro del contenedor):

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
```bash
spark-submit /opt/spark/data/read_all_tables.py
```
_Ejecuta el script y verás los datos de cada tabla en la salida._

---

# (Añade aquí los siguientes apartados de la práctica 5.2 siguiendo este mismo formato)

```bash
docker exec spark-master-custom bash -c "/opt/spark/bin/spark-submit --master local[*] data/quijote_word_count.py dichoso"  #Puedes cambiar la palabra
```

![Pasted image 20250426030958](https://github.com/user-attachments/assets/8e5d7e28-7fcc-4aae-814e-4413e0482250)


## 5.3

Dentro de la carpeta 'hadoop-docker'
1. Construir la imagen de Hadoop
```bash
docker build -t hadoop-custom:3.3.2 .
```

2. Iniciar el contenedor de Hadoop
```bash
docker run -d --name hadoop-namenode --network spark-network hadoop-custom:3.3.2 tail -f /dev/null
```

3. Formatear el NameNode (solo la primera vez)
```bash
docker exec -it hadoop-namenode bash -c "/opt/hadoop/bin/hdfs namenode -format"
```

4. Acceder al contenedor para iniciar los servicios manualmente
```bash
docker exec -it hadoop-namenode bash
```

5. Dentro del contenedor, ejecutar:

```bash
export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root
/opt/hadoop/sbin/hadoop-daemon.sh start namenode
/opt/hadoop/sbin/hadoop-daemon.sh start datanode
/opt/hadoop/sbin/hadoop-daemon.sh start secondarynamenode
```

6. (Opcional) Verificar HDFS dentro del contenedor
```bash
/opt/hadoop/bin/hdfs dfs -ls /
```

7. Salir del contenedor Hadoop
```bash
exit
```

8. Ejecutar el script de Spark para guardar el resultado en HDFS
```bash
docker exec -it spark-master-custom bash -c "/opt/spark/bin/spark-submit data/quijote_wordcount_to_hdfs.py"
```

9. Comprobar el resultado en HDFS

```bash
docker exec -it hadoop-namenode bash -c "/opt/hadoop/bin/hdfs dfs -ls /quijote_wordcount"

docker exec -it hadoop-namenode bash -c "/opt/hadoop/bin/hdfs dfs -cat /quijote_wordcount/part-* | head"
```

![Pasted image 20250426035026](https://github.com/user-attachments/assets/f29c6d12-1c2c-4713-a375-d02d34dd1433)



# 6.

```bash
docker exec -it spark-master-custom bash -c "/opt/spark/bin/spark-submit data/numbers_task.py"
```

![Pasted image 20250426035942](https://github.com/user-attachments/assets/a825e6cd-ce24-48aa-8bda-02d81e39b680)


# 7. 

```bash
docker exec -it spark-master-custom bash -c "/opt/spark/bin/spark-submit data/words_rdd_tasks.py"
```

![Pasted image 20250426040206](https://github.com/user-attachments/assets/25bc1580-a065-4e56-89d6-e5583b305f28)


# 8. 

```bash
docker exec -it spark-master-custom bash -c "/opt/spark/bin/spark-submit data/names_rdd_tasks.py"
```

![Pasted image 20250426041515](https://github.com/user-attachments/assets/171ec4dd-ed3f-4230-91d3-1191bc70a289)


# 9.

```bash
 docker exec -it spark-master-custom bash -c "/opt/spark/bin/spark-submit data/marks_rdd_tasks.py"
```
![Pasted image 20250426042348](https://github.com/user-attachments/assets/af7e4543-b1de-4cf5-80b0-027abcdc3589)

![Pasted image 20250426042400](https://github.com/user-attachments/assets/cdbbf8ef-1749-407d-a0ef-ec367f5a3d3d)

![Pasted image 20250426042408](https://github.com/user-attachments/assets/03c0878a-3530-4934-b26f-cb25003862cb)

![Pasted image 20250426042419](https://github.com/user-attachments/assets/5f15e3af-fd0e-4a23-9b71-6c71f316368d)

![Pasted image 20250426042427](https://github.com/user-attachments/assets/f3f29943-fe10-426f-9f6a-4ab81a3bf8e7)

![Pasted image 20250426042434](https://github.com/user-attachments/assets/50c406ea-c54c-4481-b718-08d6d3164ff7)

![Pasted image 20250426042442](https://github.com/user-attachments/assets/da40d151-ac9c-41b4-b2f2-d6f001a7f3da)

![Pasted image 20250426042455](https://github.com/user-attachments/assets/459774e0-3729-4a33-8790-9765c8d084bb)

![Pasted image 20250426042502](https://github.com/user-attachments/assets/066b7828-f300-4d2c-8c53-799ee9952265)

![Pasted image 20250426042508](https://github.com/user-attachments/assets/78fdfac5-d188-48be-af2f-10d591ac6024)


