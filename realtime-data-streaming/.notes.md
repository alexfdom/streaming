```
pip install apache-airflow

pip install kafka-python

```

```
docker-compose up -d

docker logs <container_id>

docker ps -a 

```

```
pip install cassandra-driver

pip install spark pyspark

```

Google: maven repositoty -> https://mvnrepository.com -> search: spark cassandra connector 
-> https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector 


Google: maven repository -> https://mvnrepository.com -> search: spark sql kafka
-> https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10 

Google: spark kafka example -> https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html 

```
python realtime-data-streaming/spark-jobs/spark_stream.py

```

````
docker exec -it cassandra cqlsh -u cassandra -p casssandra localhost 9042

cassandra@cqlsh> SELECT * FROM spark_streams.created_users;

cassandra@cqlsh> SELECT first_name, last_name FROM spark_streams.created_users;


````

```
yazi
```


ruff extension for vscode

https://mypy.readthedocs.io/en/stable/running_mypy.html

```
spark-submit --master spark://localhost:7077 spark_stream.py
```

````
ps aux | grep cassandra

docker exec -it cassandra cqlsh -u cassandra -p cassandra localhost 9042

docker ps

docker inspect cassandra

docker logs cassandra

nc -vz localhost 9042
````

