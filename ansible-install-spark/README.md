# Installing Spark Cluster

This Ansible script can be used to install spark.

## Edit hosts

Edit the hosts file and add the ip or hostnames of the machines for the Spark master and slaves. 

**Note:** Only specify one Spark master.  

## Install Ansible

```sudo yum -y install ansible``` 

## Install Spark

You can edit the script if needed.  Right now it's configured to install spark 2.3.2

Run playbook

```ansible-playbook --private-key /home/azureuser/az -i hosts playbooks/install_spark.yaml```


The playbook
- Basition
  - Download Spark Bundle
- Master and Slaves
  - Creates a spark user
  - Installs Java
  - Copies Spark Bundle
  - Creates Spark Folder (/opt/spark)
  - Uncompresses Spark
  - Copies log4j.properties and puts it in place
  - Makes spark user owner of /opt/spark 


## Start/Stop Spark

Start:  ```ansible-playbook --private-key /home/azureuser/az -i hosts playbooks/start_spark.yaml```
Stop:  ```ansible-playbook --private-key /home/azureuser/az -i hosts playbooks/start_stop.yaml```
  
  
## Copy sparktest to Spark Workers


## Submit Job to Spark


Secure shell to one of the spark nodes and become the spark user.

```
ssh -i az a1
sudo su - spark
```

### Examples

KafkaToKafka
```
/opt/spark/bin/spark-submit \
  --master spark://m1:7077 \
  --conf spark.executor.extraClassPath="/home/spark/sparktest-jar-with-dependencies.jar" \
  --driver-class-path "/home/spark/sparktest-jar-with-dependencies.jar" \
  --conf spark.driver.extraJavaOptions=-Dlog4j.configurationFile=/home/spark/log4j2conf.xml \
  --conf spark.executor.extraJavaOptions=-Dlog4j.configurationFile=/home/spark/log4j2conf.xml \
  --conf spark.executor.memory=4000m \
  --conf spark.executor.cores=4 \
  --conf spark.cores.max=48 \
  --conf spark.streaming.concurrentJobs=64 \
  --conf spark.scheduler.mode=FAIR \
  --conf spark.locality.wait=0s \
  --conf spark.streaming.kafka.consumer.cache.enabled=false \
  --conf spark.cassandra.output.batch.size.rows=auto \
  --conf spark.cassandra.output.concurrent.writes=200 \
  --class org.jennings.estest.KafkaToKafka \
  /home/spark/sparktest-jar-with-dependencies.jar broker.hub-gw01.l4lb.thisdcos.directory:9092 planes  broker.hub-gw01.l4lb.thisdcos.directory:9092 planes-out spark://m1:7077 1
```

SendFileKafka
```
 /opt/spark/bin/spark-submit \
  --master spark://m1:7077 \
  --conf spark.executor.extraClassPath="/home/spark/sparktest-jar-with-dependencies.jar" \
  --driver-class-path "/home/spark/sparktest-jar-with-dependencies.jar" \
  --conf spark.driver.extraJavaOptions=-Dlog4j.configurationFile=/home/spark/log4j2conf.xml \
  --conf spark.executor.extraJavaOptions=-Dlog4j.configurationFile=/home/spark/log4j2conf.xml \
  --conf spark.executor.memory=4000m \
  --conf spark.executor.cores=4 \
  --conf spark.cores.max=48 \
  --conf spark.streaming.concurrentJobs=64 \
  --conf spark.scheduler.mode=FAIR \
  --conf spark.locality.wait=0s \
  --conf spark.streaming.kafka.consumer.cache.enabled=false \
  --conf spark.cassandra.output.batch.size.rows=auto \
  --conf spark.cassandra.output.concurrent.writes=200 \
  --class org.jennings.estest.SendFileKafka \
  /home/spark/sparktest-jar-with-dependencies.jar /home/spark/planes00001 a1:9092 planes spark://m1:7077 10
```

