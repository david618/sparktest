from centos:7

RUN yum -y install java-1.8.0-openjdk 

# curl -O https://archive.apache.org/dist/spark/spark-2.3.2/spark-2.3.2-bin-hadoop2.7.tgz
# tar gunzip spark-2.3.2-bin-hadoop2.7.tgz
ADD ./dockerfiles/spark-2.3.2-bin-hadoop2.7.tar /opt/


ADD ./target/dependency-jars/  /opt/dependency-jars/
ADD ./target/sparktest.jar /opt/


