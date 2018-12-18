from centos:7

# Image include sparktest and java only
# When running you'll need to mount HOST volume /opt/mesosphere
# docker build . -t david62243/sparktest

RUN yum -y install java-1.8.0-openjdk

RUN curl https://archive.apache.org/dist/spark/spark-2.3.2/spark-2.3.2-bin-hadoop2.7.tgz | tar -C /opt -zx

ADD ./target/dependency-jars/  /opt/dependency-jars/
ADD ./target/sparktest.jar /opt/


ENV MESOS_NATIVE_JAVA_LIBRARY /opt/mesosphere/lib/libmesos.so
