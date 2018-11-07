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


## Start/Stop spark

Start:  ```ansible-playbook --private-key /home/azureuser/az -i hosts playbooks/start_spark.yaml```
Stop:  ```ansible-playbook --private-key /home/azureuser/az -i hosts playbooks/start_stop.yaml```
  
  
  
