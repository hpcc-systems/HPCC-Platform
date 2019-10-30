
## Prerequisites
Tested on Ubuntu 18.04.
Need ubuntu 18.04 update 4 or above

Install git
```sh
sudo apt-get install -y git
```


Install Docker CE
https://docs.docker.com/install/linux/docker-ce/ubuntu/#set-up-the-repository


## Create Docker Swarm
```sh
sudo docker swarm init --advertise-addr <ip>
```
&lt;ip&gt; is one of the ip on this Linux
For example, for Virtualbox it can be host-only or bridged ip or docker0 ip


To list node joined:
```sh
sudo docker node ls
```

##  Get the App
Get HPCC-Docker-Stack to your host Linux:
```sh
git clone https://github.com/hpcc-systems/hpcc-docker
```

##  Run HPCC Cluster

### Start a HPCC Cluster
cd to HPCC-Docker-Stack and run
```sh
sudo docker stack deploy -c docker-stack.yml hpcc
```
"hpcc" is the app name. You can give other name.

Wait for all containers start...
```sh
sudo docker service ls
```
All service with prefix "hpcc_" should be started by checking "REPLICAS"  column.

To configure the cluster to go bin/:
```sh
./cluster_config.sh
```
If it deploys successfully it should print all HPCC node's running status
To access ECLWatch: http://localhost:8010

There are several help scripts under bin/
### cluster_config.sh
Configure HPCC Cluster, setup ansible hosts file and stop/start HPCC cluster

### cluster_query.sh
Query entries of Docker/HPCC cluster
For example list all components defined in environmentx.ml:
```sh
./cluster_query -q comp -c .*
```
Get node ips of HPCC cluster:
```sh
./cluster_query -q ip -g HPCC
```
Get docker container id from ip
```sh
./cluster_query -q id -p <ip>
```
Get docker container id for admin node
```sh
./cluster_query -q id -g admin
```

You can use &lt;id&gt; to access the container:
```sh
sudo docker exec -it <id> /bin/bash
```
Just type "exit" to return the host


### cluster_run.sh
This script is for start/stop/restart HPCC as well as query status.
It also can start configmgr.

To start/stop/restart or show status HPCC cluster:
```sh
./cluster_run.sh [start|stop|restart|status]
```
To performan above action for particlar node give contianer id or ip:
```sh
./cluster_run.sh -p <ip> [start|stop|restart|status]

./cluster_run.sh -id <id> [start|stop|restart|status]
```

To performace an action for a component:
```sh
./cluster_run.sh -p <ip> -c <comp name> [start|stop|restart|status]
```
For example
```sh
./cluster_run.sh -p <ip> -c mydali status
```


To start configmgr:
```sh
./cluster_run.sh -c configmgr start
```
Type Ctrl C to stop it

Replace "hpcc" with your app name if is different.

### cluster_env.sh
This will :
   stop HPCC cluster
   transfer /etc/HPCCSystems/source/environment.xml on admin node to all HPCC cluster nodes
   start HPCC cluster


### Stop a HPCC Cluster
cd to HPCC-Docker-Stack and run
```sh
sudo docker stack rm hpcc
```