# Push Docker Image to Repositories

We currently push HPCC Systems Docker Images to three Docker Image Registries
- Docker Hub
- Gitlab (gitlab.ins.risk.regn.net)
- AWS Elatic Container Registry (ECR)


## Build/Tag/Push

### Build
```console
sudo  docker build -t <platform|clienttools>:<tag> --build-arg version=<version> .
```

```console
#sudo  docker tag <platform|clienttools>:<tag> <Docker Image Registry Repository>:<tag>
#for example,
sudo  docker tag hpccsystems/platform:7.4.24-1 446598291512.dkr.ecr.us-east-2.amazonaws.com/hpccsystems/platform:7.4.24-1
```

### Push
```console
sudo  docker push <Docker Image Registry Repository>:<tag>
```
## Docker Hub
### Login
```console
sudo docker login -u <user> -p <password>
```

### Push
```console
sudo docker push hpccsystems/<platform|clienttools|hpcc-admin>:<tag>
```


## Gitlab

### Login
```console
sudo docker login -u <user> -p <password> gitlab.ins.risk.regn.net:4567
```

### Push
```console
sudo docker push gitlab.ins.risk.regn.net:4567/docker-images/hpccsystems/ln-platform-wp|ln-clienttools>:<tag>
```


## Elastic Container Registry (ECR)

ECR is regional service not global service. Pulling images cross regions is possible but will have additional latency.

### Login
```console
sudo $(aws ecr get-login --no-include-email --region us-east-2)

```

### Push
```console
#sudo docker push <aws account id>.dkr.ecr.us-east-2.amazonaws.com/hpcc-systems-platform:<tag>
#for example,
sudo  docker push 446598291512.dkr.ecr.us-east-2.amazonaws.com/hpccsystems/platform:7.4.24-1
```
