#!/bin/bash

# clean up old images

[[ "$1" == "-all" ]] && docker rm $(docker ps -q -f 'status=exited')
docker rmi $(docker images -q -f "dangling=true")

HEAD=$(git rev-parse --short HEAD)
PREV=$(git describe --abbrev=0 --tags)
for f in `docker images  --format "{{.Repository}}:{{.Tag}}" | grep hpccsystems/ | grep -v $HEAD | grep -v $PREV` ; do
  docker rmi $f
done
