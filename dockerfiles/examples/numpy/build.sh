label=cca7dcd044-Debug-dirty-1ab275a72e006dca02c010ef5f22d4db

docker image build -t hpccsystems/platform-core:numpy \
     --build-arg DOCKER_REPO=hpccsystems \
     --build-arg BUILD_LABEL=${label} \
     addnumpy/ 

build_numpy_image() {
   docker image build -t hpccsystems/$1:numpy --no-cache \
     --build-arg DOCKER_REPO=hpccsystems \
     --build-arg BUILD_LABEL=numpy \
     ~/HPCC-Platform/dockerfiles/$1/ 
}

build_nonumpy_image() {
   docker image build -t hpccsystems/$1:numpy \
     --build-arg DOCKER_REPO=hpccsystems \
     --build-arg BUILD_LABEL=${label}\
     ~/HPCC-Platform/dockerfiles/$1/ 
}

for f in roxie eclagent hthor thormaster thorslave ; do build_numpy_image $f  ; done

for f in dali esp eclccserver toposerver ; do build_nonumpy_image $f ; done

