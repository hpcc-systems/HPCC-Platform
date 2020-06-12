LABEL=$1
[[ -n ${LABEL} ]] && LABEL=latest

docker image build -t hpccsystems/platform-core:numpy \
     --build-arg DOCKER_REPO=hpccsystems \
     --build-arg BUILD_LABEL=${LABEL} \
     addnumpy/ 

