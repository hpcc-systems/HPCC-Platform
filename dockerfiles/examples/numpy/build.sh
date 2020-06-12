LABEL=$1
[[ -z ${LABEL} ]] && LABEL=latest

docker image build -t hpccsystems/platform-core:numpy \
     --build-arg DOCKER_REPO=hpccsystems \
     --build-arg BUILD_LABEL=${LABEL} \
     addnumpy/ 

