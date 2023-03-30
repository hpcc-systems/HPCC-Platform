ARG DOCKER_REPO=hpccsystems
ARG IMAGE_NAME=platform-core
ARG IMAGE_TAG=9.0.0-rc3-Debug
ARG BUILD_IMAGE
FROM ${BUILD_IMAGE} as build
FROM ${DOCKER_REPO}/${IMAGE_NAME}:${IMAGE_TAG} 

COPY --from=build /opt/HPCCSystems /opt/HPCCSystems
