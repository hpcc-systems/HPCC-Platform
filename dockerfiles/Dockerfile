# This dockerfile is used to run the buildall script (which builds all docker images) from inside its
# own Docker image, for use in automated github workflows.
# If building manually, it's simpler just to run buildall.sh directly

FROM docker:19.03.2 as runtime

RUN apk update \
  && apk upgrade \
  && apk add --no-cache git bash

ENTRYPOINT ["dockerfiles/buildall.sh"]

