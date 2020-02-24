##############################################################################
#
#    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
##############################################################################

# This dockerfile is used to run the buildall script (which builds all docker images) from inside its
# own Docker image, for use in automated github workflows.
# If building manually, it's simpler just to run buildall.sh directly

FROM docker:19.03.2 as runtime

RUN apk update \
  && apk upgrade \
  && apk add --no-cache git bash

ENTRYPOINT ["dockerfiles/buildall.sh"]

