################################################################################
#    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.
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
################################################################################

project(hpccmetrics_prometheus)

set ( srcs
      prometheusSink.cpp
    )

set ( INCLUDES
      ${HPCC_SOURCE_DIR}/system/httplib/httplib.h
    )

include_directories(
        ${HPCC_SOURCE_DIR}/system/include
        ${HPCC_SOURCE_DIR}/system/jlib
        ${HPCC_SOURCE_DIR}/system/httplib
)

ADD_DEFINITIONS(  -DPROMETHEUSSINK_EXPORTS )
HPCC_ADD_LIBRARY( hpccmetrics_prometheussink SHARED ${srcs} )
TARGET_LINK_LIBRARIES( hpccmetrics_prometheussink
                       jlib
                     )

INSTALL ( TARGETS hpccmetrics_prometheussink RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
