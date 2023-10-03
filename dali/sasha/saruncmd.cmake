################################################################################
#    HPCC SYSTEMS software Copyright (C) 2023 HPCC Systems®.
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


# Component: saruncmdlib
#####################################################
# Description:
# ------------
#    Cmake Input File for saruncmdlib
#####################################################

project( saruncmdlib )

set (    SRCS
         sacmd.cpp
         saruncmd.cpp
    )

include_directories (
         .
         ${HPCC_SOURCE_DIR}/system/mp
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/system/jlib
    )

ADD_DEFINITIONS ( -D_USRDLL -DSASHACLI_API_EXPORTS )

HPCC_ADD_LIBRARY( saruncmdlib SHARED ${SRCS} )
install ( TARGETS saruncmdlib RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( saruncmdlib
         jlib
         mp
    )

