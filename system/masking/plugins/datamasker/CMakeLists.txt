################################################################################
#    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems.
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

# Component: datamasker
#####################################################
# Description:
# ------------
#	 Cmake Input File for datamasker
#####################################################

project( datamasker)

set ( SRCS
    datamaskerplugin.cpp
)

include_directories (
    ${HPCC_SOURCE_DIR}/system/include
    ${HPCC_SOURCE_DIR}/system/jlib
    ./../../include
)

ADD_DEFINITIONS( -D_USRDLL -DDATAMASKER_EXPORTS )

if (RELEASE_BUILD EQUAL 1)
    ADD_DEFINITIONS( -DISC_NO_MAIN)
endif (RELEASE_BUILD EQUAL 1)

HPCC_ADD_LIBRARY( datamasker SHARED ${SRCS} )

install ( 
    TARGETS datamasker 
    RUNTIME DESTINATION ${EXEC_DIR} 
    LIBRARY DESTINATION ${LIB_DIR} 
    COMPONENT Runtime
)

target_link_libraries ( datamasker
    jlib
)
