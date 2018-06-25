################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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


# Component: envgen2
#####################################################
# Description:
# ------------
#    Cmake Input File for envgen2
#####################################################

project( envgen2 )

HPCC_ADD_SUBDIRECTORY (test "PLATFORM")

set ( INCLUDES
      EnvGen.hpp
    )
set ( SRCS EnvGen.cpp )

include_directories (
         .
         ./../deploy
         ./../../system/include
         ./../../system/jlib
         ./../../system/mp
         ./../configenv/
         ./../configenv/common
         ./../configenv/xml_jlibpt
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
    )

ADD_DEFINITIONS ( -D_CONSOLE )

HPCC_ADD_EXECUTABLE ( envgen2 ${SRCS} )
install ( TARGETS envgen2 DESTINATION sbin )
target_link_libraries ( envgen2
        jlib
        configenv
        )
