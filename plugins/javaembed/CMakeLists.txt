################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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


# Component: javaembed

#####################################################
# Description:
# ------------
#    Cmake Input File for javaembed
#####################################################

set ( toolsdir "${HPCC_SOURCE_DIR}/tools" )

project( javaembed )

find_package(JNI)

set (    SRCS
         javaembed.cpp
    )

include_directories (
         ${JNI_INCLUDE_DIRS}
         ./../../system/include
         ./../../rtl/eclrtl
         ./../../rtl/include
         ./../../system/jlib
    )

ADD_DEFINITIONS( -D_USRDLL -DJAVAEMBED_EXPORTS )

HPCC_ADD_LIBRARY( javaembed SHARED ${SRCS} )
install ( TARGETS javaembed DESTINATION plugins )

target_link_libraries ( javaembed
    ${JNI_LIBRARIES}
    eclrtl
    jlib
    )
