################################################################################
#    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems.
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

# Component: spookyhash

#####################################################
# Description:
# ------------
#    Cmake Input File for spookyhash
#####################################################


project( spookyhash )

set ( SRCS
        spookyhash.c
        context.c
        globals.c
)

ADD_DEFINITIONS( -D_LIB )

SET_SOURCE_FILES_PROPERTIES( ${SRCS} PROPERTIES LANGUAGE C )

if (CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_CLANG)
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC -std=c99")
    set (CMAKE_C_FLAGS_RELWITHDEBINFO "${CMAKE_C_FLAGS_RELWITHDEBINFO} -Wall -O3")
endif()

HPCC_ADD_LIBRARY( spookyhash STATIC ${SRCS} )
