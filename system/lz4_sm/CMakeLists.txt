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

# Component: lz4

#####################################################
# Description:
# ------------
#    Cmake Input File for lz4
#####################################################


project( lz4 )

set ( SRCS
        lz4/lib/lz4.c
        lz4/lib/lz4hc.c
)

include_directories (
        lz4/lib
)

ADD_DEFINITIONS( -D_LIB )

SET_SOURCE_FILES_PROPERTIES( ${SRCS} PROPERTIES LANGUAGE C )

if (CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_CLANG)
  set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC -std=c99")
  remove_definitions (-fsanitize=undefined -fno-sanitize=alignment -fsanitize-trap=undefined)
endif()

HPCC_ADD_LIBRARY( lz4 STATIC ${SRCS} )
