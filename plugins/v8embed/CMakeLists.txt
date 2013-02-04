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


# Component: v8embed

#####################################################
# Description:
# ------------
#    Cmake Input File for v8embed
#####################################################

project( v8embed )

set (    SRCS
         v8embed.cpp
    )

find_package(V8)
if (NOT V8_FOUND)
  message(FATAL_ERROR "MAKE_V8EMBED requested but v8 not found")
endif()


include_directories (
         ${V8_INCLUDE_DIR}
         ./../../system/include
         ./../../rtl/eclrtl
         ./../../rtl/include
         ./../../common/deftype
         ./../../system/jlib
    )

ADD_DEFINITIONS( -D_USRDLL -DV8EMBED_EXPORTS )

HPCC_ADD_LIBRARY( v8embed SHARED ${SRCS} )
if (${CMAKE_VERSION} VERSION_LESS "2.8.9")
  message("WARNING: Cannot set NO_SONAME. shlibdeps will give warnings when package is installed")
else()
  set_target_properties( v8embed PROPERTIES NO_SONAME 1 )
endif()

install ( TARGETS v8embed DESTINATION plugins )

target_link_libraries ( v8embed
    ${V8_LIBRARIES}
    eclrtl
    jlib
    )
