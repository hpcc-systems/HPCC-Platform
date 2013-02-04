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


# Component: Rembed

#####################################################
# Description:
# ------------
#    Cmake Input File for Rembed
#####################################################

project( Rembed )

find_package(R)
if (NOT R_FOUND)
  message(FATAL_ERROR "MAKE_REMBED requested but R libraries not found")
endif()

set (    SRCS
         Rembed.cpp
    )

include_directories (
         "${R_INCLUDE_DIRS}"
         ./../../system/include
         ./../../rtl/eclrtl
         ./../../rtl/include
         ./../../common/deftype
         ./../../system/jlib
    )

ADD_DEFINITIONS( -D_USRDLL -DREMBED_EXPORTS )

HPCC_ADD_LIBRARY( Rembed SHARED ${SRCS} )
if (${CMAKE_VERSION} VERSION_LESS "2.8.9")
  message("WARNING: Cannot set NO_SONAME. shlibdeps will give warnings when package is installed")
else()
  set_target_properties( Rembed PROPERTIES NO_SONAME 1 )
endif()

install ( TARGETS Rembed DESTINATION plugins )

target_link_libraries ( Rembed
    ${R_LIBRARIES}
    eclrtl
    jlib
    )
