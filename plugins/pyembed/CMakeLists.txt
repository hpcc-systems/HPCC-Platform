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


# Component: pyembed

#####################################################
# Description:
# ------------
#    Cmake Input File for pyembed
#####################################################

set ( toolsdir "${HPCC_SOURCE_DIR}/tools" )
set ( debug_python Off )   # A lot slower but can assist in debugging...

project( pyembed )

set (    SRCS
         pyembed.cpp
    )

include_directories (
         /usr/include/python2.7
         ./../../system/include
         ./../../rtl/eclrtl
         ./../../rtl/include
         ./../../system/jlib
    )

ADD_DEFINITIONS( -D_USRDLL -DPYEMBED_EXPORTS )
if (debug_python)
  ADD_DEFINITIONS(-DPy_DEBUG)
endif()

HPCC_ADD_LIBRARY( pyembed SHARED ${SRCS} )
install ( TARGETS pyembed DESTINATION plugins )

if (debug_python)
  target_link_libraries ( pyembed python2.7_d )
else()
  target_link_libraries ( pyembed python2.7 )
endif()

target_link_libraries ( pyembed
    eclrtl
    jlib
    )
