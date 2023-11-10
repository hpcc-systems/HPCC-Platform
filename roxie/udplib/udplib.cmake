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

# Component: udplib 

#####################################################
# Description:
# ------------
#    Cmake Input File for udplib
#####################################################


project( udplib ) 

set (    SRCS 
         udpmsgpk.cpp 
         udpsha.cpp 
         udptrr.cpp 
         udptrs.cpp
	 udptopo.cpp
	 udpipmap.cpp
    )

include_directories ( 
         ./../../roxie/roxiemem 
         ./../../system/include 
         ./../../system/jlib 
         ./../../roxie/ccd
         ${HPCC_SOURCE_DIR}/testing/unittests
         ./../../roxie/roxie
    )

HPCC_ADD_LIBRARY( udplib SHARED ${SRCS} )
set_target_properties( udplib PROPERTIES 
    COMPILE_FLAGS -D_USRDLL
    DEFINE_SYMBOL UDPLIB_EXPORTS )
install ( TARGETS udplib RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )

target_link_libraries ( udplib 
         jlib
         roxiemem 
    )
