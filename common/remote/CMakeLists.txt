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


# Component: remote 
#####################################################
# Description:
# ------------
#    Cmake Input File for remote
#####################################################

project( remote ) 

HPCC_ADD_SUBDIRECTORY(hooks)

set (    SRCS 
         rmtfile.cpp 
         rmtpass.cpp 
         rmtspawn.cpp 
         rmtssh.cpp 
         rmtsmtp.cpp 
         sockfile.cpp
         
         remoteerr.hpp
         rmtfile.hpp
         rmtpass.hpp
         rmtsmtp.hpp
         rmtspawn.hpp
         rmtssh.hpp
         sockfile.hpp
    )
if (USE_URIPARSER)
  set ( SRCS
        ${SRCS}
        uri.cpp
        uri.hpp
      )
endif(USE_URIPARSER)

include_directories (
         ./../../system/hrpc 
         ./../../system/mp 
         ./../../system/include 
         ./../../system/jlib 
    )

ADD_DEFINITIONS( -D_USRDLL -DREMOTE_EXPORTS )

HPCC_ADD_LIBRARY( remote SHARED ${SRCS}  )
install ( TARGETS remote RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )

target_link_libraries ( remote 
    jlib 
    mp
    ${URIPARSER_LIBRARIES}
    )
