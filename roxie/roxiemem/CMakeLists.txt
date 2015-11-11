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

# Component: roxiemem 
#####################################################
# Description:
# ------------
#    Cmake Input File for roxiemem
#####################################################

project( roxiemem ) 

set (    SRCS 
         roxiemem.cpp 
         roxierowbuff.cpp
    )
         
set (    INCLUDES
         roxiemem.hpp
         roxierowbuff.hpp
    )

include_directories (
         ./../../system/include
         ./../../system/jlib
         ./../../rtl/include
         ./../../testing/unittests
    )

ADD_DEFINITIONS( -D_USRDLL -DROXIEMEM_EXPORTS)

HPCC_ADD_LIBRARY( roxiemem SHARED ${SRCS} ${INCLUDES})
if (NOT MAKE_R_ONLY)
  install ( TARGETS roxiemem RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
endif()
target_link_libraries ( roxiemem
         jlib
         ${CPPUNIT_LIBRARIES}
    )


