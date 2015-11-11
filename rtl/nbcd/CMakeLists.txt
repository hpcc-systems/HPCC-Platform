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

# Component: nbcd 

#####################################################
# Description:
# ------------
#    Cmake Input File for nbcd
#####################################################


project( nbcd ) 

set ( SRCS 
      nbcd.cpp 

      nbcd.hpp 

      sourcedoc.xml
    )

include_directories ( 
      ./../../system/include 
      ./../../system/jlib
    )

ADD_DEFINITIONS( -D_USRDLL -DNBCD_EXPORTS )

HPCC_ADD_LIBRARY( nbcd SHARED ${SRCS} )
if (NOT MAKE_R_ONLY)
  install ( TARGETS nbcd RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} ARCHIVE DESTINATION componentfiles/cl/lib )
endif()
target_link_libraries ( nbcd 
      jlib
      ${CPPUNIT_LIBRARIES}
    )


