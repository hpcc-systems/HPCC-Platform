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

# Component: eclrtl 

#####################################################
# Description:
# ------------
#    Cmake Input File for eclrtl
#####################################################


project( eclrtl ) 

set (    SRCS 
         eclrtl.cpp 
         rtldistr.cpp 
         rtlds.cpp 
         rtlint.cpp 
         rtlkey.cpp 
         rtlqstr.cpp 
         rtlrank.cpp 
         rtlfield.cpp 
         rtlread.cpp 
         rtltype.cpp 
         rtlxml.cpp 
         workflow.cpp 
    )

include_directories ( 
         ./../../rtl/include 
         ./../../rtl/nbcd 
         ./../../system/include 
         ./../../common/workunit
         ./../../system/jlib 
         ${ICU_INCLUDE_DIR}
         ${BOOST_REGEX_INCLUDE_DIR}
    )

ADD_DEFINITIONS( -D_USRDLL -DECLRTL_EXPORTS )

if (WIN32)
else ()
    ADD_DEFINITIONS( -DBOOST_DYN_LINK )
endif ()

HPCC_ADD_LIBRARY( eclrtl SHARED ${SRCS} )
install ( TARGETS eclrtl RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} ARCHIVE DESTINATION componentfiles/cl/lib )
target_link_libraries ( eclrtl 
      ${BOOST_REGEX_LIBRARIES}
      ${ICU_LIBRARIES}
      jlib
      nbcd 
    )

FOREACH( iFILES
    ${CMAKE_CURRENT_SOURCE_DIR}/eclinclude.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/eclrtl.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/eclrtl_imp.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rtldistr.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rtlds_imp.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rtlfield_imp.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rtlkey.hpp
)
    Install ( FILES ${iFILES} DESTINATION componentfiles/cl/include COMPONENT Runtime )
ENDFOREACH ( iFILES )

