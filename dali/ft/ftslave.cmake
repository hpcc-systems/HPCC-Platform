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


# Component: ftslave 
#####################################################
# Description:
# ------------
#    Cmake Input File for ftslave
#####################################################

project( ftslave ) 

set (    SRCS 
         ftslave.cpp 
    )

include_directories ( 
         ${HPCC_SOURCE_DIR}/include 
         ${HPCC_SOURCE_DIR}/jlib
         ${HPCC_SOURCE_DIR}/dali/base 
         ${HPCC_SOURCE_DIR}/mp 
         ${HPCC_SOURCE_DIR}/remote 
         ${HPCC_SOURCE_DIR}/security/shared
    )

HPCC_ADD_EXECUTABLE ( ftslave ${SRCS} )
set_target_properties (ftslave PROPERTIES COMPILE_FLAGS -D_CONSOLE)
install ( TARGETS ftslave RUNTIME DESTINATION ${EXEC_DIR} )
target_link_libraries ( ftslave
         dalibase 
         dalift 
         ftslavelib
         hrpc 
         jlib
         mp 
         remote 
    )

if (NOT CONTAINERIZED)
    target_link_libraries ( ftslave environment )
endif()
