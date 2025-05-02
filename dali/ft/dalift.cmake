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

# Component: dalift 
#####################################################
# Description:
# ------------
#    Cmake Input File for dalift
#####################################################

project( dalift ) 

set (    SRCS 
         daft.cpp 
         daftformat.cpp 
         daftmc.cpp 
         daftprogress.cpp 
         filecopy.cpp 
         ftbase.cpp 
         fttransform.cpp 
    )

include_directories ( 
         ${HPCC_SOURCE_DIR}/common/remote
         ${HPCC_SOURCE_DIR}/dali/base
         ${HPCC_SOURCE_DIR}/common/deftype
         ${HPCC_SOURCE_DIR}/ecl/hql
         ${HPCC_SOURCE_DIR}/fs/dafsclient
         ${HPCC_SOURCE_DIR}/rtl/eclrtl
         ${HPCC_SOURCE_DIR}/rtl/include
         ${HPCC_SOURCE_DIR}/system/jhtree
         ${HPCC_SOURCE_DIR}/system/jlib
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/system/mp
         ${HPCC_SOURCE_DIR}/system/security/shared
    )

if (NOT CONTAINERIZED)
    include_directories ( ${HPCC_SOURCE_DIR}/common/environment )
endif()

HPCC_ADD_LIBRARY( dalift SHARED ${SRCS} )
set_target_properties (dalift PROPERTIES 
    COMPILE_FLAGS -D_CONSOLE
    DEFINE_SYMBOL DALIFT_EXPORTS
    )
install ( TARGETS dalift RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( dalift 
         jlib
         eclrtl
         jhtree
         hql
         mp 
         hrpc 
         dafsclient
         dalibase 
    )

if (NOT CONTAINERIZED)
    target_link_libraries ( dalift environment )
endif()

