################################################################################
#    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.
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

# XRef Libraries
#######################################################
# Description:
# ------------
#           Cmake file for XRef libraries
#
########################################################

project (xref)

set (SRCS
    xref.cpp
    xref.hpp
    )

include_directories (
         .
         ..
         ${HPCC_SOURCE_DIR}/dali/base
         ${HPCC_SOURCE_DIR}/system/mp
         ${HPCC_SOURCE_DIR}/system/jlib
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${HPCC_SOURCE_DIR}/fs/dafsclient
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
         ${HPCC_SOURCE_DIR}/testing/unittests
    )

ADD_DEFINITIONS( -D_USRDLL -DXREF_EXPORTS )

HPCC_ADD_LIBRARY( xref SHARED ${SRCS} )

install ( TARGETS xref RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR})

target_link_libraries ( xref
         jlib
         dafsclient
         dalibase
         mp
         hrpc
    )

if (NOT CONTAINERIZED)
    target_link_libraries ( xref environment )
endif()

if (USE_CPPUNIT)
    set (UNITTEST_SRCS
        xrefunittests.cpp
        )

    HPCC_ADD_LIBRARY( xrefunittests SHARED ${UNITTEST_SRCS} )

    install ( TARGETS xrefunittests LIBRARY DESTINATION ${LIB_DIR})

    target_link_libraries ( xrefunittests
         xref
         jlib
         ${CppUnit_LIBRARIES}
    )

endif()
