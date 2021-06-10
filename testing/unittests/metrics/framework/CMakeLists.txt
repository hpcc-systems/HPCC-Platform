################################################################################
#
#    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.
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

if (USE_CPPUNIT)

    project( metricframeworkunittests )

    set (    SRCS
            MetricFrameworkTests.cpp
            )

    include_directories (
            .
            ..
            ${HPCC_SOURCE_DIR}/system/include
            ${HPCC_SOURCE_DIR}/system/jlib
            ${HPCC_SOURCE_DIR}/system/mp
            ${HPCC_SOURCE_DIR}/system/libbase58
            ${HPCC_SOURCE_DIR}/fs/dafsclient
            ${HPCC_SOURCE_DIR}/common/deftype
            ${CMAKE_BINARY_DIR}
            ${CMAKE_BINARY_DIR}/oss
    )

    ADD_DEFINITIONS( -D_CONSOLE )

    HPCC_ADD_EXECUTABLE ( metricframeworkunittests ${SRCS} )

    install ( TARGETS metricframeworkunittests RUNTIME DESTINATION ${EXEC_DIR} )
    target_link_libraries ( metricframeworkunittests
            jlib
            ${CPPUNIT_LIBRARIES}
            )
endif ()
