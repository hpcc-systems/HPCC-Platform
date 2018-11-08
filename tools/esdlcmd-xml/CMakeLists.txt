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

# Component: esdlcmd-xml

#####################################################
# Description:
# ------------
#    Cmake Input File for esdlcmd-xml
#####################################################

project( esdl-xml )

set ( SRCS
	${CMAKE_CURRENT_SOURCE_DIR}/esdl2xml.cpp
	${CMAKE_CURRENT_SOURCE_DIR}/../esdlcmd/esdlcmdutils.cpp
)

include_directories (
	${CMAKE_CURRENT_BINARY_DIR}   # for generated .h file
	${HPCC_SOURCE_DIR}/system/include
	${HPCC_SOURCE_DIR}/system/jlib
	${HPCC_SOURCE_DIR}/tools/esdlcomp
	${HPCC_SOURCE_DIR}/tools/esdlcmd
	${CMAKE_CURRENT_SOURCE_DIR}
)

ADD_DEFINITIONS( -D_CONSOLE )
IF (NOT WIN32)
	ADD_DEFINITIONS( -O0 )
ENDIF()

HPCC_ADD_EXECUTABLE ( esdl-xml ${SRCS} )

MESSAGE("----INSTALLING ESDL-XML")
install ( TARGETS esdl-xml RUNTIME DESTINATION ${EXEC_DIR} )
if ( UNIX )
    install ( PROGRAMS esdl-xml.install DESTINATION etc/init.d/install COMPONENT Runtime )
    install ( PROGRAMS esdl-xml.uninstall DESTINATION etc/init.d/uninstall COMPONENT Runtime )
endif ( UNIX )

target_link_libraries ( esdl-xml
	jlib
	esdlcomp
)
