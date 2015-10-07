################################################################################
#    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems.
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


# Check if environment variable QTDIR is set.
IF( CONFIGURATOR_QT_UI AND DEFINED ENV{QTDIR} )
    SET( CMAKE_PREFIX_PATH $ENV{QTDIR} ${CMAKE_PREFIX_PATH} )
    SET( Qt5_INCLUDE_DIRS ${CMAKE_PREFIX_PATH}/include )
    SET( Qt5_LIB_DIR ${CMAKE_PREFIX_PATH}/lib )
    SET( QT_MODULE_PATH ${CMAKE_PREFIX_PATH}/lib/cmake)
    SET( CMAKE_MODULE_PATH ${CONFIGURATOR_MODULE_PATH}  ${QT_MODULE_PATH} )

    MESSAGE( "Set CMAKE_MODULE_PATH to ${CMAKE_MODULE_PATH}" )
    MESSAGE( "-- Qt5_INCLUDE_DIRS is set to ${Qt5_INCLUDE_DIRS}" )
    MESSAGE( "-- Qt5_LIB_DIR is set to ${Qt5_LIB_DIR}" )

    HPCC_ADD_SUBDIRECTORY (configurator_ui)
    HPCC_ADD_SUBDIRECTORY (configurator_app)

ELSEIF ( CONFIGURATOR_QT_UI )
    MESSAGE( FATAL_ERROR "ERROR: Environment variable QTDIR is not set. Please locate your Qt folder and set QTDIR.")
ENDIF()


HPCC_ADD_SUBDIRECTORY (configurator)
