CMAKE_MINIMUM_REQUIRED( VERSION 2.8.11 )

PROJECT(configurator_ui)

SET( SRCS AppData.cpp ConfiguratorUI.cpp )

# Find includes in corresponding build directories
SET(CMAKE_INCLUDE_CURRENT_DIR ON)
# Instruct CMake to run moc automatically when needed.
SET(CMAKE_AUTOMOC ON)

INCLUDE_DIRECTORIES(
        ${Qt5_INCLUDE_DIRS}
        ${Qt5_INCLUDE_DIRS}/QtCore
        ${Qt5_INCLUDE_DIRS}/QtGui
        ${Qt5_INCLUDE_DIRS}/QtQml
        ${HPCC_SOURCE_DIR}/configuration/configurator
        ${HPCC_SOURCE_DIR}/configuration/configurator_app
        ${HPCC_SOURCE_DIR}/system/jlib
        ${HPCC_SOURCE_DIR}/system/include
	${HPCC_SOURCE_DIR}/deployment/deploy

)

ADD_DEFINITIONS( -D_USRDLL )
HPCC_ADD_LIBRARY ( configurator_ui  ${SRCS} )

