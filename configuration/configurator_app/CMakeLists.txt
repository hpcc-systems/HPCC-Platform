PROJECT ( configurator_app )

CMAKE_MINIMUM_REQUIRED( VERSION 2.8.11)

SET( CMAKE_INCLUDE_CURRENT_DIR ON )
SET( CMAKE_AUTOMOC ON )

FIND_PACKAGE( Qt5Core )
FIND_PACKAGE( Qt5Widgets )
FIND_PACKAGE( Qt5OpenGL )
FIND_PACKAGE( Qt5Quick )
FIND_PACKAGE( Qt5Qml )

QT5_WRAP_UI( UI_HDRS MainWindow.ui )

INCLUDE_DIRECTORIES(
        ${Qt5_INCLUDE_DIRS}
        ${Qt5_INCLUDE_DIRS}/QtGui
        ${Qt5_INCLUDE_DIRS}/QtWidgets
        ${Qt5_INCLUDE_DIRS}/QtCore
        ${Qt5_INCLUDE_DIRS}/QtQml
        ${HPCC_SOURCE_DIR}/configuration/configurator
        ${HPCC_SOURCE_DIR}/configuration/configurator_ui/
        ${HPCC_SOURCE_DIR}/system/jlib
        ${HPCC_SOURCE_DIR}/system/include
        ${HPCC_SOURCE_DIR}/deployment/deploy
 )

SET(    SRCS
        Main.cpp
        MainWindow.cpp
        Worker.cpp
        ComponentSelectorDialog.cpp
        ${HPCC_SOURCE_DIR}/configuration/configurator_ui/AppData.cpp
)

HPCC_ADD_EXECUTABLE ( configurator_app ${SRCS} ${UI_HDRS} )

TARGET_LINK_LIBRARIES( configurator_app
    jlib
    Qt5::Qml
    Qt5::Widgets
    Qt5::Quick
    ${Qt5Core_LIBRARIES}
    configurator
    configurator_ui
)
