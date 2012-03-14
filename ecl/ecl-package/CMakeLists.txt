##############################################################################
# Copyright (C) 2011 HPCC Systems.
##############################################################################


# Component: ecl-package
#####################################################
# Description:
# ------------
#    Cmake Input File for ecl-package
#####################################################


project( ecl-package )

include(${HPCC_SOURCE_DIR}/esp/scm/espscm.cmake)

set (    SRCS
         ${ESPSCM_GENERATED_DIR}/ws_packageprocess_esp.cpp
         ecl-package.cpp
         ${HPCC_SOURCE_DIR}/ecl/eclcmd/eclcmd_shell.cpp
         ${HPCC_SOURCE_DIR}/ecl/eclcmd/eclcmd_common.hpp
         ${HPCC_SOURCE_DIR}/ecl/eclcmd/eclcmd_common.cpp
    )

include_directories (
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/system/jlib
         ${HPCC_SOURCE_DIR}/common/workunit
         ${HPCC_SOURCE_DIR}/esp/clients
         ${HPCC_SOURCE_DIR}/esp/bindings
         ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/xpp
         ${HPCC_SOURCE_DIR}/esp/platform
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/system/xmllib
         ${HPCC_SOURCE_DIR}/ecl/eclcmd
    )

ADD_DEFINITIONS( -D_CONSOLE )

add_executable ( ecl-package ${SRCS} )
add_dependencies ( ecl-package espscm ws_packageprocess )
install ( TARGETS ecl-package DESTINATION ${DIR_NAME}/bin )
target_link_libraries ( ecl-package
        jlib
        esphttp
        workunit
    )

install ( PROGRAMS ecl-package.install DESTINATION ${DIR_NAME}/etc/init.d/install COMPONENT Runtime )

