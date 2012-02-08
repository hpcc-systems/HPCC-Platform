##############################################################################
# Copyright (C) 2012 HPCC Systems.
##############################################################################


# Component: ecl-queries
#####################################################
# Description:
# ------------
#    Cmake Input File for ecl-queries
#####################################################


project( ecl-queries )

include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

set (    SRCS
         ${ESPSCM_GENERATED_DIR}/ws_workunits_esp.cpp
         ecl-queries.cpp
         ../eclcmd_shell.cpp
         ../eclcmd_common.hpp
         ../eclcmd_common.cpp
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

add_executable ( ecl-queries ${SRCS} )
add_dependencies ( ecl-queries espscm ws_workunits )
install ( TARGETS ecl-queries DESTINATION ${DIR_NAME}/bin )
target_link_libraries ( ecl-queries
        jlib
        esphttp
        workunit
    )

install ( PROGRAMS ecl-queries.install DESTINATION ${OSSDIR}/etc/init.d/install COMPONENT Runtime )
