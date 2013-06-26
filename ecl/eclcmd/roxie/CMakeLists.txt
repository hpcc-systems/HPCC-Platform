##############################################################################
# Copyright (C) 2012 HPCC Systems.
##############################################################################


# Component: ecl-roxie
#####################################################
# Description:
# ------------
#    Cmake Input File for ecl-roxie
#####################################################


project( ecl-roxie )

include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

set (    SRCS
         ${ESPSCM_GENERATED_DIR}/common_esp.cpp
         ${ESPSCM_GENERATED_DIR}/ws_smc_esp.cpp
         ${ESPSCM_GENERATED_DIR}/ws_dfuXref_esp.cpp
         ecl-roxie.cpp
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

HPCC_ADD_EXECUTABLE ( ecl-roxie ${SRCS} )
add_dependencies ( ecl-roxie espscm ws_workunits )
install ( TARGETS ecl-roxie RUNTIME DESTINATION ${EXEC_DIR} )
target_link_libraries ( ecl-roxie
        jlib
        esphttp
        workunit
    )

if (UNIX)
    install ( PROGRAMS ecl-roxie.install DESTINATION etc/init.d/install COMPONENT Runtime )
    install ( PROGRAMS ecl-roxie.uninstall DESTINATION etc/init.d/uninstall COMPONENT Runtime )
endif (UNIX)
