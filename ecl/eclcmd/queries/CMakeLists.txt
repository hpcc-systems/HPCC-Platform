##############################################################################
# Copyright (C) 2012 HPCC Systems®.
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
         ${ESPSCM_GENERATED_DIR}/common_esp.cpp
         ${ESPSCM_GENERATED_DIR}/ws_workunits_struct_esp.cpp
         ${ESPSCM_GENERATED_DIR}/ws_workunits_req_resp_esp.cpp
         ${ESPSCM_GENERATED_DIR}/ws_workunits_queryset_req_resp_esp.cpp
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
         ${HPCC_SOURCE_DIR}/esp/smc/SMCLib
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/system/xmllib
         ${HPCC_SOURCE_DIR}/ecl/eclcmd
         ${HPCC_SOURCE_DIR}/ecl/eclcc
         ${HPCC_SOURCE_DIR}/common/thorhelper
    )

ADD_DEFINITIONS( -D_CONSOLE )

HPCC_ADD_EXECUTABLE ( ecl-queries ${SRCS} )
add_dependencies ( ecl-queries espscm)
install ( TARGETS ecl-queries RUNTIME DESTINATION ${EXEC_DIR} )
target_link_libraries ( ecl-queries
        jlib
        esphttp
        workunit
    )

if (UNIX)
    install ( PROGRAMS ecl-queries.install DESTINATION etc/init.d/install COMPONENT Runtime )
    install ( PROGRAMS ecl-queries.uninstall DESTINATION etc/init.d/uninstall COMPONENT Runtime )
endif (UNIX)
