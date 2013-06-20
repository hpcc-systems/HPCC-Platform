##############################################################################
# HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
##############################################################################

# Component: ws_packageprocess
#####################################################
# Description:
# ------------
#    Cmake Input File for ws_packageprocess
#####################################################

project( ws_packageprocess )

include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

set (    SRCS
         ${HPCC_SOURCE_DIR}/dali/dfu/dfuutil.cpp
         ${ESPSCM_GENERATED_DIR}/ws_fs_esp.cpp
         ${ESPSCM_GENERATED_DIR}/ws_packageprocess_esp.cpp
         ${HPCC_SOURCE_DIR}/esp/scm/ws_packageprocess.ecm
         ws_packageprocessPlugin.cpp
         ws_packageprocessService.cpp
         ws_packageprocessService.hpp
         packageprocess_errors.h
    )

include_directories (
         ${HPCC_SOURCE_DIR}/esp/platform
         ${HPCC_SOURCE_DIR}/system/jlib
         ${HPCC_SOURCE_DIR}/esp/services
         ${HPCC_SOURCE_DIR}/system/xmllib
         ${HPCC_SOURCE_DIR}/system/security/securesocket
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/system/mp
         ${HPCC_SOURCE_DIR}/esp/clients
         ${HPCC_SOURCE_DIR}/esp/bindings
         ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/xpp
         ${HPCC_SOURCE_DIR}/dali/base
         ${HPCC_SOURCE_DIR}/common/environment
         ${HPCC_SOURCE_DIR}/dali/dfu
         ${HPCC_SOURCE_DIR}/common/remote
         ${HPCC_SOURCE_DIR}/common/workunit
         ${HPCC_SOURCE_DIR}/rtl/include
    )

ADD_DEFINITIONS( -D_USRDLL )

HPCC_ADD_LIBRARY( ws_packageprocess SHARED ${SRCS} )
install ( TARGETS ws_packageprocess RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
add_dependencies (ws_packageprocess espscm)
target_link_libraries ( ws_packageprocess
         jlib
         xmllib
         esphttp
         securesocket
         dalibase
         ws_fs
    )

