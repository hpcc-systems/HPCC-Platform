## Copyright © 2011 HPCC Systems.  All rights reserved.
SET(outFiles)
GENERATE_BASH(processor ${bash-vars} "configmgr.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "get_ip_address.sh.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "dafilesrv.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "install-init.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "hpcc-init.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "hpcc_common.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "uninstall-init.in" outFiles)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/regex.awk.in.cmake" "${CMAKE_CURRENT_BINARY_DIR}/regex.awk")
ADD_CUSTOM_TARGET(ProcessFiles-initfiles-bash-etc-init.d ALL DEPENDS ${outFiles})

