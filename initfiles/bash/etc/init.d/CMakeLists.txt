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

SET(outFiles)

GENERATE_BASH(processor ${bash-vars} "dafilesrv.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "install-init.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "hpcc-init.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "hpcc_common.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "uninstall-init.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "setupPKI.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "distributePKI.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "safe_copyPKI.in" outFiles)

ADD_CUSTOM_TARGET(ProcessFiles-initfiles-bash-etc-init.d ALL DEPENDS ${outFiles})
FOREACH( oFILES
    ${outFiles}
    ${CMAKE_CURRENT_SOURCE_DIR}/pid.sh
    ${CMAKE_CURRENT_SOURCE_DIR}/lock.sh
    ${CMAKE_CURRENT_SOURCE_DIR}/init-functions
    ${CMAKE_CURRENT_SOURCE_DIR}/export-path
)
    install ( PROGRAMS ${oFILES} DESTINATION etc/init.d COMPONENT Runtime )
ENDFOREACH ( oFILES )

if ( PLATFORM )
    install ( PROGRAMS hpcc-init.install DESTINATION etc/init.d/install COMPONENT Runtime )
    install ( PROGRAMS hpcc-init.uninstall DESTINATION etc/init.d/uninstall COMPONENT Runtime )
endif()

