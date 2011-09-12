################################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
################################################################################

SET(outFiles)

GENERATE_BASH(processor ${bash-vars} "dafilesrv.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "install-init.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "hpcc-init.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "hpcc_common.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "uninstall-init.in" outFiles)

ADD_CUSTOM_TARGET(ProcessFiles-initfiles-bash-etc-init.d ALL DEPENDS ${outFiles})
FOREACH( oFILES
    ${outFiles}
    ${CMAKE_CURRENT_SOURCE_DIR}/pid.sh
    ${CMAKE_CURRENT_SOURCE_DIR}/lock.sh
    ${CMAKE_CURRENT_SOURCE_DIR}/init-functions
    ${CMAKE_CURRENT_SOURCE_DIR}/export-path
)
    install ( PROGRAMS ${oFILES} DESTINATION ${OSSDIR}/etc/init.d COMPONENT Runtime )
ENDFOREACH ( oFILES )
