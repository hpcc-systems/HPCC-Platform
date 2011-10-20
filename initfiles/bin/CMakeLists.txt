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
GENERATE_BASH(processor ${bash-vars} "init_dafilesrv.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "init_eclagent.in" outFiles)
ADD_CUSTOM_TARGET(ProcessFiles-initfiles-bin ALL DEPENDS ${outFiles})

FOREACH( iFILES
    ${outFiles}
    ${CMAKE_CURRENT_SOURCE_DIR}/init_configesp
    ${CMAKE_CURRENT_SOURCE_DIR}/init_dali
    ${CMAKE_CURRENT_SOURCE_DIR}/init_dfuserver
    ${CMAKE_CURRENT_SOURCE_DIR}/init_eclccserver
    ${CMAKE_CURRENT_SOURCE_DIR}/init_eclscheduler
    ${CMAKE_CURRENT_SOURCE_DIR}/init_esp
    ${CMAKE_CURRENT_SOURCE_DIR}/init_ftslave
    ${CMAKE_CURRENT_SOURCE_DIR}/init_roxie
    ${CMAKE_CURRENT_SOURCE_DIR}/init_roxie_cluster
    ${CMAKE_CURRENT_SOURCE_DIR}/init_sasha
    ${CMAKE_CURRENT_SOURCE_DIR}/init_thor
)
    install ( PROGRAMS ${iFILES} DESTINATION ${OSSDIR}/bin COMPONENT Runtime )
ENDFOREACH ( iFILES )
