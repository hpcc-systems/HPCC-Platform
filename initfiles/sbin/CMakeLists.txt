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
GENERATE_BASH(processor ${bash-vars} "hpcc_setenv.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "complete-uninstall.sh.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "keygen.sh.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "add_conf_settings.sh.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "rm_conf_settings.sh.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "configmgr.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "get_ip_address.sh.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "install-cluster.sh.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "hpcc-push.sh.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "hpcc-run.sh.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "remote-install-engine.sh.in" outFiles)

SET(noInstall)
GENERATE_BASH(processor ${bash-vars} "prerm.in" noInstall)


ADD_CUSTOM_TARGET(ProcessFiles-initfiles-sbin ALL DEPENDS ${outFiles} ${noInstall})
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/regex.awk.in.cmake" "${CMAKE_CURRENT_BINARY_DIR}/regex.awk")

FOREACH( oFILES
    ${outFiles}
    ${CMAKE_CURRENT_BINARY_DIR}/regex.awk
    ${CMAKE_CURRENT_SOURCE_DIR}/alter_confs.sh
)
    install ( PROGRAMS ${oFILES} DESTINATION ${OSSDIR}/etc/init.d COMPONENT Runtime )
ENDFOREACH ( oFILES )