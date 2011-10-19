################################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it
#    and/or modify
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
configure_file("version.in" "version")
configure_file("environment.conf.in" "environment.conf")
configure_file("environment.xml.in" "environment.xml")
FOREACH( iFILES
    ${CMAKE_CURRENT_BINARY_DIR}/environment.conf
    ${CMAKE_CURRENT_BINARY_DIR}/version
)
    Install ( FILES ${iFILES}  DESTINATION ${OSSDIR}${CONFIG_DIR} COMPONENT Runtime )
ENDFOREACH ( iFILES)

Install ( FILES ${CMAKE_CURRENT_BINARY_DIR}/environment.xml DESTINATION ${OSSDIR}${CONFIG_DIR}/rpmnew COMPONENT Runtime )
Install ( FILES ${CMAKE_CURRENT_BINARY_DIR}/environment.conf DESTINATION ${CONFIG_DIR}/rpmnew COMPONENT Runtime )
Install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/genenvrules.conf DESTINATION ${CONFIG_DIR}/rpmnew COMPONENT Runtime )

ADD_SUBDIRECTORY(configmgr)
