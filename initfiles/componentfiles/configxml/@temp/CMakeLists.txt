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

FOREACH ( iFILES
    ${CMAKE_CURRENT_SOURCE_DIR}/esp_service.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/wsecl_onchange.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/esp_service_wsecl.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/plugins.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/esp_service_WsSMC.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/roxiePlugins.xsl
)
    Install ( FILES ${iFILES} DESTINATION ${OSSDIR}/componentfiles/configxml/@temp COMPONENT Runtime )
ENDFOREACH ( iFILES )