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


# component: esp/scm

#####################################################
# Description:
# ------------
#    Cmake Input File esp/scm
#####################################################

project (scm)

set (SCM_BUILD ON)
include ( ${CMAKE_CURRENT_SOURCE_DIR}/espscm.cmake)
include ( ${CMAKE_CURRENT_SOURCE_DIR}/smcscm.cmake)
include ( ${CMAKE_CURRENT_SOURCE_DIR}/additional.cmake)
set (SCM_BUILD OFF)

ADD_CUSTOM_TARGET( 
  espscm ALL
  DEPENDS ${ESP_GENERATED_INCLUDES}
)
