################################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    This program is free software: you can redistribute it and/or All rights
#    reserved. This program is NOT PRESENTLY free software: you can NOT
#    redistribute
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
HPCC_ADD_SUBDIRECTORY (ecldirect "PLATFORM")
IF (USE_OPENLDAP)
    HPCC_ADD_SUBDIRECTORY (ws_access "PLATFORM")
ENDIF(USE_OPENLDAP)
HPCC_ADD_SUBDIRECTORY (ws_account "PLATFORM")
HPCC_ADD_SUBDIRECTORY (ws_config "PLATFORM")
HPCC_ADD_SUBDIRECTORY (ws_dfu)
HPCC_ADD_SUBDIRECTORY (ws_ecl "PLATFORM")
HPCC_ADD_SUBDIRECTORY (ws_fileio "PLATFORM")
HPCC_ADD_SUBDIRECTORY (ws_fs)
HPCC_ADD_SUBDIRECTORY (ws_machine "PLATFORM")
HPCC_ADD_SUBDIRECTORY (ws_roxiequery "PLATFORM")
HPCC_ADD_SUBDIRECTORY (ws_smc "PLATFORM")
HPCC_ADD_SUBDIRECTORY (ws_topology "PLATFORM")
HPCC_ADD_SUBDIRECTORY (ws_workunits "PLATFORM")
HPCC_ADD_SUBDIRECTORY (WsDeploy "PLATFORM")
HPCC_ADD_SUBDIRECTORY (ws_packageprocess "PLATFORM")
