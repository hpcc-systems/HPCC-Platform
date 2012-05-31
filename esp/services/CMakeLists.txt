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
HPCC_ADD_SUBDIRECTORY (ecldirect ${CLIENTTOOLS_ONLY})
IF (USE_OPENLDAP)
    HPCC_ADD_SUBDIRECTORY (ws_access ${CLIENTTOOLS_ONLY})
ENDIF(USE_OPENLDAP)
HPCC_ADD_SUBDIRECTORY (ws_account ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (ws_config ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (ws_dfu )
HPCC_ADD_SUBDIRECTORY (ws_ecl ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (ws_fileio ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (ws_fs)
HPCC_ADD_SUBDIRECTORY (ws_machine ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (ws_roxiequery ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (ws_smc ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (ws_topology ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (ws_workunits ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (WsDeploy ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (ws_packageprocess ${CLIENTTOOLS_ONLY})
