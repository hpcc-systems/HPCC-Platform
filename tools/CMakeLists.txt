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
HPCC_ADD_SUBDIRECTORY (esdl)
HPCC_ADD_SUBDIRECTORY (hidl)
HPCC_ADD_SUBDIRECTORY (backupnode ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (combine ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (dumpkey ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (keydiff ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (pstart ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (pskill ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (testsocket ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (swapnode ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (vkey ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (xmlsize ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (wuget)
HPCC_ADD_SUBDIRECTORY (copyexp ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (genht ${CLIENTTOOLS_ONLY})

# start-stop-daemon is linux only.
if ( CMAKE_SYSTEM MATCHES Linux )
    message ("-- Building start-stop-daemon")
    HPCC_ADD_SUBDIRECTORY (start-stop-daemon)
endif()
