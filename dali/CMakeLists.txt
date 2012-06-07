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
HPCC_ADD_SUBDIRECTORY (base "ALL")
HPCC_ADD_SUBDIRECTORY (dafilesrv "ALL")
HPCC_ADD_SUBDIRECTORY (dalidiag "PLATFORM")
HPCC_ADD_SUBDIRECTORY (dalistop "PLATFORM")
HPCC_ADD_SUBDIRECTORY (datest "PLATFORM")
if (USE_CPPUNIT)
    HPCC_ADD_SUBDIRECTORY (daunittest "PLATFORM")
endif()
HPCC_ADD_SUBDIRECTORY (dfu "ALL")
HPCC_ADD_SUBDIRECTORY (dfuplus "CLIENTTOOLS")
HPCC_ADD_SUBDIRECTORY (dfuxref "PLATFORM")
HPCC_ADD_SUBDIRECTORY (dfuXRefLib "ALL")
HPCC_ADD_SUBDIRECTORY (ft "ALL")
HPCC_ADD_SUBDIRECTORY (regress "PLATFORM")
HPCC_ADD_SUBDIRECTORY (daliadmin "PLATFORM")
HPCC_ADD_SUBDIRECTORY (server "PLATFORM")
HPCC_ADD_SUBDIRECTORY (treeview "PLATFORM")
HPCC_ADD_SUBDIRECTORY (updtdalienv "PLATFORM")
HPCC_ADD_SUBDIRECTORY (sasha "PLATFORM")
