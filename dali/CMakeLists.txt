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
HPCC_ADD_SUBDIRECTORY (base)
HPCC_ADD_SUBDIRECTORY (dafilesrv)
HPCC_ADD_SUBDIRECTORY (dalidiag ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (dalistop ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (datest ${CLIENTTOOLS_ONLY})
if (USE_CPPUNIT)
    HPCC_ADD_SUBDIRECTORY (daunittest ${CLIENTTOOLS_ONLY})
endif()
HPCC_ADD_SUBDIRECTORY (dfu)
HPCC_ADD_SUBDIRECTORY (dfuplus)
HPCC_ADD_SUBDIRECTORY (dfuxref ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (dfuXRefLib)
HPCC_ADD_SUBDIRECTORY (ft)
HPCC_ADD_SUBDIRECTORY (regress ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (daliadmin ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (server ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (treeview ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (updtdalienv ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (sasha ${CLIENTTOOLS_ONLY})
