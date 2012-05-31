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
HPCC_ADD_SUBDIRECTORY (commonext)
HPCC_ADD_SUBDIRECTORY (deftype)
HPCC_ADD_SUBDIRECTORY (dllserver)
HPCC_ADD_SUBDIRECTORY (environment)
HPCC_ADD_SUBDIRECTORY (fileview2)
HPCC_ADD_SUBDIRECTORY (monitoring ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (remote)
HPCC_ADD_SUBDIRECTORY (roxiecommlib)
HPCC_ADD_SUBDIRECTORY (roxiehelper ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (roxiemanager ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (thorhelper)
HPCC_ADD_SUBDIRECTORY (workunit)
HPCC_ADD_SUBDIRECTORY (wuwebview ${CLIENTTOOLS_ONLY})
