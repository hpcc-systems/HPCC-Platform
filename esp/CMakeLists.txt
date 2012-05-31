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
HPCC_ADD_SUBDIRECTORY (bindings ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (clients ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (eclwatch ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (files ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (platform ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (protocols)
HPCC_ADD_SUBDIRECTORY (scm)
HPCC_ADD_SUBDIRECTORY (services)
HPCC_ADD_SUBDIRECTORY (smc)
HPCC_ADD_SUBDIRECTORY (test ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (tools ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (xslt ${CLIENTTOOLS_ONLY})
