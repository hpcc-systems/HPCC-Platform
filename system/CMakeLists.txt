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
HPCC_ADD_SUBDIRECTORY (hrpc)
HPCC_ADD_SUBDIRECTORY (include ${CLIENTTOOLS_ONLY})
HPCC_ADD_SUBDIRECTORY (jhtree)
HPCC_ADD_SUBDIRECTORY (jlib)
HPCC_ADD_SUBDIRECTORY (lzma)
HPCC_ADD_SUBDIRECTORY (mp)
HPCC_ADD_SUBDIRECTORY (security)
HPCC_ADD_SUBDIRECTORY (xmllib)
HPCC_ADD_SUBDIRECTORY (xmllibtest ${CLIENTTOOLS_ONLY})
