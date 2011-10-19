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
add_subdirectory (base)
add_subdirectory (dafilesrv)
add_subdirectory (dalidiag)
add_subdirectory (dalistop)
add_subdirectory (datest)
if (USE_CPPUNIT)
    add_subdirectory (daunittest)
endif()
add_subdirectory (dfu)
add_subdirectory (dfuplus)
add_subdirectory (dfuxref)
add_subdirectory (dfuXRefLib)
add_subdirectory (ft)
add_subdirectory (regress)
add_subdirectory (sdsfix)
add_subdirectory (daliadmin)
add_subdirectory (server)
add_subdirectory (treeview)
add_subdirectory (updtdalienv)
add_subdirectory (sasha)
