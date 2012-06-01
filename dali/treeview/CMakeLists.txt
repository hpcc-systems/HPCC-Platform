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


# Component: treeview 
#####################################################
# Description:
# ------------
#    Cmake Input File for treeview
#####################################################

if (WIN32)
    project( treeview ) 

    set (    SRCS 
             connect.cpp 
             inspectctrl.cpp 
             newvaluedlg.cpp 
             stdafx.cpp 
             treeview.cpp 
             treeview.rc 
             treeviewdlg.cpp 
             util.cpp 
        )

    include_directories ( 
             ./../../system/mp 
             ./../base 
             ./../../system/include 
             ./../../system/jlib 
        )

    add_definitions("-D_AFXDLL")

    if ( NOT CLIENTTOOLS_ONLY )
        add_executable ( treeview WIN32 ${SRCS} )
        install ( TARGETS treeview DESTINATION ${OSSDIR}/bin )
        target_link_libraries ( treeview
             jlib
             mp 
             hrpc 
             remote 
             dalibase 
        )
    endif()
endif (WIN32)

