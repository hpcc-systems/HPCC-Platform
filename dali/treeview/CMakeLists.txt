################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################


# Component: treeview 
#####################################################
# Description:
# ------------
#    Cmake Input File for treeview
#####################################################

if (WIN32 AND INCLUDE_TREEVIEW)
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
    HPCC_ADD_EXECUTABLE ( treeview WIN32 ${SRCS} )
    install ( TARGETS treeview RUNTIME DESTINATION ${EXEC_DIR} )
    target_link_libraries ( treeview
             jlib
             mp 
             hrpc 
             remote 
             dalibase 
        )
endif (WIN32 AND INCLUDE_TREEVIEW)

