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

# Component: dalift 
#####################################################
# Description:
# ------------
#    Cmake Input File for dalift
#####################################################

project( dalift ) 

set (    SRCS 
         daft.cpp 
         daftdir.cpp 
         daftformat.cpp 
         daftmc.cpp 
         daftprogress.cpp 
         daftsize.cpp 
         filecopy.cpp 
         ftbase.cpp 
         fttransform.cpp 
    )

include_directories ( 
         ./../../common/remote 
         ./../../system/mp 
         ./../base 
         ./../../system/include 
         ./../../system/jlib 
         ./../../common/environment 
    )

HPCC_ADD_LIBRARY( dalift SHARED ${SRCS} )
set_target_properties (dalift PROPERTIES 
    COMPILE_FLAGS -D_CONSOLE
    DEFINE_SYMBOL DALIFT_EXPORTS
)
install ( TARGETS dalift DESTINATION ${OSSDIR}/lib )
target_link_libraries ( dalift 
    jlib
    mp 
    hrpc 
    remote 
    dalibase 
    environment 
)

