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

# Component: jhtree 

#####################################################
# Description:
# ------------
#    Cmake Input File for jhtree
#####################################################


project( jhtree ) 

set (    SRCS 
         hlzw.cpp 
         ctfile.cpp 
         jhtree.cpp 
         jhutil.cpp 
         keybuild.cpp 
         keydiff.cpp 
         sourcedoc.xml
    )

set (    INCLUDES
         ctfile.hpp
         hlzw.h
         jhtree.hpp
         jhutil.hpp
         keybuild.hpp
         keydiff.hpp
    )

include_directories ( 
         .
         ./../../common/deftype 
         ./../../common/thorhelper 
         ./../../rtl/eclrtl 
         ./../../rtl/include
         ./../../system/include 
         ./../../system/jlib 
         ./../../testing/unittests
    )

ADD_DEFINITIONS( -D_USRDLL -DJHTREE_EXPORTS -DECLRTL_LOCAL )

HPCC_ADD_LIBRARY( jhtree SHARED ${SRCS} ${INCLUDES} )
install ( TARGETS jhtree RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( jhtree 
         eclrtl
         jlib
         ${CPPUNIT_LIBRARIES}
    )


