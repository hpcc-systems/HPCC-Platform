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


# Component: dalibase 

#####################################################
# Description:
# ------------
#    Cmake Input File for dalibase
#####################################################


project( dalibase ) 

set (    SRCS 
         daaudit.cpp 
         daclient.cpp 
         dacoven.cpp 
         dacsds.cpp 
         dadfs.cpp 
         dadiags.cpp 
         dafdesc.cpp 
         danqs.cpp 
         dasds.cpp 
         dasess.cpp 
         dasubs.cpp 
         dautils.cpp 
    )
    
set (    INCLUDES
         daaudit.hpp
         daclient.hpp
         dacoven.hpp
         dadfs.hpp
         dadiags.hpp
         dafdesc.hpp
         danqs.hpp
         dasds.hpp
         dasess.hpp
         dasubs.hpp
         dautils.hpp
    )   

include_directories ( 
         ./../../common/deftype 
         ./../../common/remote 
         ./../server 
         ./../../system/mp 
         ./../../system/include 
         ./../../system/jlib 
         ./../../rtl/include 
         ./../../system/security/shared
         ./../../system/security/cryptohelper
    )

ADD_DEFINITIONS( -DLOGMSGCOMPONENT=3 -D_USRDLL -DDALI_EXPORTS )

HPCC_ADD_LIBRARY( dalibase SHARED ${SRCS} ${INCLUDES} )

if(NOT PLUGIN)
    install(
        TARGETS dalibase
        RUNTIME DESTINATION ${EXEC_DIR}
        LIBRARY DESTINATION ${LIB_DIR})
    target_link_libraries(
        dalibase 
        jlib
        mp 
        hrpc 
        remote)
endif()
