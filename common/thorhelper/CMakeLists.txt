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


# Component: thorhelper 

#####################################################
# Description:
# ------------
#    Cmake Input File for thorhelper
#####################################################

project( thorhelper ) 

set (    SRCS 

         commonext.cpp
         csvsplitter.cpp 
         layouttrans.cpp 
         thorcommon.cpp 
         thorfile.cpp 
         thorparse.cpp 
         thorpipe.cpp 
         thorrparse.cpp 
         thorsoapcall.cpp 
         thorstats.cpp
         thorstep.cpp 
         thorstep2.cpp 
         thorstrand.cpp
         thortalgo.cpp 
         thortlex.cpp 
         thortparse.cpp 
         thorxmlread.cpp 
         thorxmlwrite.cpp
         roxierow.cpp
         roxiehelper.cpp
         roxiedebug.cpp
         
         commonext.hpp
         csvsplitter.hpp 
         layouttrans.hpp 
         thorcommon.hpp 
         thorfile.hpp 
         thorparse.hpp 
         thorpipe.hpp 
         thorrparse.hpp 
         thorsoapcall.hpp 
         thorstats.hpp
         thorstep.hpp 
         thorstrand.hpp
         thorxmlread.hpp 
         thorxmlwrite.hpp
         roxierow.hpp
         thorralgo.ipp
         thorrparse.ipp
         thorsort.cpp
         thorsort.hpp
         
         roxiedebug.hpp
         roxiedebug.ipp
         roxiehelper.hpp
         roxiehelper.ipp
         roxielmj.hpp
                 
    )

include_directories ( 
         ./../../system/security/securesocket 
         ./../../common/remote 
         ./../../system/jhtree 
         ./../../rtl/eclrtl 
         ./../../rtl/nbcd 
         ./../../system/include 
         ./../../system/mp
         ./../../system/jlib
         ./../../system/security/zcrypt
         ./../../dali/base
         ./../deftype 
         ./../workunit
         ./../../rtl/include 
         ./../../roxie/roxiemem
         ./../../testing/unittests
         ${TBB_INCLUDE_DIR}
    )

ADD_DEFINITIONS( -DTHORHELPER_EXPORTS -D_USRDLL )

HPCC_ADD_LIBRARY( thorhelper SHARED ${SRCS} )
install ( TARGETS thorhelper RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( thorhelper 
         jlib 
         nbcd 
         eclrtl 
         roxiemem
         deftype 
         dalibase 
    )

if (USE_NUMA)
 target_link_libraries ( thorhelper numa )
endif ()

if (${USE_TBB} )
   target_link_libraries ( thorhelper ${TBB_LIBRARIES})
endif()


IF (USE_OPENSSL)
    target_link_libraries ( thorhelper 
    	securesocket
    )
ENDIF()

IF (USE_ZLIB)
    target_link_libraries ( thorhelper
        ${ZLIB_LIBRARIES}
        zcrypt
    )
ENDIF()