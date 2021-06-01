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

# Component: activitymasters_lcr 
#####################################################
# Description:
# ------------
#    Cmake Input File for activitymasters_lcr
#####################################################

project( activitymasters_lcr ) 

set (    SRCS 
         ../master/thactivitymaster.cpp 
         action/thaction.cpp 
         aggregate/thaggregate.cpp 
         apply/thapply.cpp 
         catch/thcatch.cpp 
         choosesets/thchoosesets.cpp 
         countproject/thcountproject.cpp 
         csvread/thcsvread.cpp 
         diskread/thdiskread.cpp 
         diskwrite/thdiskwrite.cpp 
         distribution/thdistribution.cpp 
         enth/thenth.cpp 
         external/thexternal.cpp
         fetch/thfetch.cpp 
         filter/thfilter.cpp 
         firstn/thfirstn.cpp 
         funnel/thfunnel.cpp 
         group/thgroup.cpp
         hashdistrib/thhashdistrib.cpp 
         indexread/thindexread.cpp 
         indexwrite/thindexwrite.cpp 
         iterate/thiterate.cpp 
         join/thjoin.cpp 
         keydiff/thkeydiff.cpp 
         keyedjoin/thkeyedjoin.cpp 
         keyedjoin/thkeyedjoin-legacy.cpp 
         keypatch/thkeypatch.cpp 
         limit/thlimit.cpp 
         lookupjoin/thlookupjoin.cpp 
         loop/thloop.cpp 
         merge/thmerge.cpp 
         msort/thmsort.cpp 
         nullaction/thnullaction.cpp 
         pipewrite/thpipewrite.cpp 
         result/thresult.cpp 
         rollup/throllup.cpp 
         selectnth/thselectnth.cpp 
         spill/thspill.cpp 
         thdiskbase.cpp 
         topn/thtopn.cpp 
         when/thwhen.cpp 
         wuidread/thwuidread.cpp 
         wuidwrite/thwuidwrite.cpp 
         xmlread/thxmlread.cpp 
         xmlwrite/thxmlwrite.cpp 
    )

include_directories ( 
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
         ./../thorutil 
         ./../../fs/dafsclient 
         ./../../system/jhtree 
         ./../../system/mp 
         ./../master 
         ./../../common/workunit 
         ./../shared 
         ./../graph 
         ./../../common/deftype 
         ./../../system/include 
         ./../../dali/base 
         ./../../ecl/hql
         ./../../rtl/include 
         ./../../common/dllserver 
         ./../msort 
         ./../thorcodectx 
         ./../../system/jlib 
         ./../mfilemanager 
         ./../../common/thorhelper 
         ./../activities 
         ./../../rtl/eclrtl
         ${HPCC_SOURCE_DIR}/dali/ft
         ./../../system/security/shared
    )

HPCC_ADD_LIBRARY( activitymasters_lcr SHARED ${SRCS} )
set_target_properties(activitymasters_lcr PROPERTIES 
    COMPILE_FLAGS -D_USRDLL
    DEFINE_SYMBOL ACTIVITYMASTERS_EXPORTS )
install ( TARGETS activitymasters_lcr RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( activitymasters_lcr
         jlib
         dafsclient 
         hql
         thorsort_lcr 
         jhtree 
         nbcd 
         eclrtl 
         deftype 
         thorhelper 
         dalibase 
         dllserver 
         workunit 
         thorcodectx_lcr 
         graph_lcr 
         dalift 
         mfilemanager_lcr 
         graphmaster_lcr 
    )

if (NOT CONTAINERIZED)
    target_link_libraries ( activitymasters_lcr environment )
endif()

