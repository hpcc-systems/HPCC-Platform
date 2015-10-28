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

# Component: activityslaves_lcr 
#####################################################
# Description:
# ------------
#    Cmake Input File for activityslaves_lcr
#####################################################

project( activityslaves_lcr ) 

set (    SRCS 
         ../slave/slave.cpp 
         aggregate/thaggregateslave.cpp 
         aggregate/thgroupaggregateslave.cpp 
         apply/thapplyslave.cpp 
         catch/thcatchslave.cpp 
         choosesets/thchoosesetsslave.cpp 
         countproject/thcountprojectslave.cpp 
         csvread/thcsvrslave.cpp 
         degroup/thdegroupslave.cpp 
         diskread/thdiskreadslave.cpp 
         diskwrite/thdwslave.cpp 
         distribution/thdistributionslave.cpp 
         enth/thenthslave.cpp 
         fetch/thfetchslave.cpp 
         filter/thfilterslave.cpp 
         firstn/thfirstnslave.cpp 
         funnel/thfunnelslave.cpp 
         group/thgroupslave.cpp 
         hashdistrib/thhashdistribslave.cpp 
         indexread/thindexreadslave.cpp 
         indexwrite/thindexwriteslave.cpp 
         iterate/thgroupiterateslave.cpp 
         iterate/thiterateslave.cpp 
         join/thjoinslave.cpp 
         keydiff/thkeydiffslave.cpp 
         keyedjoin/thkeyedjoinslave.cpp 
         keypatch/thkeypatchslave.cpp 
         limit/thlimitslave.cpp 
         lookupjoin/thlookupjoinslave.cpp 
         loop/thloopslave.cpp 
         merge/thmergeslave.cpp 
         msort/thgroupsortslave.cpp 
         msort/thmsortslave.cpp 
         msort/thsortu.cpp 
         normalize/thnormalizeslave.cpp 
         nsplitter/thnsplitterslave.cpp 
         null/thnullslave.cpp 
         nullaction/thnullactionslave.cpp 
         parse/thparseslave.cpp 
         piperead/thprslave.cpp 
         pipewrite/thpwslave.cpp 
         project/thprojectslave.cpp 
         pull/thpullslave.cpp 
         result/thresultslave.cpp 
         rollup/throllupslave.cpp 
         sample/thsampleslave.cpp 
         selectnth/thselectnthslave.cpp 
         selfjoin/thselfjoinslave.cpp 
         soapcall/thsoapcallslave.cpp 
         spill/thspillslave.cpp 
         temptable/thtmptableslave.cpp 
         thactivityutil.cpp 
         thdiskbaseslave.cpp 
         topn/thtopnslave.cpp 
         trace/thtraceslave.cpp
         when/thwhenslave.cpp 
         wuidread/thwuidreadslave.cpp 
         wuidwrite/thwuidwriteslave.cpp 
         xmlparse/thxmlparseslave.cpp 
         xmlread/thxmlreadslave.cpp 
         xmlwrite/thxmlwriteslave.cpp 
    )

include_directories ( 
         ./../thorutil 
         ./../../system/jhtree 
         ./../../system/mp 
         ./../../common/workunit 
         ./../shared 
         ./../graph 
         ./../activities/msort 
         ./../../common/deftype 
         ./../../system/include 
         ./../../dali/base 
         ./../../rtl/include 
         ./../slave 
         ./../../system/jlib 
         ./../thorcodectx 
         ./../mfilemanager 
         ./../../common/thorhelper 
         ./../activities 
         ./../../rtl/eclrtl 
         ./../../roxie/roxiemem
         ${HPCC_SOURCE_DIR}/dali/ft
    )

HPCC_ADD_LIBRARY( activityslaves_lcr SHARED ${SRCS} )
set_target_properties(activityslaves_lcr PROPERTIES 
    COMPILE_FLAGS -D_USRDLL
    DEFINE_SYMBOL ACTIVITYSLAVES_EXPORTS )
install ( TARGETS activityslaves_lcr RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( activityslaves_lcr 
         jlib
         thorsort_lcr 
         nbcd 
         eclrtl 
         deftype 
         thorhelper 
         remote 
         dalibase 
         environment 
         dllserver 
         workunit 
         thorcodectx_lcr 
         jhtree 
         graph_lcr 
         graphslave_lcr 
         roxiemem
    )


