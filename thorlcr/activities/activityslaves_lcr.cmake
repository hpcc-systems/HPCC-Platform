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
         ./../thorcrc 
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
         ./../../common/commonext 
         ./../activities 
         ./../../rtl/eclrtl 
         ./../../roxie/roxiemem
    )

HPCC_ADD_LIBRARY( activityslaves_lcr SHARED ${SRCS} )
set_target_properties(activityslaves_lcr PROPERTIES 
    COMPILE_FLAGS -D_USRDLL
    DEFINE_SYMBOL ACTIVITYSLAVES_EXPORTS )
install ( TARGETS activityslaves_lcr DESTINATION ${OSSDIR}/lib )
target_link_libraries ( activityslaves_lcr 
         jlib
         thorsort_lcr 
         commonext 
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


