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
         fetch/thfetch.cpp 
         filter/thfilter.cpp 
         firstn/thfirstn.cpp 
         funnel/thfunnel.cpp 
         hashdistrib/thhashdistrib.cpp 
         indexread/thindexread.cpp 
         indexwrite/thindexwrite.cpp 
         iterate/thiterate.cpp 
         join/thjoin.cpp 
         keydiff/thkeydiff.cpp 
         keyedjoin/thkeyedjoin.cpp 
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
         soapcall/thsoapcall.cpp 
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
         ./../thorutil 
         ./../../common/remote 
         ./../../system/jhtree 
         ./../../system/mp 
         ./../master 
         ./../../common/workunit 
         ./../shared 
         ./../graph 
         ./../../common/deftype 
         ./../../system/include 
         ./../../dali/base 
         ./../../rtl/include 
         ./../../common/dllserver 
         ./../msort 
         ./../thorcodectx 
         ./../../system/jlib 
         ./../mfilemanager 
         ./../../common/thorhelper 
         ./../../common/commonext 
         ./../activities 
         ./../../rtl/eclrtl 
    )

HPCC_ADD_LIBRARY( activitymasters_lcr SHARED ${SRCS} )
set_target_properties(activitymasters_lcr PROPERTIES 
    COMPILE_FLAGS -D_USRDLL
    DEFINE_SYMBOL ACTIVITYMASTERS_EXPORTS )
install ( TARGETS activitymasters_lcr DESTINATION ${OSSDIR}/lib )
target_link_libraries ( activitymasters_lcr
         jlib
         remote 
         thorsort_lcr 
         commonext 
         jhtree 
         nbcd 
         eclrtl 
         deftype 
         thorhelper 
         dalibase 
         environment 
         dllserver 
         workunit 
         thorcodectx_lcr 
         graph_lcr 
         dalift 
         mfilemanager_lcr 
         graphmaster_lcr 
    )


