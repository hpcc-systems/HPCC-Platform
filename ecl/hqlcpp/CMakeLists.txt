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


# Component: hqlcpp 

#####################################################
# Description:
# ------------
#    Cmake Input File for hqlcpp
#####################################################


project( hqlcpp ) 

set (    SRCS 
         hqlcatom.cpp 
         hqlccommon.cpp 
         hqlckey.cpp 
         hqlcpp.cpp 
         hqlcppc.cpp 
         hqlcppcase.cpp 
         hqlcppds.cpp 
         hqlcpputil.cpp 
         hqlcse.cpp 
         hqlcset.cpp 
         hqlecl.cpp 
         hqlgraph.cpp 
         hqlhtcpp.cpp 
         hqlinline.cpp 
         hqliproj.cpp 
         hqliter.cpp 
         hqllib.cpp 
         hqlnlp.cpp 
         hqlpopt.cpp 
         hqlregex.cpp 
         hqlres.cpp 
         hqlresource.cpp 
         hqlsource.cpp 
         hqlstep.cpp 
         hqlstmt.cpp 
         hqltcppc.cpp 
         hqltcppc2.cpp 
         hqltomita.cpp 
         hqlttcpp.cpp 
         hqlwcpp.cpp 
    
         hqlcatom.hpp
         hqlccommon.hpp
         hqlcerrors.hpp
         hqlcpp.hpp
         hqlcppc.hpp
         hqlcpputil.hpp
         hqlecl.hpp
         hqlfunc.hpp
         hqlinline.hpp
         hqlpopt.hpp
         hqlregex.hpp
         hqlres.hpp
         hqlresource.hpp
         hqlstmt.hpp
         hqltcppc.hpp
         hqlwcpp.hpp
         hqlcpp.ipp
         hqlcse.ipp
         hqlcset.ipp
         hqlfunc.ipp
         hqlgraph.ipp
         hqlhtcpp.ipp
         hqliproj.ipp
         hqliter.ipp
         hqllib.ipp
         hqlnlp.ipp
         hqlregex.ipp
         hqlresource.ipp
         hqlsource.ipp
         hqlstmt.ipp
         hqltcppc.ipp
         hqltomita.ipp
         hqlttcpp.ipp
         hqlwcpp.ipp
         
         hqlcppsys.ecl
        )


include_directories ( 
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
         ./../../common/remote 
         ./../../common/workunit 
         ./../../common/deftype 
         ./../../system/include 
         ./../../ecl/hql 
         ./../../common/dllserver 
         ./../../system/jlib 
         ./../../common/thorhelper 
         ./../../rtl/eclrtl 
         ./../../rtl/include
    )

IF (NOT WIN32)
  include_directories(${BINUTILS_INCLUDE_DIR})
ENDIF()

ADD_DEFINITIONS( -DHQLCPP_EXPORTS -D_USRDLL )

HPCC_ADD_LIBRARY( hqlcpp SHARED ${SRCS} )
install ( TARGETS hqlcpp DESTINATION ${OSSDIR}/lib )
target_link_libraries ( hqlcpp
         jlib
         nbcd 
         eclrtl 
         deftype 
         thorhelper 
         hql 
         workunit
    )
if (NOT WIN32)
  target_link_libraries ( hqlcpp ${BINUTILS_LIBRARIES})
endif()


