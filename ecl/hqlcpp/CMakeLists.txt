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


# Component: hqlcpp 

#####################################################
# Description:
# ------------
#    Cmake Input File for hqlcpp
#####################################################


project( hqlcpp ) 

set (    SRCS 
         hqlalias.cpp
         hqlcatom.cpp
         hqlccommon.cpp 
         hqlcfilter.cpp
         hqlckey.cpp 
         hqlcpp.cpp 
         hqlcppc.cpp 
         hqlcppcase.cpp 
         hqlcppds.cpp 
         hqlcpputil.cpp 
         hqlcse.cpp 
         hqlcset.cpp 
         hqlctrans.cpp
         hqlecl.cpp 
         hqlgraph.cpp 
         hqlhoist.cpp
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
    
         hqlalias.hpp
         hqlcatom.hpp
         hqlccommon.hpp
         hqlcerrors.hpp
         hqlcfilter.hpp
         hqlcpp.hpp
         hqlcppc.hpp
         hqlcppds.hpp
         hqlcpputil.hpp
         hqlctrans.hpp
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
         hqlhoist.hpp
         hqlhtcpp.ipp
         hqliproj.hpp
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
         ./../../system/jhtree
         ./../../common/thorhelper
         ./../../rtl/eclrtl
         ./../../rtl/include
         ${HPCC_SOURCE_DIR}/system/codesigner
    )

IF (NOT WIN32)
  include_directories(${BINUTILS_INCLUDE_DIR})
ENDIF()

if (CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_CLANG)
  SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wno-switch -Wno-unused-parameter -Werror -Wno-error=delete-non-virtual-dtor")

  #implicit-fallthrough is currently disabled because it does not seem to process noreturn attributes on a virtual function correctly (throwStringExceptionV)
  if (CMAKE_COMPILER_IS_GNUCC AND NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS "7.0.0")
    SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wimplicit-fallthrough=0")
  endif()
endif()

ADD_DEFINITIONS( -DHQLCPP_EXPORTS -D_USRDLL )

HPCC_ADD_LIBRARY( hqlcpp SHARED ${SRCS} )
install ( TARGETS hqlcpp RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
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


