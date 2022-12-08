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

# Component: eclrtl 

#####################################################
# Description:
# ------------
#    Cmake Input File for eclrtl
#####################################################


project( eclrtl ) 

set (    SRCS 
         eclhelper_base.cpp
         eclhelper_dyn.cpp
         eclrtl.cpp 
         eclregex.cpp
         rtlbcd.cpp 
         rtldistr.cpp 
         rtlds.cpp 
         rtldynfield.cpp 
         rtlint.cpp 
         rtlkey.cpp 
         rtlnewkey.cpp
         rtlnktest.cpp
         rtlqstr.cpp 
         rtlrank.cpp 
         rtlfield.cpp 
         rtlread.cpp 
         rtlrecord.cpp
         rtltype.cpp 
         rtlxml.cpp
         rtlcommon.cpp
         rtlformat.cpp
         
         eclinclude4.hpp
         eclrtl.hpp
         eclrtl_imp.hpp
         rtlbcd.hpp
         rtldistr.hpp
         rtlds_imp.hpp
         rtlembed.hpp
         rtlfield.hpp
         rtlkey2.hpp
         rtlkey.hpp
         rtlnewkey.hpp
         rtlread_imp.hpp
         rtlrecord.hpp
         rtlsize.hpp
         rtltype.hpp
         rtlbcdtest.cpp 
    )

include_directories ( 
         ./../../rtl/include 
         ./../../rtl/nbcd 
         ./../../rtl/eclrtl 
         ./../../system/include 
         ./../../system/jlib 
         ./../../roxie/roxiemem
         ./../../testing/unittests
         ${Boost_INCLUDE_DIRS}
    )

ADD_DEFINITIONS( -D_USRDLL -DECLRTL_EXPORTS )

if ((USE_BOOST_REGEX) AND (NOT WIN32))
  ADD_DEFINITIONS( -DBOOST_DYN_LINK )
endif ()

if ((CMAKE_COMPILER_IS_GNUCXX AND NOT CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 8.0.0))
  set_source_files_properties(eclrtl.cpp PROPERTIES COMPILE_FLAGS -fno-tree-vectorize)
endif ()

HPCC_ADD_LIBRARY( eclrtl SHARED ${SRCS} )

if(CENTOS_6_BOOST)
  add_dependencies(eclrtl boost-regex)
  target_link_libraries( eclrtl boost-regex)
else()
  target_link_libraries( eclrtl ${Boost_REGEX_LIBRARY} )
endif()

target_link_libraries ( eclrtl 
      jlib
      nbcd
      roxiemem
    )

if (USE_ICU)
    include_directories (
         ${ICU_INCLUDE_DIR}
        )
    target_link_libraries ( eclrtl
          ${ICU_LIBRARIES}
        )
endif ()

if (CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_CLANG)
  SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wno-switch -Wno-unused-parameter -Werror -Wno-delete-non-virtual-dtor -Wno-overloaded-virtual -Wno-deprecated-declarations")
endif()



if (NOT PLUGIN)
  install ( TARGETS eclrtl RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} ARCHIVE DESTINATION componentfiles/cl/lib )
  FOREACH( iFILES
    ${CMAKE_CURRENT_SOURCE_DIR}/eclinclude4.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/eclrtl.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/eclrtl_imp.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rtlbcd.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rtldistr.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rtlds_imp.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rtlfield.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rtlkey.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rtlrecord.hpp
  )
    install ( FILES ${iFILES} DESTINATION componentfiles/cl/include COMPONENT Runtime CALC_DEPS )
  ENDFOREACH ( iFILES )
endif()
