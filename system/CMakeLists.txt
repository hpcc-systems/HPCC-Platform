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

HPCC_ADD_SUBDIRECTORY (include)
HPCC_ADD_SUBDIRECTORY (jhtree)
HPCC_ADD_SUBDIRECTORY (yaml)
HPCC_ADD_SUBDIRECTORY (jlib)
HPCC_ADD_SUBDIRECTORY (aws)
HPCC_ADD_SUBDIRECTORY (azure)

if (NOT JLIB_ONLY)
   HPCC_ADD_SUBDIRECTORY (logaccess)
   HPCC_ADD_SUBDIRECTORY (hrpc)
   HPCC_ADD_SUBDIRECTORY (tbb_sm)
   HPCC_ADD_SUBDIRECTORY (mp)
   HPCC_ADD_SUBDIRECTORY (security)
   HPCC_ADD_SUBDIRECTORY (xmllib)
   HPCC_ADD_SUBDIRECTORY (xmllibtest "PLATFORM")
   
   if (USE_AERON)
      project (aeron_include)
      remove_definitions(-fvisibility=hidden)
      if (CMAKE_COMPILER_IS_GNUCXX OR CMAKE_COMPILER_IS_CLANGXX)
          SET (CMAKE_SAVED_AERON_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
          SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-format-nonliteral")
      endif()      
      HPCC_ADD_SUBDIRECTORY (aeron)
      if (CMAKE_COMPILER_IS_GNUCXX OR CMAKE_COMPILER_IS_CLANGXX)
          SET (CMAKE_CXX_FLAGS "${CMAKE_SAVED_AERON_CXX_FLAGS}")
      endif()      
   endif()
endif( )
