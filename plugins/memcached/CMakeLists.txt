################################################################################
#    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.
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


# Component: memcached

#####################################################
# Description:
# ------------
#    Cmake Input File for memcached
#####################################################

project( memcached )

if (USE_MEMCACHED)
  ADD_PLUGIN(memcached PACKAGES MEMCACHED OPTION MAKE_MEMCACHED)
  if ( MAKE_MEMCACHED )
    set (    SRCS
             memcachedplugin.hpp
             memcachedplugin.cpp
        )

    include_directories (
             ./../../system/include
             ./../../rtl/eclrtl
             ./../../rtl/include
             ./../../rtl/nbcd
             ./../../common/deftype
             ./../../common/workunit
             ./../../ecl/hql
             ./../../system/jlib
             ${LIBMEMCACHED_INCLUDE_DIR}
        )

    ADD_DEFINITIONS( -D_USRDLL -DECL_MEMCACHED_EXPORTS)
    SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  -std=gnu++0x")

    HPCC_ADD_LIBRARY( memcached SHARED ${SRCS} )
    if (${CMAKE_VERSION} VERSION_LESS "2.8.9")
      message("WARNING: Cannot set NO_SONAME. shlibdeps will give warnings when package is installed")
    else()
      set_target_properties( memcached PROPERTIES NO_SONAME 1 )
    endif()

    install ( TARGETS memcached DESTINATION plugins )

    target_link_libraries ( memcached
        eclrtl
        jlib
        workunit
        ${LIBMEMCACHED_LIBRARIES}
        )
  endif()
endif()
#Even if not making the memcached plugin, we want to install the header
install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/memcached.ecllib DESTINATION plugins COMPONENT Runtime)
