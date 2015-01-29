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

# Component: redis

#####################################################
# Description:
# ------------
#    Cmake Input File for redis
#####################################################

project( redis )

if (USE_REDIS)
  ADD_PLUGIN(redis PACKAGES REDIS OPTION MAKE_REDIS)
  if ( MAKE_REDIS )
    set (    SRCS
             redisplugin.hpp
             redissync.hpp
             redisasync.hpp

             redisplugin.cpp
             redissync.cpp
             redisasync.cpp
        )

    include_directories (
             ./../../system/include
             ./../../rtl/eclrtl
             ./../../rtl/include
             ./../../common/deftype
             ./../../system/jlib
             ${LIBHIREDIS_INCLUDE_DIRS}
        )

    ADD_DEFINITIONS( -D_USRDLL -DECL_REDIS_EXPORTS)

    HPCC_ADD_LIBRARY( redis SHARED ${SRCS} )
    if (${CMAKE_VERSION} VERSION_LESS "2.8.9")
      message("WARNING: Cannot set NO_SONAME. shlibdeps will give warnings when package is installed")
    else()
      set_target_properties( redis PROPERTIES NO_SONAME 1 )
    endif()

    install ( TARGETS redis DESTINATION plugins)

    target_link_libraries ( redis
        eclrtl
        jlib
        ${LIBHIREDIS_LIBRARIES}
        )
  endif()
endif()

#Even if not making the redis plugin, we want to install the header
install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_redis.ecllib DESTINATION plugins COMPONENT Runtime)
