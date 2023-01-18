################################################################################
#    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.
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

project(redis)

if(REDIS)
    ADD_PLUGIN(redis)
    if(MAKE_REDIS)
        find_package(hiredis CONFIG REQUIRED)    

        set(
            SRCS
            redis.hpp
            redis.cpp)

        include_directories(
            ./../../system/include
            ./../../rtl/eclrtl
            ./../../rtl/include
            ./../../common/deftype
            ./../../system/jlib
            )

        add_definitions(-D_USRDLL -DECL_REDIS_EXPORTS)

        HPCC_ADD_LIBRARY(redis SHARED ${SRCS})
        if(${CMAKE_VERSION} VERSION_LESS "2.8.9")
            message(WARNING "Cannot set NO_SONAME. shlibdeps will give warnings when package is installed")
        elseif(NOT APPLE)
            set_target_properties(redis PROPERTIES NO_SONAME 1)
        endif()

        install(
            TARGETS redis
            DESTINATION plugins
            CALC_DEPS)

        target_link_libraries(
            redis
            eclrtl
            jlib
            hiredis::hiredis
            )
    endif()
endif()

if(PLATFORM OR CLIENTTOOLS_ONLY)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_redis.ecllib
        DESTINATION plugins
        COMPONENT Runtime)
endif()
