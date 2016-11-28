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
    add_custom_command(OUTPUT ${PROJECT_BINARY_DIR}/libhiredis.so.0.13
        COMMAND make dynamic
        COMMAND mv libhiredis.so ${PROJECT_BINARY_DIR}/libhiredis.so.0.13
        COMMAND make clean
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/hiredis
        COMMENT Building hiredis c library)
    add_custom_command(OUTPUT ${PROJECT_BINARY_DIR}/libhiredis.so
        COMMAND ln -sf libhiredis.so.0.13 libhiredis.so
        DEPENDS ${PROJECT_BINARY_DIR}/libhiredis.so.0.13
        WORKING_DIRECTORY ${PROJECT_BINARY_DIR})
    add_custom_target(hiredis-build ALL
        DEPENDS ${PROJECT_BINARY_DIR}/libhiredis.so)

    install(CODE "set(ENV{LD_LIBRARY_PATH} \"\$ENV{LD_LIBRARY_PATH}:${PROJECT_BINARY_DIR}\")")

    add_library(hiredis SHARED IMPORTED)
    set_property(TARGET hiredis PROPERTY IMPORTED_LOCATION ${PROJECT_BINARY_DIR}/libhiredis.so)
    add_dependencies(hiredis hiredis-build)

    install(FILES
        ${PROJECT_BINARY_DIR}/libhiredis.so
        ${PROJECT_BINARY_DIR}/libhiredis.so.0.13
        DESTINATION ${LIB_DIR}
        PERMISSIONS OWNER_WRITE OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
        COMPONENT Runtime)

    install(FILES
        ${PROJECT_SOURCE_DIR}/hiredis/COPYING
        DESTINATION ${LIB_DIR}
        COMPONENT Runtime
        RENAME libhiredis.copywrite)

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
        ./hiredis)

    add_definitions(-D_USRDLL -DECL_REDIS_EXPORTS)

    HPCC_ADD_LIBRARY(redis SHARED ${SRCS})
    if(${CMAKE_VERSION} VERSION_LESS "2.8.9")
        message(WARNING "Cannot set NO_SONAME. shlibdeps will give warnings when package is installed")
    elseif(NOT APPLE)
        set_target_properties(redis PROPERTIES NO_SONAME 1)
    endif()

    install(
        TARGETS redis
        DESTINATION plugins)

    target_link_libraries(
        redis
        eclrtl
        jlib
        hiredis)
endif()

if(PLATFORM OR CLIENTTOOLS_ONLY)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_redis.ecllib
        DESTINATION plugins
        COMPONENT Runtime)
endif()
