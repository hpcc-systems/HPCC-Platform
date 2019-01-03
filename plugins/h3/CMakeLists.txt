################################################################################
#    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.
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

# Component: h3

#####################################################
# Description:
# ------------
#    Cmake Input File for h3
#####################################################

project(h3)

if(H3)
    include("uber-h3/cmake/CheckAlloca.cmake")
    include("uber-h3/cmake/CheckVLA.cmake")

    check_alloca(have_alloca)
    check_vla(have_vla)

    set(SRCS
        ./uber-h3/src/h3lib/lib/algos.c

        h3.hpp
        h3.cpp)

    include_directories(
        ./../../system/include
        ./../../rtl/eclrtl
        ./../../rtl/include
        ./../../common/deftype
        ./../../system/jlib
        ./uber-h3/src/h3lib/include
        )

    add_definitions(-D_USRDLL -DECL_H3_EXPORTS)

    HPCC_ADD_LIBRARY(h3 SHARED ${SRCS} ${UBER_H3_SOURCE_FILES})
    if(${CMAKE_VERSION} VERSION_LESS "2.8.9")
        message(WARNING "Cannot set NO_SONAME. shlibdeps will give warnings when package is installed")
    elseif(NOT APPLE)
        set_target_properties(h3 PROPERTIES NO_SONAME 1)
    endif()

    if(have_alloca)
        target_compile_definitions(h3 PUBLIC H3_HAVE_ALLOCA)
    endif()
    if(have_vla)
        target_compile_definitions(h3 PUBLIC H3_HAVE_VLA)
    endif()

    install(
        TARGETS h3
        DESTINATION plugins)

    target_link_libraries(h3
        eclrtl
        jlib
        ${EXAMPLE_PLUGIN_DEP_LIBRARIES})
endif()

if(PLATFORM)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_h3.ecllib
        DESTINATION plugins
        COMPONENT Runtime)
endif()
