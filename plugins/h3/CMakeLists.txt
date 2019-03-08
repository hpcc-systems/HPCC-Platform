################################################################################
#    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.
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

if(USE_H3)

    set (CMAKE_C_STANDARD 99)

    set(SRCS
        h3.hpp
        h3.cpp
        uber-h3/src/h3lib/lib/algos.c
        uber-h3/src/h3lib/lib/coordijk.c
        uber-h3/src/h3lib/lib/bbox.c
        uber-h3/src/h3lib/lib/polygon.c
        uber-h3/src/h3lib/lib/h3Index.c
        uber-h3/src/h3lib/lib/vec2d.c
        uber-h3/src/h3lib/lib/vec3d.c
        uber-h3/src/h3lib/lib/linkedGeo.c
        uber-h3/src/h3lib/lib/localij.c
        uber-h3/src/h3lib/lib/geoCoord.c
        uber-h3/src/h3lib/lib/h3UniEdge.c
        uber-h3/src/h3lib/lib/mathExtensions.c
        uber-h3/src/h3lib/lib/vertexGraph.c
        uber-h3/src/h3lib/lib/faceijk.c
        uber-h3/src/h3lib/lib/baseCells.c
        )

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

    target_compile_definitions(h3 PUBLIC H3_HAVE_ALLOCA)

    install(
        TARGETS h3
        DESTINATION plugins)

    target_link_libraries(h3
        eclrtl
        jlib
        ${EXAMPLE_PLUGIN_DEP_LIBRARIES})

endif(USE_H3)

if(PLATFORM OR CLIENTTOOLS_ONLY)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_h3.ecllib
        DESTINATION plugins
        COMPONENT Runtime)
endif()
