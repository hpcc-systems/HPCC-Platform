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

if(H3)
    ADD_PLUGIN(H3)
    if(MAKE_H3)
        find_package(h3 CONFIG REQUIRED)

        set(SRCS
            h3.hpp
            h3.cpp
            )

        include_directories(
            ./../../system/include
            ./../../rtl/eclrtl
            ./../../rtl/include
            ./../../common/deftype
            ./../../system/jlib
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
            DESTINATION plugins
            CALC_DEPS)

        target_link_libraries(h3
            eclrtl
            jlib
            h3::h3
            ${EXAMPLE_PLUGIN_DEP_LIBRARIES})
    endif()
endif()

if(PLATFORM OR CLIENTTOOLS_ONLY)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_h3.ecllib
        DESTINATION plugins
        COMPONENT Runtime)
endif()
