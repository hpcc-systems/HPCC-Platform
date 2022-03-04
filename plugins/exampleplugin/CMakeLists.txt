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

# Component: exampleplugin

#####################################################
# Description:
# ------------
#    Cmake Input File for exampleplugin
#####################################################

project(exampleplugin)

if(EXAMPLEPLUGIN)
    #ADD_PLUGIN(...) executes HPCC/cmake_modules/FindEXAMPLE_PLUGIN_DEP.cmake to find any required DEPendencies such
    #as include files and libraries to link against. MAKE_EXAMPLEPLUGIN is set true if all the required dependencies
    #set in HPCC/cmake_modules/FindEXAMPLE_PLUGIN_DEP.cmake are met.
    #Both EXAMPLE_PLUGIN_DEP_INCLUDE_DIR and EXAMPLE_PLUGIN_DEP_LIBRARIES are also found and set in
    #HPCC/cmake_modules/FindEXAMPLE_PLUGIN_DEP.cmake.
    ADD_PLUGIN(exampleplugin PACKAGES EXAMPLE_PLUGIN_DEP MINVERSION 0.1.0)

    if(MAKE_EXAMPLEPLUGIN)
        set(
            SRCS
            #Add all sources to be compiled here.
            exampleplugin.hpp
            exampleplugin.cpp)

        include_directories(
            #Add any required HPCC include dirs here. EXAMPLE_PLUGIN_DEP_INCLUDE_DIR contains any and all external (non HPCC) paths.
            ./../../system/include
            ./../../rtl/eclrtl
            ./../../rtl/include
            ./../../common/deftype
            ./../../system/jlib
            ${EXAMPLE_PLUGIN_DEP_INCLUDE_DIR})

        add_definitions(-D_USRDLL -DECL_EXAMPLE_PLUGIN_EXPORTS)

        HPCC_ADD_LIBRARY(exampleplugin SHARED ${SRCS})
        if(${CMAKE_VERSION} VERSION_LESS "2.8.9")
            message(WARNING "Cannot set NO_SONAME. shlibdeps will give warnings when package is installed")
        elseif(NOT APPLE)
            set_target_properties(exampleplugin PROPERTIES NO_SONAME 1)
        endif()

        install(
            TARGETS exampleplugin
            DESTINATION plugins)

        target_link_libraries(
            exampleplugin
            #Add any required HPCC libraries to be linked against here. EXAMPLE_PLUGIN_DEP_LIBRARIES contains any and all external (non HPCC) libraries.
            eclrtl
            jlib
            ${EXAMPLE_PLUGIN_DEP_LIBRARIES})
    endif()
endif()

#Here the ECL plugin/service definitions contained with *.ecllib are set to be installed with the platform regardless
#of whether the plugin itself was configured to be built. This is because the plugin libraries are dynamically linked
#with the platform such that ECL queries can be compiled without the plugin needing to be installed locally, e.g. as
#it the case for eclserver. Adequate tests must be conducted to ensure against undefined references. In addition,
#this means that any changes to the plugin/service definitions must be correlated with the correct platform build
#version when installing.
if(PLATFORM)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_exampleplugin.ecllib
        DESTINATION plugins
        COMPONENT Runtime)
endif()
