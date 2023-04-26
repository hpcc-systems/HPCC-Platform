################################################################################
#    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.
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

# Component: nlp

#####################################################
# Description:
# ------------
#    Cmake Input File for nlp
#####################################################

project(nlp)

IF(NLP)
    ADD_PLUGIN(nlp)
    if(MAKE_NLP)
        find_package(nlp-engine CONFIG REQUIRED)

        set(CMAKE_CXX_STANDARD 11)

        set(SRCS
            nlp.hpp
            nlp.cpp
            nlp_eng.hpp
            nlp_eng.cpp)

        include_directories(
            ./../../system/include
            ./../../rtl/eclrtl
            ./../../rtl/include
            ./../../common/deftype
            ./../../system/jlib
            ${NLP_ENGINE_INCLUDE_DIR}
            ${NLP_ENGINE_INCLUDE_DIR}/Api
            ${NLP_ENGINE_INCLUDE_DIR}/Api/lite)

        add_definitions(-D_USRDLL -DECL_NLP_EXPORTS)

        HPCC_ADD_LIBRARY(nlp SHARED ${SRCS})
        if(${CMAKE_VERSION} VERSION_LESS "2.8.9")
            message(WARNING "Cannot set NO_SONAME. shlibdeps will give warnings when package is installed")
        elseif(NOT APPLE)
            set_target_properties(nlp PROPERTIES NO_SONAME 1)
        endif()

        target_compile_definitions(nlp PUBLIC NLP_HAVE_ALLOCA)

        if (WIN32)
            install(RUNTIME_DEPENDENCY_SET nlp_deps
                DESTINATION ${EXEC_DIR} 
                PRE_EXCLUDE_REGEXES "api-ms-win-.*\.dll"
                POST_INCLUDE_REGEXES "^${VCPKG_FILES_DIR}.*"
                POST_EXCLUDE_REGEXES ".*")
        else()
            install(RUNTIME_DEPENDENCY_SET nlp_deps
                DESTINATION ${LIB_DIR} 
                DIRECTORIES "${VCPKG_FILES_DIR}/vcpkg_installed"
                POST_INCLUDE_REGEXES "^${VCPKG_FILES_DIR}\/vcpkg_installed\/.*/libnlp-engine.*"
                POST_EXCLUDE_REGEXES ".*")
        endif()

        install(
            TARGETS nlp
            RUNTIME_DEPENDENCY_SET nlp_deps
            DESTINATION plugins)

        target_link_libraries(nlp PRIVATE
            eclrtl
            jlib
            nlp-engine
        )

        if(PLATFORM)
            install(
                DIRECTORY ${nlp-engine_DIR}/analyzers
                DESTINATION plugins/nlp/nlp-engine
                COMPONENT Runtime)
            install(
                DIRECTORY ${nlp-engine_DIR}/data
                DESTINATION plugins/nlp/nlp-engine
                COMPONENT Runtime)
        endif()
    endif()
endif()

if(PLATFORM OR CLIENTTOOLS_ONLY)
    install(
    FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_nlp.ecllib
    DESTINATION plugins
    COMPONENT Runtime)
endif()
