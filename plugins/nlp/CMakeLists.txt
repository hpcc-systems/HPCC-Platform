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
SET(CMAKE_UNITY_BUILD FALSE)

if(USE_NLP)

    # too many warnings about register qualifiers, and clashes of std::byte with the definition of byte in platform.h
    set(CMAKE_CXX_STANDARD 11)

    include_directories(nlp-engine/cs/include)
    include_directories(nlp-engine/include)
    include_directories(nlp-engine/include/Api)
    include_directories(nlp-engine/include/Api/lite)

    add_subdirectory(nlp-engine/cs)
    add_subdirectory(nlp-engine/src)
    add_subdirectory(nlp-engine/lite)

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
        ./nlp-engine/include/Api
        ./nlp-engine)

    add_definitions(-D_USRDLL -DECL_NLP_EXPORTS)

    HPCC_ADD_LIBRARY(nlp SHARED ${SRCS} ${UBER_NLP_SOURCE_FILES})
    if(${CMAKE_VERSION} VERSION_LESS "2.8.9")
        message(WARNING "Cannot set NO_SONAME. shlibdeps will give warnings when package is installed")
    elseif(NOT APPLE)
        set_target_properties(nlp PROPERTIES NO_SONAME 1)
    endif()

    target_compile_definitions(nlp PUBLIC NLP_HAVE_ALLOCA)

    install(
        TARGETS nlp
        DESTINATION plugins
        CALC_DEPS)

    target_link_libraries(nlp
        eclrtl
        jlib
        prim
        kbm
        consh
        words
        lite
        ${EXAMPLE_PLUGIN_DEP_LIBRARIES})

    if(PLATFORM OR CLIENTTOOLS_ONLY)

        install(
            FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_nlp.ecllib
            DESTINATION plugins
            COMPONENT Runtime)

    endif()

    if(PLATFORM)

        install(
            DIRECTORY ./nlp-engine/analyzers ./nlp-engine/data
            DESTINATION plugins/nlp/nlp-engine
            COMPONENT Runtime)

    endif()

endif(USE_NLP)
