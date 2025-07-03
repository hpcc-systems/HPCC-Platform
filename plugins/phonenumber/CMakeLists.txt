################################################################################
#    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.
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

# Component: PHONENUMBER

#####################################################
# Description:
# ------------
#    Cmake Input File for PHONENUMBER
#    Api for google libphonenumber
#####################################################

project(phonenumber)

if(PHONENUMBER)
    ADD_PLUGIN(PHONENUMBER)
    if(MAKE_PHONENUMBER)
        find_package(libphonenumber CONFIG REQUIRED)
        find_package(Boost REQUIRED COMPONENTS date_time system thread)
        find_package(absl CONFIG REQUIRED)

        set(SRCS
            phonenumber.hpp
            phonenumber.cpp
            )
        
        include_directories(
            ${HPCC_SOURCE_DIR}/system/include
            ${HPCC_SOURCE_DIR}/rtl/eclrtl
            ${HPCC_SOURCE_DIR}/rtl/include
            ${HPCC_SOURCE_DIR}/common/deftype
            ${HPCC_SOURCE_DIR}/system/jlib
            ${libphonenumber_INCLUDE_DIRS}
        )

        add_definitions(-D_USRDLL -DECL_PHONENUMBER_EXPORTS)

        HPCC_ADD_LIBRARY(phonenumber SHARED ${SRCS})
        if(NOT APPLE)
            set_target_properties(phonenumber PROPERTIES NO_SONAME 1)
        endif()

        target_compile_definitions(phonenumber PUBLIC PHONENUMBER_HAVE_ALLOCA)

        install(
            TARGETS phonenumber
            DESTINATION plugins
            CALC_DEPS
        )

        target_link_libraries(phonenumber
            PUBLIC
                eclrtl
                jlib
                ${Boost_LIBRARIES}
                libphonenumber::phonenumber
                absl::base
                absl::strings
                absl::log_internal_check_op
                absl::log_internal_message
        )
    endif()
endif()

if(PLATFORM OR CLIENTTOOLS_ONLY)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_phonenumber.ecllib
        DESTINATION plugins
        COMPONENT Runtime
    )
endif()
