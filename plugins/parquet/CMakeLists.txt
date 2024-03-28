##############################################################################

#    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.

#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at

#       http://www.apache.org/licenses/LICENSE-2.0

#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
##############################################################################

# Component: parquetembed

#############################################################
# Description:
# -----------
# Cmake Input File for parquetembed
#############################################################

project(parquetembed)
message("CMAKE Version: ${CMAKE_VERSION}")

if(PARQUETEMBED OR USE_PARQUET)
    ADD_PLUGIN(parquetembed)
    if(MAKE_PARQUETEMBED OR USE_PARQUET)
        find_package(Arrow CONFIG REQUIRED)
        find_package(Parquet CONFIG REQUIRED)
        find_package(ArrowDataset CONFIG REQUIRED)

        set(
            SRCS
            parquetembed.cpp
        )

        INCLUDE_DIRECTORIES(
            ${HPCC_SOURCE_DIR}/system/include
            ${HPCC_SOURCE_DIR}/rtl/eclrtl
            ${HPCC_SOURCE_DIR}/rtl/include
            ${HPCC_SOURCE_DIR}/rtl/nbcd
            ${HPCC_SOURCE_DIR}/common/deftype
            ${HPCC_SOURCE_DIR}/system/jlib
            ${HPCC_SOURCE_DIR}/roxie/roxiemem
        )

        add_definitions(-D_USRDLL -DPARQUETEMBED_PLUGIN_EXPORTS)

        HPCC_ADD_LIBRARY(parquetembed SHARED ${SRCS})

        install(
            TARGETS parquetembed
            DESTINATION plugins CALC_DEPS
        )

        install(
            TARGETS parquetembed
            RUNTIME DESTINATION ${EXEC_DIR}
            LIBRARY DESTINATION ${LIB_DIR}
            COMPONENT Runtime CALC_DEPS
        )

        target_link_libraries(
            parquetembed
            eclrtl
            jlib
            "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Arrow::arrow_static,Arrow::arrow_shared>"
            "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Parquet::parquet_static,Parquet::parquet_shared>"
            "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,ArrowDataset::arrow_dataset_static,ArrowDataset::arrow_dataset_shared>"
        )
    endif()
endif()

if(PLATFORM OR CLIENTTOOLS_ONLY)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/parquet.ecllib
        DESTINATION plugins
        COMPONENT Runtime
    )
endif()
