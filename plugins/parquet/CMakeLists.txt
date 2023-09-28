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

if(PARQUETEMBED)
    ADD_PLUGIN(parquetembed)
    if(MAKE_PARQUETEMBED)
        find_package(Arrow CONFIG REQUIRED)
        find_package(Parquet CONFIG REQUIRED)
        find_package(ArrowDataset CONFIG REQUIRED)

        set(
            SRCS
            parquetembed.cpp
        )

        INCLUDE_DIRECTORIES(
            ${HPCC_SOURCE_DIR}/esp/platform
            ${HPCC_SOURCE_DIR}/system/include
            ${HPCC_SOURCE_DIR}/rtl/eclrtl
            ${HPCC_SOURCE_DIR}/rtl/include
            ${HPCC_SOURCE_DIR}/rtl/nbcd
            ${HPCC_SOURCE_DIR}/common/deftype
            ${HPCC_SOURCE_DIR}/system/jlib
            ${HPCC_SOURCE_DIR}/roxie/roxiemem
        )

        HPCC_ADD_LIBRARY(parquetembed SHARED ${SRCS})

        install(
            TARGETS parquetembed
            DESTINATION plugins CALC_DEPS
        )

        install(
            FILES ${LIBARROW_LIB_REAL}
            DESTINATION ${LIB_DIR} CALC_DEPS
            PERMISSIONS OWNER_WRITE OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
            COMPONENT Runtime)

        install(
            FILES ${LIBARROW_LIB} ${LIBARROW_LIB_ABI}
            DESTINATION ${LIB_DIR} CALC_DEPS
            COMPONENT Runtime)

        install(
            FILES ${LIBPARQUET_LIB_REAL}
            DESTINATION ${LIB_DIR} CALC_DEPS
            PERMISSIONS OWNER_WRITE OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
            COMPONENT Runtime)

        install(
            FILES ${LIBPARQUET_LIB} ${LIBPARQUET_LIB_ABI}
            DESTINATION ${LIB_DIR} CALC_DEPS
            COMPONENT Runtime)

        install(
            FILES ${LIBARRORACERO_LIB_REAL}
            DESTINATION ${LIB_DIR} CALC_DEPS
            PERMISSIONS OWNER_WRITE OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
            COMPONENT Runtime)

        install(
            FILES ${LIBARRORACERO_LIB} ${LIBARROWDATASET_LIB_ABI}
            DESTINATION ${LIB_DIR} CALC_DEPS
            COMPONENT Runtime)

        install(
            FILES ${LIBARROWDATASET_LIB_REAL}
            DESTINATION ${LIB_DIR} CALC_DEPS
            PERMISSIONS OWNER_WRITE OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
            COMPONENT Runtime)

        install(
            FILES ${LIBARROWDATASET_LIB} ${LIBARROWDATASET_LIB_ABI}
            DESTINATION ${LIB_DIR} CALC_DEPS
            COMPONENT Runtime)

        target_link_libraries(
            parquetembed
            eclrtl
            jlib
            Arrow::arrow_shared
            Parquet::parquet_shared
            ArrowDataset::arrow_dataset_shared
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
