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


# Component: eclblas

#####################################################
# Description:
# ------------
#    Cmake Input File for eclblas
#####################################################


project(eclblas)

if(ECLBLAS)
    ADD_PLUGIN(eclblas)
    if(MAKE_ECLBLAS)
        find_package(OpenBLAS REQUIRED)

        add_definitions(-D_USE_CBLAS)
   
        set(SRCS
            dasum.cpp
            daxpy.cpp
            dgemm.cpp
            dgetf2.cpp
            dpotf2.cpp
            dscal.cpp
            dsyrk.cpp
            dtrsm.cpp
            eclblas.cpp
            extract_tri.cpp
            make_diag.cpp)

        include_directories(
            ./../../system/include
            ./../../rtl/eclrtl
            ./../../rtl/include
            ./../../common/deftype
            ./../../system/jlib
            ${OpenBLAS_DIR})

        ADD_DEFINITIONS(-D_USRDLL -DECLBLAS_EXPORTS)

        HPCC_ADD_LIBRARY(eclblas SHARED ${SRCS})
        install ( TARGETS eclblas RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} CALC_DEPS)  # No need to put in plugins dir
        target_link_libraries(eclblas
            eclrtl
            OpenBLAS::OpenBLAS)
    else()
        message(AUTHOR_WARNING "Not building eclblas library for standard library due to lacking libcblas")
    endif()
endif()

if(PLATFORM OR CLIENTTOOLS_ONLY)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_eclblas.ecllib
        DESTINATION plugins
        COMPONENT Runtime)
endif()
