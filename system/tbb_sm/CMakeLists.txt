################################################################################
#    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems.
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

# Component: tbb

#####################################################
# Description:
# ------------
#    Cmake Input File for tbb, tbbmalloc, tbbproxy
#####################################################

project( tbb )

if(USE_TBB OR USE_TBBMALLOC)
    # tbb target
    execute_process(
        COMMAND /bin/bash -c "grep TBB_COMPATIBLE_INTERFACE_VERSION tbb_stddef.h | egrep -o [0-9.]+"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/tbb/include/tbb
        OUTPUT_VARIABLE SONAME_VER
        OUTPUT_STRIP_TRAILING_WHITESPACE
        )
    
    execute_process(
        COMMAND /bin/bash -c "${CMAKE_MAKE_PROGRAM} info | awk 'BEGIN{FS=\"=\";} /^tbb_build_prefix/{print $2;}'"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/tbb
        OUTPUT_VARIABLE TBB_BUILD_NAME
        OUTPUT_STRIP_TRAILING_WHITESPACE
        )

    set(tbbcleandirs ${TBB_BUILD_NAME}_debug ${TBB_BUILD_NAME}_release)
    SET_DIRECTORY_PROPERTIES(PROPERTIES ADDITIONAL_MAKE_CLEAN_FILES "${tbbcleandirs}")

    set(TBB_BUILD_NAME "${TBB_BUILD_NAME}_release")

    add_custom_command(
        OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${TBB_BUILD_NAME}/libtbbmalloc_proxy${CMAKE_SHARED_LIBRARY_SUFFIX}.${SONAME_VER}
               ${CMAKE_CURRENT_BINARY_DIR}/${TBB_BUILD_NAME}/libtbbmalloc_proxy${CMAKE_SHARED_LIBRARY_SUFFIX}
               ${CMAKE_CURRENT_BINARY_DIR}/${TBB_BUILD_NAME}/libtbbmalloc${CMAKE_SHARED_LIBRARY_SUFFIX}.${SONAME_VER}
               ${CMAKE_CURRENT_BINARY_DIR}/${TBB_BUILD_NAME}/libtbbmalloc${CMAKE_SHARED_LIBRARY_SUFFIX}
               ${CMAKE_CURRENT_BINARY_DIR}/${TBB_BUILD_NAME}/libtbb${CMAKE_SHARED_LIBRARY_SUFFIX}.${SONAME_VER}
               ${CMAKE_CURRENT_BINARY_DIR}/${TBB_BUILD_NAME}/libtbb${CMAKE_SHARED_LIBRARY_SUFFIX}
        #COMMAND ${CMAKE_MAKE_PROGRAM} tbb_build_dir=${CMAKE_CURRENT_BINARY_DIR} clean
        COMMAND ${CMAKE_MAKE_PROGRAM} tbb_build_dir=${CMAKE_CURRENT_BINARY_DIR} cfg=release
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/tbb
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/tbb
        )

    add_custom_target(tbb
        DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/${TBB_BUILD_NAME}/libtbbmalloc_proxy${CMAKE_SHARED_LIBRARY_SUFFIX}.${SONAME_VER}
                ${CMAKE_CURRENT_BINARY_DIR}/${TBB_BUILD_NAME}/libtbbmalloc_proxy${CMAKE_SHARED_LIBRARY_SUFFIX}
                ${CMAKE_CURRENT_BINARY_DIR}/${TBB_BUILD_NAME}/libtbbmalloc${CMAKE_SHARED_LIBRARY_SUFFIX}.${SONAME_VER}
                ${CMAKE_CURRENT_BINARY_DIR}/${TBB_BUILD_NAME}/libtbbmalloc${CMAKE_SHARED_LIBRARY_SUFFIX}
                ${CMAKE_CURRENT_BINARY_DIR}/${TBB_BUILD_NAME}/libtbb${CMAKE_SHARED_LIBRARY_SUFFIX}.${SONAME_VER}
                ${CMAKE_CURRENT_BINARY_DIR}/${TBB_BUILD_NAME}/libtbb${CMAKE_SHARED_LIBRARY_SUFFIX}
        )

    add_library(libtbb SHARED IMPORTED GLOBAL)
    add_library(libtbbmalloc SHARED IMPORTED GLOBAL)
    add_library(libtbbmalloc_proxy SHARED IMPORTED GLOBAL)
    if (APPLE)
        set_property(
            TARGET libtbb
            PROPERTY IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/${TBB_BUILD_NAME}/libtbb${CMAKE_SHARED_LIBRARY_SUFFIX})
        set_property(
            TARGET libtbbmalloc
            PROPERTY IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/${TBB_BUILD_NAME}/libtbbmalloc${CMAKE_SHARED_LIBRARY_SUFFIX})
        set_property(
            TARGET libtbbmalloc_proxy
            PROPERTY IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/${TBB_BUILD_NAME}/libtbbmalloc_proxy${CMAKE_SHARED_LIBRARY_SUFFIX})
    else()
        set_property(
            TARGET libtbb
            PROPERTY IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/${TBB_BUILD_NAME}/libtbb${CMAKE_SHARED_LIBRARY_SUFFIX}.${SONAME_VER})
        set_property(
            TARGET libtbbmalloc
            PROPERTY IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/${TBB_BUILD_NAME}/libtbbmalloc${CMAKE_SHARED_LIBRARY_SUFFIX}.${SONAME_VER})
        set_property(
            TARGET libtbbmalloc_proxy
            PROPERTY IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/${TBB_BUILD_NAME}/libtbbmalloc_proxy${CMAKE_SHARED_LIBRARY_SUFFIX}.${SONAME_VER})
    endif()

    install(CODE "set(ENV{LD_LIBRARY_PATH} \"\$ENV{LD_LIBRARY_PATH}:${PROJECT_BINARY_DIR}:${PROJECT_BINARY_DIR}/${TBB_BUILD_NAME}\")")
    if(NOT PLUGIN)
        if(USE_TBB)
            if (APPLE)
                install(FILES
                    ${CMAKE_CURRENT_BINARY_DIR}/${TBB_BUILD_NAME}/libtbb${CMAKE_SHARED_LIBRARY_SUFFIX}
                    DESTINATION lib)
            else()
                install(FILES
                    ${CMAKE_CURRENT_BINARY_DIR}/${TBB_BUILD_NAME}/libtbb${CMAKE_SHARED_LIBRARY_SUFFIX}.${SONAME_VER}
                    ${CMAKE_CURRENT_BINARY_DIR}/${TBB_BUILD_NAME}/libtbb${CMAKE_SHARED_LIBRARY_SUFFIX}
                    DESTINATION lib)
            endif()
        endif()
        if(USE_TBBMALLOC)
            if (APPLE)
                install(FILES
                    ${CMAKE_CURRENT_BINARY_DIR}/${TBB_BUILD_NAME}/libtbbmalloc${CMAKE_SHARED_LIBRARY_SUFFIX}
                    DESTINATION lib)
                install(FILES
                    ${CMAKE_CURRENT_BINARY_DIR}/${TBB_BUILD_NAME}/libtbbmalloc_proxy${CMAKE_SHARED_LIBRARY_SUFFIX}
                    DESTINATION lib)
            else()
                install(FILES
                    ${CMAKE_CURRENT_BINARY_DIR}/${TBB_BUILD_NAME}/libtbbmalloc${CMAKE_SHARED_LIBRARY_SUFFIX}.${SONAME_VER}
                    ${CMAKE_CURRENT_BINARY_DIR}/${TBB_BUILD_NAME}/libtbbmalloc${CMAKE_SHARED_LIBRARY_SUFFIX}
                    DESTINATION lib)
                install(FILES
                    ${CMAKE_CURRENT_BINARY_DIR}/${TBB_BUILD_NAME}/libtbbmalloc_proxy${CMAKE_SHARED_LIBRARY_SUFFIX}.${SONAME_VER}
                    ${CMAKE_CURRENT_BINARY_DIR}/${TBB_BUILD_NAME}/libtbbmalloc_proxy${CMAKE_SHARED_LIBRARY_SUFFIX}
                    DESTINATION lib)
            endif()
        endif()
    endif()
endif()
