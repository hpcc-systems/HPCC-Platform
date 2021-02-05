################################################################################
#    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.
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

# Component: kafka

#####################################################
# Description:
# ------------
#    Cmake Input File for kafka
#####################################################

project(kafka)

if(KAFKA)
    ADD_PLUGIN(kafka)
    if(MAKE_KAFKA)
        # librdkafka packages do not include everything we need to properly
        # build against it; until/if they are ever fixed we will need to build
        # our own version of librdkafka from source and include it ourselves

        if(NOT EXISTS "${PROJECT_SOURCE_DIR}/librdkafka/configure")
            message(FATAL_ERROR
"   The librdkafka submodule is not available.
   This normally indicates that the git submodule has not been fetched.
   Please run git submodule update --init --recursive")
        endif()

        set(LIBRDKAFKA_BUILD_DIR "${PROJECT_BINARY_DIR}/../../build")

        if(APPLE)
            set(LIBRDKAFKA_LIB ${LIBRDKAFKA_BUILD_DIR}/lib/librdkafka.dylib)
            set(LIBRDKAFKA_LIB_REAL ${LIBRDKAFKA_BUILD_DIR}/lib/librdkafka.1.dylib)
            set(LIBRDKAFKACPP_LIB ${LIBRDKAFKA_BUILD_DIR}/lib/librdkafka++.dylib)
            set(LIBRDKAFKACPP_LIB_REAL ${LIBRDKAFKA_BUILD_DIR}/lib/librdkafka++.1.dylib)
        else()
            set(LIBRDKAFKA_LIB ${LIBRDKAFKA_BUILD_DIR}/lib/librdkafka.so)
            set(LIBRDKAFKA_LIB_REAL ${LIBRDKAFKA_BUILD_DIR}/lib/librdkafka.so.1)
            set(LIBRDKAFKACPP_LIB ${LIBRDKAFKA_BUILD_DIR}/lib/librdkafka++.so)
            set(LIBRDKAFKACPP_LIB_REAL ${LIBRDKAFKA_BUILD_DIR}/lib/librdkafka++.so.1)
        endif()

        # librdkafka does not support out-of-source builds, so let's copy all
        # of its source code to our binary directory and build there; further,
        # we need to pull some working directory shenanigans for each command
        # in order to make the built scripts function correctly

        add_custom_target(librdkafka-build ALL
            COMMAND mkdir -p ${LIBRDKAFKA_BUILD_DIR}
            # If we've copied the source files before, compute checksums
            COMMAND if [ -d ${LIBRDKAFKA_BUILD_DIR}/src ]\; then md5sum ${LIBRDKAFKA_BUILD_DIR}/src/*/*.c ${LIBRDKAFKA_BUILD_DIR}/src/*/*.cpp ${LIBRDKAFKA_BUILD_DIR}/src/*/*.h > ${LIBRDKAFKA_BUILD_DIR}/src/_md5.txt\; fi
            COMMAND echo "Copying librdkafka source files" && cp -rp ${PROJECT_SOURCE_DIR}/librdkafka ${LIBRDKAFKA_BUILD_DIR}/src
            # Run configure if it hasn't been run before
            COMMAND if [ ! -f ${LIBRDKAFKA_BUILD_DIR}/src/Makefile.config ]\; then echo "Configuring librdkafka" && cd ${LIBRDKAFKA_BUILD_DIR}/src && ./configure --prefix=${LIBRDKAFKA_BUILD_DIR}\; fi
            # If either there is no checksum file or if the checksums are different, re-make librdkafka
            COMMAND if [ ! -f ${LIBRDKAFKA_BUILD_DIR}/src/_md5.txt ] || ! md5sum --quiet --status -c ${LIBRDKAFKA_BUILD_DIR}/src/_md5.txt\; then echo "Building librdkafka" && cd ${LIBRDKAFKA_BUILD_DIR}/src && make libs && make install\; fi
            COMMAND rm -f ${LIBRDKAFKA_BUILD_DIR}/src/_md5.txt
            COMMENT Copying and building librdkafka)

        install(CODE "set(ENV{LD_LIBRARY_PATH} \"\$ENV{LD_LIBRARY_PATH}:${LIBRDKAFKA_BUILD_DIR}/lib\")")

        add_library(librdkafka SHARED IMPORTED)
        set_property(TARGET librdkafka PROPERTY IMPORTED_LOCATION ${LIBRDKAFKA_LIB})
        add_dependencies(librdkafka librdkafka-build)

        add_library(librdkafkacpp SHARED IMPORTED)
        set_property(TARGET librdkafkacpp PROPERTY IMPORTED_LOCATION ${LIBRDKAFKACPP_LIB})
        add_dependencies(librdkafkacpp librdkafka-build)

        set(
            SRCS
            kafka.hpp
            kafka.cpp)

        include_directories(
            ./../../system/include
            ./../../rtl/eclrtl
            ./../../rtl/include
            ./../../common/deftype
            ./../../system/jlib
            ${LIBRDKAFKA_BUILD_DIR}/include
            ${CMAKE_BINARY_DIR}
            ${CMAKE_BINARY_DIR}/oss)

        add_definitions(-D_USRDLL -DECL_KAFKA_EXPORTS)
        HPCC_ADD_LIBRARY(kafka SHARED ${SRCS})

        if(${CMAKE_VERSION} VERSION_LESS "2.8.9")
            message(WARNING "Cannot set NO_SONAME. shlibdeps will give warnings when package is installed")
        elseif(NOT APPLE)
            set_target_properties(kafka PROPERTIES NO_SONAME 1)
        endif()

        install(
            TARGETS kafka
            DESTINATION plugins)

        install(
            FILES ${LIBRDKAFKA_LIB_REAL} ${LIBRDKAFKACPP_LIB_REAL}
            DESTINATION ${LIB_DIR}
            PERMISSIONS OWNER_WRITE OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
            COMPONENT Runtime)

        install(
            FILES ${LIBRDKAFKA_LIB} ${LIBRDKAFKACPP_LIB}
            DESTINATION ${LIB_DIR}
            COMPONENT Runtime)

        target_link_libraries(
            kafka
            librdkafka
            librdkafkacpp
            eclrtl
            jlib
            ${ZLIB_LIBRARIES})
    endif()
endif()

if(PLATFORM OR CLIENTTOOLS_ONLY)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/kafka.ecllib
        DESTINATION plugins
        COMPONENT Runtime)
endif()
