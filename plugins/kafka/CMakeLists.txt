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

project( kafka )

if (USE_KAFKA)

    ADD_PLUGIN(kafka PACKAGES OPTION MAKE_KAFKA)

    if ( MAKE_KAFKA )

        # librdkafka packages do not include everything we need to properly
        # build against it; until/if they are ever fixed we will need to build
        # our own version of librdkafka from source and include it ourselves

        if( NOT EXISTS "${PROJECT_SOURCE_DIR}/librdkafka/configure" )
            message( FATAL_ERROR
"   The librdkafka submodule is not available.
   This normally indicates that the git submodule has not been fetched.
   Please run git submodule update --init --recursive")
        endif()

        if (APPLE)
            set ( LIBRDKAFKA_LIB ${PROJECT_BINARY_DIR}/lib/librdkafka.dylib )
            set ( LIBRDKAFKA_LIB_REAL ${PROJECT_BINARY_DIR}/lib/librdkafka.1.dylib )
            set ( LIBRDKAFKACPP_LIB ${PROJECT_BINARY_DIR}/lib/librdkafka++.dylib )
            set ( LIBRDKAFKACPP_LIB_REAL ${PROJECT_BINARY_DIR}/lib/librdkafka++.1.dylib )
        else()
            set ( LIBRDKAFKA_LIB ${PROJECT_BINARY_DIR}/lib/librdkafka.so )
            set ( LIBRDKAFKA_LIB_REAL ${PROJECT_BINARY_DIR}/lib/librdkafka.so.1 )
            set ( LIBRDKAFKACPP_LIB ${PROJECT_BINARY_DIR}/lib/librdkafka++.so )
            set ( LIBRDKAFKACPP_LIB_REAL ${PROJECT_BINARY_DIR}/lib/librdkafka++.so.1 )
        endif()

        # librdkafka does not support out-of-source builds, so let's copy all
        # of its source code to our binary directory and build there; further,
        # we need to pull some working directory shenanigans for each command
        # in order to make the built scripts function correctly

        add_custom_command ( OUTPUT ${LIBRDKAFKA_LIB}
                COMMAND cp -r ${PROJECT_SOURCE_DIR}/librdkafka ${PROJECT_BINARY_DIR}/src
                COMMAND cd ${PROJECT_BINARY_DIR}/src && ./configure --prefix=${PROJECT_BINARY_DIR}
                COMMAND cd ${PROJECT_BINARY_DIR}/src && make && make install
                COMMENT Copying and building librdkafka
            )

        add_custom_target ( librdkafka-build ALL DEPENDS ${LIBRDKAFKA_LIB} )

        # Add both libraries from librdkafka

        add_library ( librdkafka SHARED IMPORTED )
        set_property ( TARGET librdkafka PROPERTY IMPORTED_LOCATION ${LIBRDKAFKA_LIB} )
        add_dependencies ( librdkafka librdkafka-build )

        add_library ( librdkafkacpp STATIC IMPORTED )
        set_property ( TARGET librdkafkacpp PROPERTY IMPORTED_LOCATION ${LIBRDKAFKACPP_LIB} )
        add_dependencies ( librdkafkacpp librdkafka-build )

        set (   SRCS
                kafka.hpp
                kafka.cpp
            )

        include_directories (
                ./../../system/include
                ./../../rtl/eclrtl
                ./../../rtl/include
                ./../../common/deftype
                ./../../system/jlib
                ${PROJECT_BINARY_DIR}/include
                ${CMAKE_BINARY_DIR}
                ${CMAKE_BINARY_DIR}/oss
            )

        ADD_DEFINITIONS( -D_USRDLL -DECL_KAFKA_EXPORTS )
        HPCC_ADD_LIBRARY( kafka SHARED ${SRCS} )

        if (${CMAKE_VERSION} VERSION_LESS "2.8.9")
            message("WARNING: Cannot set NO_SONAME. shlibdeps will give warnings when package is installed")
        elseif(NOT APPLE)
            set_target_properties( kafka PROPERTIES NO_SONAME 1 )
        endif()

        install ( TARGETS kafka
                DESTINATION plugins
            )

        # Install our built librdkafka libraries into the RPM
        install ( FILES ${LIBRDKAFKA_LIB} ${LIBRDKAFKA_LIB_REAL} ${LIBRDKAFKACPP_LIB} ${LIBRDKAFKACPP_LIB_REAL}
                DESTINATION ${LIB_DIR}
                COMPONENT Runtime
            )

        target_link_libraries ( kafka
                librdkafka
                librdkafkacpp
                eclrtl
                jlib
                ${ZLIB_LIBRARIES}
            )

    endif()

endif()

#Even if not making the kafka plugin, we want to install the header
install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/kafka.ecllib DESTINATION plugins COMPONENT Runtime)
