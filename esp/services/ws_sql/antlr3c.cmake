################################################################################
#    HPCC SYSTEMS software Copyright(C) 2017 HPCC Systems.
#
#    Licensed under the Apache License, Version 2.0(the "License");
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

# generate third party library for inclusion in wssql project

set(ANTLRcCONFIGURE_COMMAND_PARAMS "--silent" "--disable-antlrdebug")
if(WIN32)
    set(ANTLR3c_lib "antlr3c.lib")
elseif(APPLE)
    set(ANTLR3c_lib "libantlr3c.dylib")
else()
    set(ANTLR3c_lib "libantlr3c.so")
endif()

if(UNIX)
    if(${ARCH64BIT} EQUAL 1)
        set(ANTLRcCONFIGURE_COMMAND_PARAMS ${ANTLRcCONFIGURE_COMMAND_PARAMS} "--enable-64bit")
        set(osdir "x86_64-linux-gnu")
    else()
        set(osdir "i386-linux-gnu")
    endif()
elseif(WIN32)
    set(osdir "lib")
else()
    set(osdir "unknown")
endif()

include(ExternalProject)
ExternalProject_Add(
    antlr3c
    DOWNLOAD_COMMAND cp -r ${CMAKE_CURRENT_SOURCE_DIR}/libantlr3c ${CMAKE_CURRENT_BINARY_DIR}/antlr3c/src
    SOURCE_DIR ${CMAKE_CURRENT_BINARY_DIR}/antlr3c/src/libantlr3c
    CONFIGURE_COMMAND ${CMAKE_CURRENT_BINARY_DIR}/antlr3c/src/libantlr3c/configure ${ANTLRcCONFIGURE_COMMAND_PARAMS} --prefix=${CMAKE_CURRENT_BINARY_DIR}/antlr3c 2> antlr.cfg.err
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/antlr3c
    BUILD_COMMAND ${CMAKE_MAKE_PROGRAM}
    BUILD_BYPRODUCTS ${CMAKE_CURRENT_BINARY_DIR}/antlr3c/lib/${ANTLR3c_lib}
    BUILD_IN_SOURCE 1
    )

set(libantlr3c_includes
    ${CMAKE_CURRENT_BINARY_DIR}/antlr3c/include
    )

add_library(libantlr3c SHARED IMPORTED GLOBAL)
set_property(TARGET libantlr3c PROPERTY IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/antlr3c/lib/${ANTLR3c_lib})
add_dependencies(libantlr3c antlr3c)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/antlr3c/lib/${ANTLR3c_lib}
    DESTINATION ${LIB_DIR}/external
    COMPONENT Runtime
    )
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/antlr3c/src/libantlr3c/COPYING
    DESTINATION ${LIB_DIR}/external
    COMPONENT Runtime
    RENAME antlr3c-bsd-license.txt
    )
