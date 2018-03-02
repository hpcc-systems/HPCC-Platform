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
    URL "http://www.antlr3.org/download/C/libantlr3c-3.4.tar.gz"
    DOWNLOAD_DIR ${CMAKE_CURRENT_BINARY_DIR}
    SOURCE_DIR ${CMAKE_CURRENT_BINARY_DIR}/antlr3c/runtime/C
    CONFIGURE_COMMAND ${CMAKE_CURRENT_BINARY_DIR}/antlr3c/runtime/C/configure ${ANTLRcCONFIGURE_COMMAND_PARAMS} --prefix=${CMAKE_CURRENT_BINARY_DIR}/antlr3c
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/antlr3c
    BUILD_COMMAND $(MAKE)
    BUILD_IN_SOURCE 1
    )

add_library(libantlr3c SHARED IMPORTED GLOBAL)
set_property(TARGET libantlr3c PROPERTY IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/antlr3c/lib/${ANTLR3c_lib})
add_dependencies(libantlr3c antlr3c)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/antlr3c/lib/libantlr3c.so
    DESTINATION ${LIB_DIR}/external
    COMPONENT Runtime
    )
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/antlr3c/runtime/C/COPYING
    DESTINATION ${LIB_DIR}/external
    COMPONENT Runtime
    RENAME antlr3c-bsd-license.txt
    )
