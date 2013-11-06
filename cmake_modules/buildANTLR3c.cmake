################################################################################
#    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.
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


# - Try to find the ANTLR 3 c library
# Once done this will define
#
#  ANTLR3c_FOUND       - System has the ANTLR3c library
#  ANTLR3c_INCLUDE_DIR - The ANTLR3c include directory
#  ANTLR3c_LIBRARIES   - The libraries needed to use ANTLR3c

if ( NOT ANTLR3C_VER )
    set( ANTLR3C_VER "3.4" )
endif()

MESSAGE("--- Target ANTLR3C Version: ${ANTLR3C_VER}")

SET (ANTLRcCONFIGURE_COMMAND_PARAMS "--silent" "--disable-antlrdebug")

IF (NOT ANTLR3c_FOUND)
    IF (WIN32)
        SET (ANTLR3c_lib "antlr3c.lib")
    ELSE()
        SET (ANTLR3c_lib "libantlr3c.so")
    ENDIF()

    IF (UNIX)
        IF (${ARCH64BIT} EQUAL 1)
            SET (ANTLRcCONFIGURE_COMMAND_PARAMS ${ANTLRcCONFIGURE_COMMAND_PARAMS} "--enable-64bit")
            SET (osdir "x86_64-linux-gnu")
        ELSE()
            SET (osdir "i386-linux-gnu")
        ENDIF()
    ELSEIF(WIN32)
      SET (osdir "lib")
    ELSE()
      SET (osdir "unknown")
    ENDIF()

    SET (ANTLR3_URL "http://www.antlr3.org")
    SET (ANTLR3c_DOWNLOAD_URL ${ANTLR3_URL}/download/C)
    SET (ANTLRcPACKAGENAME "libantlr3c-${ANTLR3C_VER}")
    SET (ANTLRcPACKAGE ${ANTLRcPACKAGENAME}.tar.gz)
    SET (ANTLRcSOURCELOCATION ${CMAKE_CURRENT_BINARY_DIR}/ANTLRC)
    SET (ANTLRcSEXPANDEDOURCELOCATION ${ANTLRcSOURCELOCATION}/${ANTLRcPACKAGENAME})
    SET (ANTLRcBUILDLOCATION ${ANTLRcSEXPANDEDOURCELOCATION}/antlr3cbuild)
    SET (ANTLRcBUILDLOCATION_LIBRARY ${ANTLRcBUILDLOCATION}/lib)
    SET (ANTLRcBUILDLOCATION_INCLUDE ${ANTLRcBUILDLOCATION}/include)

    ADD_CUSTOM_COMMAND(
        OUTPUT ${ANTLRcSOURCELOCATION}/${ANTLRcPACKAGE}
        COMMAND wget "${ANTLR3c_DOWNLOAD_URL}/${ANTLRcPACKAGE}"
        WORKING_DIRECTORY ${ANTLRcSOURCELOCATION}
    )
    ADD_CUSTOM_TARGET(${ANTLRcPACKAGENAME}-fetch DEPENDS ${ANTLRcSOURCELOCATION}/${ANTLRcPACKAGE})

    ADD_CUSTOM_COMMAND(
        OUTPUT ${ANTLRcSOURCELOCATION}/${ANTLRcPACKAGENAME}-expand.sentinel
        COMMAND ${CMAKE_COMMAND} -E tar xjf ${ANTLRcSOURCELOCATION}/${ANTLRcPACKAGE}
        COMMAND ${CMAKE_COMMAND} -E touch ${ANTLRcSOURCELOCATION}/${ANTLRcPACKAGENAME}-expand.sentinel
        DEPENDS ${ANTLRcSOURCELOCATION}/${ANTLRcPACKAGE}
        WORKING_DIRECTORY ${ANTLRcSOURCELOCATION}
    )
    ADD_CUSTOM_TARGET(${ANTLRcPACKAGENAME}-expand DEPENDS ${ANTLRcPACKAGENAME}-fetch ${ANTLRcSOURCELOCATION}/${ANTLRcPACKAGENAME}-expand.sentinel)

    INCLUDE(ExternalProject)

    ExternalProject_Add(
        libantlr3c_external
        DOWNLOAD_COMMAND ""
        SOURCE_DIR ${ANTLRcSEXPANDEDOURCELOCATION}
        CONFIGURE_COMMAND ${ANTLRcSEXPANDEDOURCELOCATION}/configure ${ANTLRcCONFIGURE_COMMAND_PARAMS} --prefix=${ANTLRcBUILDLOCATION}
        PREFIX ${ANTLRcSOURCELOCATION}
        BUILD_COMMAND $(MAKE)
        BUILD_IN_SOURCE 1
    )

    add_dependencies(libantlr3c_external DEPENDS libantlr3c-3.4-expand)

    add_library(libantlr3c UNKNOWN IMPORTED)
    #add_library(libantlr3c STATIC IMPORTED)
    set_property(TARGET libantlr3c PROPERTY IMPORTED_LOCATION ${ANTLRcBUILDLOCATION_LIBRARY}/${ANTLR3c_lib})

    add_dependencies(libantlr3c libantlr3c_external)

    SET(ANTLR3c_INCLUDE_DIR ${ANTLRcBUILDLOCATION_INCLUDE})
    SET(ANTLR3c_LIBRARIES libantlr3c)

    MESSAGE("---Expecting antlr3.h in ${ANTLRcBUILDLOCATION_INCLUDE}")
    FIND_PATH ( ANTLR3c_INCLUDE_DIR NAMES antlr3.h
                PATHS ${ANTLRcBUILDLOCATION_INCLUDE} NO_DEFAULT_PATH)

    MESSAGE("---Looking for ${ANTLR3c_lib} in ${ANTLRcBUILDLOCATION_LIBRARY}")
    FIND_LIBRARY ( ANTLR3c_LIBRARIES NAMES ${ANTLR3c_lib}
                   PATHS ${ANTLRcBUILDLOCATION_LIBRARY} NO_DEFAULT_PATH)

    INSTALL(FILES ${ANTLRcBUILDLOCATION_LIBRARY}/${ANTLR3c_lib} DESTINATION ${EXTERN_LIB_PATH} COMPONENT Runtime)

ENDIF()
