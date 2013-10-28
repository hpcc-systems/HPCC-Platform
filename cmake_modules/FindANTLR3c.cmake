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

#SET (ANTLRcCONFIGURE_COMMAND_PARAMS "--silent" "--disable-antlrdebug")
SET (ANTLRcCONFIGURE_COMMAND_PARAMS "--disable-antlrdebug")

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

#    SET(EXTERNALS_DIRECTORY /home/hadoop/Downloads)
#    IF (NOT "${EXTERNALS_DIRECTORY}" STREQUAL "")
#
#        MESSAGE("--- LOOKING for ANTLRc files in EXTERNALS directory.")
#
#        OPTION(ANTLRc_EXTERNALS_SUBPATH "Location of ANTLRc include and lib files within EXTERNALS folder.")
#        #SET(ANTLRc_EXTERNALS_SUBPATH C/libantlr3c-3.4)
#        IF ("${ANTLRc_EXTERNALS_SUBPATH}" STREQUAL "")
#            SET ( ANTLRc_EXTERNALS_SUBPATH "ANTLR" )
#        ENDIF ()
#
#        MESSAGE("---Looking for antlr3.h in ${EXTERNALS_DIRECTORY}/ANTLR/${ANTLRc_EXTERNALS_SUBPATH}/include ${EXTERNALS_DIRECTORY}/ANTLR/${ANTLRc_EXTERNALS_SUBPATH} ${EXTERNALS_DIRECTORY}/${ANTLRc_EXTERNALS_SUBPATH}")
#        FIND_PATH ( ANTLR3c_INCLUDE_DIR NAMES antlr3.h
#                    PATHS
#                    ${EXTERNALS_DIRECTORY}/ANTLR/${ANTLRc_EXTERNALS_SUBPATH}/include
#                    ${EXTERNALS_DIRECTORY}/ANTLR/${ANTLRc_EXTERNALS_SUBPATH}
#                    ${EXTERNALS_DIRECTORY}/${ANTLRc_EXTERNALS_SUBPATH}
#                    ${EXTERNALS_DIRECTORY}/${ANTLRc_EXTERNALS_SUBPATH}/include
#                    NO_DEFAULT_PATH)
#        MESSAGE("---- "${ANTLR3c_INCLUDE_DIR})
#        MESSAGE("---looking for ANTLR3c_lib in ${EXTERNALS_DIRECTORY}/${ANTLRc_EXTERNALS_SUBPATH}/.libs")
#        FIND_LIBRARY ( ANTLR3c_LIBRARIES NAMES ${ANTLR3c_lib}
#                       PATHS
#                       ${EXTERNALS_DIRECTORY}/ANTLR/${ANTLRc_EXTERNALS_SUBPATH}/.libs
#                       ${EXTERNALS_DIRECTORY}/ANTLR/${ANTLRc_EXTERNALS_SUBPATH}
#                       ${EXTERNALS_DIRECTORY}/${ANTLRc_EXTERNALS_SUBPATH}
#                       ${EXTERNALS_DIRECTORY}/${ANTLRc_EXTERNALS_SUBPATH}/.libs
#                       NO_DEFAULT_PATH)
#        MESSAGE("---- "${ANTLR3c_LIBRARIES})
#
#    ELSEIF(USE_NATIVE_LIBRARIES)
#
        MESSAGE("---Looking for ANTLRc files as NATIVES.")
        MESSAGE("--- Looking for antlr3.h")
#        FIND_PATH (ANTLR3c_INCLUDE_DIR NAMES antlr3.h PATHS /usr/include /usr/local/include/ /opt/antlr/include /usr64/include)
        MESSAGE("---- "${ANTLR3c_INCLUDE_DIR})
        MESSAGE("--- Looking for ${ANTLR3c_lib} in /usr/lib/${osdir} /usr/lib /usr/local/lib${osdir} /usr/local/lib")
#        FIND_LIBRARY (ANTLR3c_LIBRARIES NAMES ${ANTLR3c_lib} PATHS /usr/lib/${osdir} /usr/lib /usr/local/lib${osdir} /usr/local/lib)
        MESSAGE("---- "${ANTLR3c_LIBRARIES})
#   ENDIF()

#    SET (ANTLR3_URL "http://www.antlr3.org")
#    SET (ANTLR3c_DOWNLOAD_URL ${ANTLR3_URL}/download/C)
#    SET (ANTLRcPACKAGENAME "libantlr3c-3.4")
#    SET (ANTLRcPACKAGE ${ANTLRcPACKAGENAME}.tar.gz)
#    #SET (ANTLRcSOURCELOCATION /home/hpccuser/ANTLRC1)
#    SET (ANTLRcSOURCELOCATION ${CMAKE_CURRENT_BINARY_DIR}/ANTLRC)
#    SET (ANTLRcSEXPANDEDOURCELOCATION ${ANTLRcSOURCELOCATION}/${ANTLRcPACKAGENAME})
#    SET (ANTLRcBUILDLOCATION ${ANTLRcSEXPANDEDOURCELOCATION}/antlrbuild)
#    SET (ANTLRcBUILDLOCATION_LIBRARY ${ANTLRcBUILDLOCATION}/lib)
#    SET (ANTLRcBUILDLOCATION_INCLUDE ${ANTLRcBUILDLOCATION}/include)

#    ADD_CUSTOM_COMMAND(
#        OUTPUT ${ANTLRcSOURCELOCATION}/${ANTLRcPACKAGE}
#        COMMAND wget "${ANTLR3c_DOWNLOAD_URL}/${ANTLRcPACKAGE}"
#        WORKING_DIRECTORY ${ANTLRcSOURCELOCATION}
#    )
#    ADD_CUSTOM_TARGET(${ANTLRcPACKAGENAME}-fetch DEPENDS ${ANTLRcSOURCELOCATION}/${ANTLRcPACKAGE})

#    ADD_CUSTOM_COMMAND(
#        OUTPUT ${ANTLRcSOURCELOCATION}/${ANTLRcPACKAGENAME}-expand.sentinel
#        COMMAND ${CMAKE_COMMAND} -E tar xjf ${ANTLRcSOURCELOCATION}/${ANTLRcPACKAGE}
#        COMMAND ${CMAKE_COMMAND} -E touch ${ANTLRcSOURCELOCATION}/${ANTLRcPACKAGENAME}-expand.sentinel
#        DEPENDS ${ANTLRcSOURCELOCATION}/${ANTLRcPACKAGE}
#        WORKING_DIRECTORY ${ANTLRcSOURCELOCATION}
#    )
#    ADD_CUSTOM_TARGET(${ANTLRcPACKAGENAME}-expand DEPENDS ${ANTLRcPACKAGENAME}-fetch ${ANTLRcSOURCELOCATION}/${ANTLRcPACKAGENAME}-expand.sentinel)
#
#    INCLUDE(ExternalProject)
#
#    ExternalProject_Add(
#        libantlr3c_external
#        DOWNLOAD_COMMAND ""
#        SOURCE_DIR ${ANTLRcSEXPANDEDOURCELOCATION}
#        #CONFIGURE_COMMAND ${ANTLRcSEXPANDEDOURCELOCATION}/configure ${ANTLRcCONFIGURE_COMMAND_PARAMS} --prefix=${ANTLRcBUILDLOCATION}
#        #CONFIGURE_COMMAND ${ANTLRcSEXPANDEDOURCELOCATION}/configure --enable-64bit --prefix=${ANTLRcBUILDLOCATION}
#        #CONFIGURE_COMMAND ${ANTLRcSEXPANDEDOURCELOCATION}/configure --enable-64bit
#        CONFIGURE_COMMAND ${ANTLRcSEXPANDEDOURCELOCATION}/configure ${ANTLRcCONFIGURE_COMMAND_PARAMS}
#        PREFIX ${ANTLRcSOURCELOCATION}
#        BUILD_COMMAND $(MAKE)
#        BUILD_IN_SOURCE 1
#    )
#
#    add_dependencies(libantlr3c_external DEPENDS libantlr3c-3.4-expand)
#
#    #add_library(libantlr3c UNKNOWN IMPORTED)
#    add_library(libantlr3c STATIC IMPORTED)
#    #set_property(TARGET libantlr3c PROPERTY IMPORTED_LOCATION ${ANTLRcBUILDLOCATION_LIBRARY}/${ANTLR3c_lib})
#    #set_property(TARGET libantlr3c PROPERTY IMPORTED_LOCATION /usr/local/lib/${osdir}/lib/${ANTLR3c_lib})
#    set_property(TARGET libantlr3c PROPERTY IMPORTED_LOCATION /usr/local/lib/${ANTLR3c_lib})
#
#    add_dependencies(libantlr3c libantlr3c_external)
#
#    #SET(ANTLR3c_INCLUDE_DIR ${ANTLRcBUILDLOCATION_INCLUDE})
#    #SET(ANTLR3c_INCLUDE_DIR /usr/local/lib/x86_64-linux-gnu/include)
#    SET(ANTLR3c_INCLUDE_DIR /usr/local/include)
#    SET(ANTLR3c_LIBRARIES libantlr3c)
#
##    MESSAGE("---Expecting antlr3.h in ${ANTLRcBUILDLOCATION_INCLUDE}")
#    FIND_PATH (ANTLR3c_INCLUDE_DIR NAMES antlr3.h PATHS /usr/local/lib/x86_64-linux-gnu/include /usr/include /usr/local/include/ /opt/antlr/include /usr64/include)
#
##    FIND_PATH ( ANTLR3c_INCLUDE_DIR NAMES antlr3.h
##                PATHS ${ANTLRcBUILDLOCATION_INCLUDE} NO_DEFAULT_PATH)
#
##    MESSAGE("---Looking for ${ANTLR3c_lib} in ${ANTLRcBUILDLOCATION_LIBRARY}")
##    FIND_LIBRARY ( ANTLR3c_LIBRARIES NAMES ${ANTLR3c_lib}
##                   PATHS ${ANTLRcBUILDLOCATION_LIBRARY} NO_DEFAULT_PATH)
#     #FIND_LIBRARY (ANTLR3c_LIBRARIES NAMES ${ANTLR3c_lib} PATHS /usr/lib/${osdir} /usr/lib /usr/local/lib/${osdir} /usr/local/lib)
#     FIND_LIBRARY (ANTLR3c_LIBRARIES NAMES ${ANTLR3c_lib} PATHS  /usr/local/lib/${osdir})
#
#         #install(FILES /usr/local/lib/libantlr3c.so DESTINATION ${LIB_DIR} COMPONENT Runtime)
#         install(FILES /usr/local/lib/libantlr3c.so DESTINATION /opt/HPCCSystems/lib/external COMPONENT Runtime)
#
    INCLUDE(FindPackageHandleStandardArgs)
    FIND_PACKAGE_HANDLE_STANDARD_ARGS(
        ANTLR3c DEFAULT_MSG
        ANTLR3c_LIBRARIES
        ANTLR3c_INCLUDE_DIR
    )

    MARK_AS_ADVANCED(ANTLR3c_INCLUDE_DIR ANTLR3c_LIBRARIES)
    IF (NOT ANTLR3c_FOUND)
        MESSAGE("**************LIBANTLR3C NOT FOUND****************************")
        INCLUDE (${CMAKE_MODULE_PATH}buildANTLR3c.cmake)
    ENDIF()
ENDIF()
