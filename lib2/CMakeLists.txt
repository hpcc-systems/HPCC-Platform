################################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
################################################################################

cmake_minimum_required(VERSION 2.8)
PROJECT(lib2)

if (APPLE)
    set(DYLIBS ${BOOST_REGEX_LIBRARIES})
    list(APPEND DYLIBS ${ICU_LIBRARIES})
    list(APPEND DYLIBS ${XALAN_LIBRARIES})
    list(APPEND DYLIBS ${XERCES_LIBRARIES})
    list(APPEND DYLIBS ${BINUTILS_LIBRARIES})
elseif (WIN32)
    #TODO:  Should find these dlls not assume them.
    find_file (BOOST_REGEX_BIN "boost_regex-vc90-mt-1_44.dll" "${EXTERNALS_DIRECTORY}/boost/windows-i386-vc90/lib" NO_DEFAULT_PATH)
    find_file (ICU_DT_BIN "icudt36.dll" "${EXTERNALS_DIRECTORY}/icu/bin" NO_DEFAULT_PATH)
    find_file (ICU_IN_BIN "icuin36.dll" "${EXTERNALS_DIRECTORY}/icu/bin" NO_DEFAULT_PATH)
    find_file (ICU_UC_BIN "icuuc36.dll" "${EXTERNALS_DIRECTORY}/icu/bin" NO_DEFAULT_PATH)
    find_file (OPENSSL_LIB_BIN "libeay32.dll" "${EXTERNALS_DIRECTORY}/openssl/win32/lib" NO_DEFAULT_PATH)
    find_file (OPENSSL_SSL_BIN "ssleay32.dll" "${EXTERNALS_DIRECTORY}/openssl/win32/lib" NO_DEFAULT_PATH)
    find_file (XALAN_C_BIN "Xalan-C_1_10.dll" "${EXTERNALS_DIRECTORY}/xalan/xalan-c/bin" NO_DEFAULT_PATH)
    find_file (XALAN_MESSAGES_BIN "XalanMessages_1_10.dll" "${EXTERNALS_DIRECTORY}/xalan/xalan-c/bin" NO_DEFAULT_PATH)
    find_file (XERCES_C_BIN "xerces-c_2_7.dll" "${EXTERNALS_DIRECTORY}/xalan/xerces-c/bin" NO_DEFAULT_PATH)
    set(DYLIBS ${BOOST_REGEX_BIN})
    list(APPEND DYLIBS ${ICU_DT_BIN} ${ICU_IN_BIN} ${ICU_UC_BIN})
    list(APPEND DYLIBS ${OPENSSL_LIB_BIN} ${OPENSSL_SSL_BIN})
    list(APPEND DYLIBS ${XALAN_C_BIN} ${XALAN_MESSAGES_BIN})
    list(APPEND DYLIBS ${XERCES_C_BIN})
endif()

foreach(dylib ${DYLIBS})
    get_filename_component(dylib_path ${dylib} REALPATH)
    if (WIN32)
        install(PROGRAMS "${dylib_path}" DESTINATION ${EXEC_DIR})
    elseif (APPLE)
        install(PROGRAMS "${dylib_path}" DESTINATION lib2)
        get_filename_component(dylib_name_ext ${dylib_path} NAME)

        set(fixupCommand "${fixupCommand}\r\nexecute_process(COMMAND install_name_tool -change \"${dylib_path}\" \"@loader_path/../lib2/${dylib_name_ext}\" \${file})")

        #TODO:  Should be able resolve alias's to alias's correctly?
        string(REPLACE ".28.0.dylib" ".28.dylib" dylib_28_path "${dylib_path}")
        if (NOT "${dylib_28_path}" STREQUAL "${dylib_path}")
            set(fixupCommand "${fixupCommand}\r\nexecute_process(COMMAND install_name_tool -change \"${dylib_28_path}\" \"@loader_path/../lib2/${dylib_name_ext}\" \${file})")
        endif ()

        string(REPLACE ".48.1.dylib" ".48.dylib" dylib_48_path "${dylib_path}")
        if (NOT "${dylib_48_path}" STREQUAL "${dylib_path}")
            set(fixupCommand "${fixupCommand}\r\nexecute_process(COMMAND install_name_tool -change \"${dylib_48_path}\" \"@loader_path/../lib2/${dylib_name_ext}\" \${file})")
        endif ()

        install(CODE "
            file(GLOB files \"\$ENV{DESTDIR}\${CMAKE_INSTALL_PREFIX}/${EXEC_DIR}/*\" \"\$ENV{DESTDIR}\${CMAKE_INSTALL_PREFIX}/${LIB_DIR}/*.dylib\" \"\$ENV{DESTDIR}\${CMAKE_INSTALL_PREFIX}/lib2/*.dylib\")
            foreach(file \${files})
              ${fixupCommand}
            endforeach ()
        ")
   endif()
endforeach(dylib)
