################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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

cmake_minimum_required(VERSION 2.8)
PROJECT(lib2)

if (APPLE)
    if ("${BOOST_REGEX_LIBRARIES}" STREQUAL "Boost::regex")
      set(DYLIBS "${Boost_REGEX_LIBRARY_RELEASE}")
    else()
      set(DYLIBS "${BOOST_REGEX_LIBRARIES}")
    endif()
    list(APPEND DYLIBS ${ICU_LIBRARIES})
    list(APPEND DYLIBS ${XALAN_LIBRARIES})
    list(APPEND DYLIBS ${XERCES_LIBRARIES})
    list(APPEND DYLIBS ${BINUTILS_LIBRARIES})
    list(APPEND DYLIBS ${OPENSSL_LIBRARIES})
    list(APPEND DYLIBS ${ZLIB_LIBRARIES})
    list(APPEND DYLIBS ${LIBXML2_LIBRARIES})
    list(APPEND DYLIBS ${CBLAS_LIBRARIES})
    list(APPEND DYLIBS ${CBLAS_DEPS_LIBS})
elseif (WIN32)
    #TODO:  Should find these dlls not assume them.
    if (NOT USE_NATIVE_LIBRARIES)
      set (BOOST_REGEX_BIN "${BOOST_REGEX_LIBRARY_DIR}/${boost_regex_lib}")
    endif (NOT USE_NATIVE_LIBRARIES)
    if (${ARCH64BIT} EQUAL 1)
      find_file (ICU_DT_BIN "icudt57.dll" "${EXTERNALS_DIRECTORY}/icu/bin64" NO_DEFAULT_PATH)
      find_file (ICU_IN_BIN "icuin57.dll" "${EXTERNALS_DIRECTORY}/icu/bin64" NO_DEFAULT_PATH)
      find_file (ICU_UC_BIN "icuuc57.dll" "${EXTERNALS_DIRECTORY}/icu/bin64" NO_DEFAULT_PATH)
      set (LIB_CRYPTO_DLL "libcrypto-1_1-x64.dll")
      find_file (OPENSSL_LIB_BIN "${LIB_CRYPTO_DLL}" "${EXTERNALS_DIRECTORY}/openssl/1.1/win64/lib" NO_DEFAULT_PATH)
      set (LIB_SSL_DLL "libssl-1_1-x64.dll")
      find_file (OPENSSL_SSL_BIN "${LIB_SSL_DLL}" "${EXTERNALS_DIRECTORY}/openssl/1.1/win64/lib" NO_DEFAULT_PATH)
      find_file (XALAN_C_BIN "Xalan-C_1_11.dll" "${EXTERNALS_DIRECTORY}/xalan/xalan-c/win64/bin" NO_DEFAULT_PATH)
      find_file (XALAN_MESSAGES_BIN "XalanMessages_1_11.dll" "${EXTERNALS_DIRECTORY}/xalan/xalan-c/win64/bin" NO_DEFAULT_PATH)
      find_file (XERCES_C_BIN "xerces-c_3_1.dll" "${EXTERNALS_DIRECTORY}/xalan/xerces-c/win64/bin" NO_DEFAULT_PATH)
      find_file (ZLIB_BIN "zlib.dll" "${EXTERNALS_DIRECTORY}/zlib/1.2.8/lib/Win64" NO_DEFAULT_PATH)
    else()
      find_file (ICU_DT_BIN "icudt57.dll" "${EXTERNALS_DIRECTORY}/icu/bin" NO_DEFAULT_PATH)
      find_file (ICU_IN_BIN "icuin57.dll" "${EXTERNALS_DIRECTORY}/icu/bin" NO_DEFAULT_PATH)
      find_file (ICU_UC_BIN "icuuc57.dll" "${EXTERNALS_DIRECTORY}/icu/bin" NO_DEFAULT_PATH)
      set (LIB_CRYPTO_DLL "libcrypto-1_1.dll")
      find_file (OPENSSL_LIB_BIN "${LIB_CRYPTO_DLL}" "${EXTERNALS_DIRECTORY}/openssl/1.1/win32/lib" NO_DEFAULT_PATH)
      set (LIB_SSL_DLL "libssl-1_1.dll")
      find_file (OPENSSL_SSL_BIN "${LIB_SSL_DLL}" "${EXTERNALS_DIRECTORY}/openssl/1.1/win32/lib" NO_DEFAULT_PATH)
      find_file (XALAN_C_BIN "Xalan-C_1_11.dll" "${EXTERNALS_DIRECTORY}/xalan/xalan-c/win32/bin" NO_DEFAULT_PATH)
      find_file (XALAN_MESSAGES_BIN "XalanMessages_1_11.dll" "${EXTERNALS_DIRECTORY}/xalan/xalan-c/win32/bin" NO_DEFAULT_PATH)
      find_file (XERCES_C_BIN "xerces-c_3_1.dll" "${EXTERNALS_DIRECTORY}/xalan/xerces-c/win32/bin" NO_DEFAULT_PATH)
      find_file (ZLIB_BIN "zlib.dll" "${EXTERNALS_DIRECTORY}/zlib/1.2.8/lib/Win32" NO_DEFAULT_PATH)
    endif()

    set(DYLIBS "")
    if (NOT USE_NATIVE_LIBRARIES AND USE_BOOST_REGEX)
        list(APPEND DYLIBS ${BOOST_REGEX_BIN})
    endif (NOT USE_NATIVE_LIBRARIES AND USE_BOOST_REGEX)
    list(APPEND DYLIBS ${ICU_DT_BIN} ${ICU_IN_BIN} ${ICU_UC_BIN})
    list(APPEND DYLIBS ${OPENSSL_LIB_BIN} ${OPENSSL_SSL_BIN})
    list(APPEND DYLIBS ${XALAN_C_BIN} ${XALAN_MESSAGES_BIN})
    list(APPEND DYLIBS ${XERCES_C_BIN})
    list(APPEND DYLIBS ${ZLIB_BIN})
endif()

if (WIN32)

   set (LIB_CRYPTO_BIN   "${CMAKE_BINARY_DIR}/bin/${CMAKE_BUILD_TYPE}/${LIB_CRYPTO_DLL}")
   set (LIB_SSL_BIN   "${CMAKE_BINARY_DIR}/bin/${CMAKE_BUILD_TYPE}/${LIB_SSL_DLL}")
   ADD_CUSTOM_COMMAND(
       COMMAND ${CMAKE_COMMAND} -E copy  ${OPENSSL_LIB_BIN} ${LIB_CRYPTO_BIN}
       COMMAND ${CMAKE_COMMAND} -E copy  ${OPENSSL_SSL_BIN} ${LIB_SSL_BIN}
       OUTPUT  ${LIB_CRYPTO_BIN} ${LIB_SSL_BIN}
   )
   ADD_CUSTOM_TARGET(copy_libssl ALL DEPENDS ${LIB_CRYPTO_BIN} ${LIB_SSL_BIN})
endif ()

foreach(dylib ${DYLIBS})
    get_filename_component(dylib_path ${dylib} REALPATH)
    if (WIN32)
        install(PROGRAMS "${dylib_path}" DESTINATION ${EXEC_DIR})
    elseif (APPLE)
        install(PROGRAMS "${dylib_path}" DESTINATION lib2)
        get_filename_component(dylib_name_ext ${dylib_path} NAME)
        get_filename_component(dylib_name_only ${dylib_name_ext} NAME_WE)

        install(CODE "
           file(GLOB files \"\$ENV{DESTDIR}\${CMAKE_INSTALL_PREFIX}/${EXEC_DIR}/*\" \"\$ENV{DESTDIR}\${CMAKE_INSTALL_PREFIX}/${LIB_DIR}/*.dylib\" \"\$ENV{DESTDIR}\${CMAKE_INSTALL_PREFIX}/plugins/*.dylib\" \"\$ENV{DESTDIR}\${CMAKE_INSTALL_PREFIX}/lib2/*.dylib\")
           foreach(file \${files})
               execute_process(COMMAND bash \"-c\"  \"otool -L \\\"\${file}\\\" | egrep \\\"(\\\\s|/)${dylib_name_only}(.[0-9]{1,})*.dylib\\\" | sed \\\"s/^[[:space:]]//g\\\" | cut -d' ' -f1\"
                  OUTPUT_VARIABLE otoolOut
                  ERROR_VARIABLE  otoolErr
                  OUTPUT_STRIP_TRAILING_WHITESPACE
               )
               if (\"\${otoolOut}\" STREQUAL \"\")
                  continue()
               endif()
               if (NOT \"\${otoolErr}\" STREQUAL \"\")
                   message(FATAL_ERROR \"Failed to check dependent lib  ${dylib_name_only} for \${file}\")
               endif()

                  set(original_dylib_path \${otoolOut})

                  file(GLOB lib2_dylib \"\$ENV{DESTDIR}\${CMAKE_INSTALL_PREFIX}/lib2/${dylib_name_only}*.dylib\")
                  if (\"\${otoolOut}\" STREQUAL \"\")
                      message(FATAL_ERROR \"Cannot find ${dylib_name_only}*.dylib under lib2\")
                  endif()
                  get_filename_component(dylib_name_ext \"\${lib2_dylib}\" NAME)

                  execute_process(
                      COMMAND install_name_tool -change \"\${original_dylib_path}\" \"@loader_path/../lib2/\${dylib_name_ext}\" \"\${file}\"
                      OUTPUT_VARIABLE out
                     ERROR_VARIABLE  err
                  )
               endif()
           endforeach ()
        ")
   endif()
endforeach(dylib)
