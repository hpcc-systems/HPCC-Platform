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

if(NOT CBLAS_FOUND)
    if(WIN32)
        set(cblas_lib "cblas")
    else()
        set(cblas_lib cblas tatlas satlas)
    endif()

    find_path(CBLAS_INCLUDE_DIR NAMES cblas.h)
    find_library(CBLAS_LIBRARIES NAMES ${cblas_lib} PATHS /usr/lib/atlas /usr/lib64/atlas)

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(CBLAS
        DEFAULT_MSG
        CBLAS_LIBRARIES 
        CBLAS_INCLUDE_DIR)

    if (APPLE)
      set(LIB_TO_DO ${CBLAS_LIBRARIES})

      set(CBLAS_DEPS_LIBS "")
      foreach (lib libquadmath;libgfortran;libgcc_s)
         message("otool -L ${LIB_TO_DO} | egrep ${lib}(.[0-9]{1,})*.dylib | sed \"s/^[[:space:]]//g\" | cut -d' ' -f1")
         execute_process(
           COMMAND bash "-c"  "otool -L \"${LIB_TO_DO}\" | egrep \"${lib}(.[0-9]{1,})*.dylib\" | sed \"s/^[[:space:]]//g\" | cut -d' ' -f1"
           OUTPUT_VARIABLE otoolOut
           ERROR_VARIABLE  otoolErr
           OUTPUT_STRIP_TRAILING_WHITESPACE
        )
        if (NOT "${otoolErr}" STREQUAL "")
          message(FATAL_ERROR "Failed to check dependent lib ${lib} for ${LIB_TO_DO}")
        endif()

        if ("${otoolOut}" STREQUAL "")
          message(FATAL_ERROR "${LIB_TO_DO} dependencies changed. Run otool -L check manually and update file FindCBLAS.cmake")
        endif()
        list(APPEND CBLAS_DEPS_LIBS ${otoolOut})
        if ("${otoolOut}" MATCHES ".*libgfortran.*")
          set(LIB_TO_DO "${otoolOut}")
        endif()
      endforeach()
    endif(APPLE)

    mark_as_advanced(CBLAS_INCLUDE_DIR CBLAS_LIBRARIES)
endif()
