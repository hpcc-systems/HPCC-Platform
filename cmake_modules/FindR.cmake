################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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

# - Try to find the R library
# Once done this will define
#
#  R_FOUND - system has the R library
#  R_INCLUDE_DIR - the R include directory(s)
#  R_LIBRARIES - The libraries needed to use R

IF (NOT R_FOUND)
  IF (WIN32)
    SET (R_lib "libR")
    SET (Rcpp_lib "libRcpp")
    SET (RInside_lib "libRInside")
  ELSE()
    SET (R_lib "R")
    SET (Rcpp_lib "Rcpp")
    SET (RInside_lib "RInside")
  ENDIF()

  FIND_PATH(R_INCLUDE_DIR R.h PATHS /usr/lib /usr/lib64 /usr/share /usr/local/lib /usr/local/lib64 PATH_SUFFIXES R/include)
  FIND_PATH(RCPP_INCLUDE_DIR Rcpp.h PATHS /usr/lib /usr/lib64 /usr/share /usr/local/lib /usr/local/lib64 PATH_SUFFIXES R/library/Rcpp/include/ R/site-library/Rcpp/include/)
  FIND_PATH(RINSIDE_INCLUDE_DIR RInside.h PATHS /usr/lib /usr/lib64 /usr/share /usr/local/lib /usr/local/lib64 PATH_SUFFIXES R/library/RInside/include  R/site-library/RInside/include)

  FIND_LIBRARY (R_LIBRARY NAMES ${R_lib}  PATHS /usr/lib /usr/share /usr/lib64 /usr/local/lib /usr/local/lib64 PATH_SUFFIXES R/lib)
  FIND_LIBRARY (RCPP_LIBRARY NAMES ${Rcpp_lib} PATHS /usr/lib /usr/share /usr/lib64 /usr/local/lib /usr/local/lib64 PATH_SUFFIXES R/library/Rcpp/lib/ R/site-library/Rcpp/lib/)
  FIND_LIBRARY (RINSIDE_LIBRARY NAMES ${RInside_lib} PATHS /usr/lib /usr/share /usr/lib64 /usr/local/lib /usr/local/lib64 PATH_SUFFIXES R/library/RInside/lib/ R/site-library/RInside/lib/)

  IF (RCPP_LIBRARY STREQUAL "RCPP_LIBRARY-NOTFOUND")
    SET (RCPP_LIBRARY "")    # Newer versions of Rcpp are header-only, with no associated library.
  ENDIF()

  SET (R_INCLUDE_DIRS ${R_INCLUDE_DIR} ${RINSIDE_INCLUDE_DIR} ${RCPP_INCLUDE_DIR})
  SET (R_LIBRARIES ${R_LIBRARY} ${RINSIDE_LIBRARY} ${RCPP_LIBRARY})

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(R DEFAULT_MSG
    R_LIBRARIES
    R_INCLUDE_DIRS
  )

  MARK_AS_ADVANCED(R_INCLUDE_DIRS R_LIBRARIES)
ENDIF()
