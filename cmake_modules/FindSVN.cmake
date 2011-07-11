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


# - Try to find the SVN library
# Once done this will define
#
#  SVN_FOUND - system has the SVN library
#  SVN_INCLUDE_DIR - the SVN include directory
#  SVN_APR_INCLUDE_DIR - the SVN APR include directory
#  SVN_LIBRARIES - The libraries needed to use SVN

IF (NOT SVN_FOUND)
  IF (NOT ${EXTERNALS_DIRECTORY} STREQUAL "")
    IF (UNIX)
      IF (${ARCH64BIT} EQUAL 1)
        SET (osdir "linux-x86_64-gcc4")
      ELSE()
        SET (osdir "linux-i686-gcc4")
      ENDIF()
    ELSEIF(WIN32)
      SET (osdir "windows-i386-vc90")
    ELSE()
      SET (osdir "unknown")
    ENDIF()
    IF (NOT ("${osdir}" STREQUAL "unknown"))
      IF (UNIX)
    FIND_PATH (SVN_INCLUDE_DIR NAMES svn_wc.h PATHS "${EXTERNALS_DIRECTORY}/subversion/${osdir}/include/subversion-1" NO_DEFAULT_PATH)
    FIND_PATH (SVN_APR_INCLUDE_DIR NAMES apr.h PATHS "${EXTERNALS_DIRECTORY}/subversion/${osdir}/include/apr-1" NO_DEFAULT_PATH)
      ELSEIF(WIN32)
    FIND_PATH (SVN_INCLUDE_DIR NAMES apr.h PATHS "${EXTERNALS_DIRECTORY}/subversion/${osdir}/include" NO_DEFAULT_PATH)
      ENDIF()
      IF(WIN32)
        FIND_LIBRARY (SVN_LIBRARIES NAMES libapr-1 PATHS "${EXTERNALS_DIRECTORY}/subversion/${osdir}/lib" NO_DEFAULT_PATH)
      ELSE()
        FIND_LIBRARY (SVN_LIBRARIES NAMES svn_client-1 PATHS "${EXTERNALS_DIRECTORY}/subversion/${osdir}/lib" NO_DEFAULT_PATH)
      ENDIF()
    ENDIF()
  ENDIF()

  if (USE_NATIVE_LIBRARIES)
    # if we didn't find in externals, look in system include path
    IF(WIN32)
        FIND_PATH (SVN_INCLUDE_DIR NAMES apr.h)
        FIND_LIBRARY (SVN_LIBRARIES NAMES libapr-1)
    ELSE()
        FIND_PATH (SVN_INCLUDE_DIR NAMES apr.h)
        FIND_LIBRARY (SVN_LIBRARIES NAMES svn_client-1)
    ENDIF()
  endif()

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(SVN DEFAULT_MSG
    SVN_LIBRARIES 
    SVN_INCLUDE_DIR
  )
  IF (SVN_FOUND)
    IF(WIN32)
        STRING(REPLACE "libapr-1" "libapriconv-1" SVN_EXTRA1 "${SVN_LIBRARIES}")
        STRING(REPLACE "libapr-1" "libaprutil-1" SVN_EXTRA2 "${SVN_LIBRARIES}")
        STRING(REPLACE "libapr-1" "svn_client-1" SVN_EXTRA3 "${SVN_LIBRARIES}")
        STRING(REPLACE "libapr-1" "svn_delta-1" SVN_EXTRA4 "${SVN_LIBRARIES}")
        STRING(REPLACE "libapr-1" "svn_diff-1" SVN_EXTRA5 "${SVN_LIBRARIES}")
        STRING(REPLACE "libapr-1" "svn_fs-1" SVN_EXTRA6 "${SVN_LIBRARIES}")
        STRING(REPLACE "libapr-1" "libsvn_fs_util-1" SVN_EXTRA7 "${SVN_LIBRARIES}")
        STRING(REPLACE "libapr-1" "libsvn_fs_fs-1" SVN_EXTRA8 "${SVN_LIBRARIES}")
        STRING(REPLACE "libapr-1" "svn_ra-1" SVN_EXTRA9 "${SVN_LIBRARIES}")
        STRING(REPLACE "libapr-1" "libsvn_ra_local-1" SVN_EXTRA10 "${SVN_LIBRARIES}")
        STRING(REPLACE "libapr-1" "libsvn_ra_svn-1" SVN_EXTRA11 "${SVN_LIBRARIES}")
        STRING(REPLACE "libapr-1" "svn_repos-1" SVN_EXTRA12 "${SVN_LIBRARIES}")
        STRING(REPLACE "libapr-1" "svn_subr-1" SVN_EXTRA13 "${SVN_LIBRARIES}")
        STRING(REPLACE "libapr-1" "svn_wc-1" SVN_EXTRA14 "${SVN_LIBRARIES}")
        STRING(REPLACE "libapr-1" "ShFolder" SVN_EXTRA15 "${SVN_LIBRARIES}")
        STRING(REPLACE "libapr-1" "xml" SVN_EXTRA16 "${SVN_LIBRARIES}")
        STRING(REPLACE "libapr-1" "zlibstat" SVN_EXTRA17 "${SVN_LIBRARIES}")

        set (SVN_LIBRARIES ${SVN_LIBRARIES} ${SVN_EXTRA1} ${SVN_EXTRA2} ${SVN_EXTRA3} ${SVN_EXTRA4} ${SVN_EXTRA5} ${SVN_EXTRA6} ${SVN_EXTRA7} ${SVN_EXTRA8} ${SVN_EXTRA9} ${SVN_EXTRA10} ${SVN_EXTRA11} ${SVN_EXTRA12} ${SVN_EXTRA13} ${SVN_EXTRA14} ${SVN_EXTRA15} ${SVN_EXTRA16} ${SVN_EXTRA17} )
    ENDIF()
  ENDIF()
ENDIF()
