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


# - Try to find the OpenLDAP libraries
# Once done this will define
#
#  OPENLDAP_FOUND - system has the OpenLDAP library
#  OPENLDAP_INCLUDE_DIR - the OpenLDAP include directory
#  OPENLDAP_LIBRARIES - The libraries needed to use OpenLDAP
#
#  Note that we use winldap for windows builds at present
#
IF (NOT OPENLDAP_FOUND)
  IF (WIN32)
    SET (ldap_dll "wldap32")
    SET (ldap_inc "Winldap.h")
  ELSE()
    SET (ldap_dll "ldap_r")
    SET (ldap_inc "ldap.h")
  ENDIF()

  IF (NOT ${EXTERNALS_DIRECTORY} STREQUAL "")
    IF (UNIX)
      IF (${ARCH64BIT} EQUAL 1)
        SET (osincdir "openldap/linux64_gcc4.1.1/include")
        SET (oslibdir "openldap/linux64_gcc4.1.1")
      ELSE()
        SET (osincdir "openldap/linux32_gcc4.1.1/include")
        SET (oslibdir "openldap/linux32_gcc4.1.1")
      ENDIF()
    ELSEIF(WIN32)
        SET (osincdir "winldap/include")
        SET (oslibdir "winldap/lib")
    ELSE()
      SET (osincdir "unknown")
    ENDIF()
    IF (NOT ("${osincdir}" STREQUAL "unknown"))
      FIND_PATH (OPENLDAP_INCLUDE_DIR NAMES ${ldap_inc} PATHS "${EXTERNALS_DIRECTORY}/${osincdir}" NO_DEFAULT_PATH)
      FIND_LIBRARY (OPENLDAP_LIBRARIES NAMES ${ldap_dll} PATHS "${EXTERNALS_DIRECTORY}/${oslibdir}" NO_DEFAULT_PATH)
    ENDIF()
  ENDIF()

  # if we didn't find in externals, look in system include path
  if (USE_NATIVE_LIBRARIES)
    FIND_PATH (OPENLDAP_INCLUDE_DIR NAMES ${ldap_inc})
    FIND_LIBRARY (OPENLDAP_LIBRARIES NAMES ${ldap_dll})
  endif()

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(OpenLDAP DEFAULT_MSG
    OPENLDAP_LIBRARIES
    OPENLDAP_INCLUDE_DIR
  )
  IF (OPENLDAP_FOUND)
    IF (UNIX)
      STRING(REPLACE "ldap_r" "lber" OPENLDAP_EXTRA "${OPENLDAP_LIBRARIES}")
      set (OPENLDAP_LIBRARIES ${OPENLDAP_LIBRARIES} ${OPENLDAP_EXTRA} )
    ELSE()
      set (OPENLDAP_LIBRARIES ${OPENLDAP_LIBRARIES} netapi32 )
    ENDIF()
  ENDIF()

  MARK_AS_ADVANCED(OPENLDAP_INCLUDE_DIR OPENLDAP_LIBRARIES)
ENDIF()
