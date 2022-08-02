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
    SET (lber_dll "netapi32")
    SET (ldap_inc "Winldap.h")
  ELSE()
    SET (ldap_dll "ldap")
    SET (lber_dll "lber")
    SET (ldap_inc "ldap.h")
  ENDIF()

  IF (NOT "${EXTERNALS_DIRECTORY}" STREQUAL "")
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
        IF (${ARCH64BIT} EQUAL 1)
           SET (oslibdir "winldap/lib64")
        ELSE()
           SET (oslibdir "winldap/lib32")
        ENDIF()
    ELSE()
      SET (osincdir "unknown")
    ENDIF()
    IF (NOT ("${osincdir}" STREQUAL "unknown"))
      FIND_PATH (OPENLDAP_INCLUDE_DIR NAMES ${ldap_inc} PATHS "${EXTERNALS_DIRECTORY}/${osincdir}" NO_DEFAULT_PATH)
      FIND_LIBRARY (OPENLDAP_LIBRARIES NAMES ${ldap_dll} PATHS "${EXTERNALS_DIRECTORY}/${oslibdir}" NO_DEFAULT_PATH)
    ENDIF()
  ENDIF()

  # if we didn't find in externals, look in system include path
  FIND_PATH (OPENLDAP_INCLUDE_DIR NAMES ${ldap_inc})
  FIND_LIBRARY (LDAP_LIBRARY ${ldap_dll})
  FIND_LIBRARY (LBER_LIBRARY ${lber_dll})

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(OPENLDAP DEFAULT_MSG OPENLDAP_INCLUDE_DIR LDAP_LIBRARY LBER_LIBRARY)
  set(OPENLDAP_LIBRARIES ${LDAP_LIBRARY} ${LBER_LIBRARY})
  mark_as_advanced(OPENLDAP_INCLUDE_DIR OPENLDAP_LIBRARIES LDAP_LIBRARY LBER_LIBRARY)
ENDIF()
