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

# - Try to find any DEPendencies required for the exampleplugin
# Once done this will define
#
#  EXAMPLE_PLUGIN_DEP_FOUND - system has the dependencies
#  EXAMPLE_PLUGIN_DEP_INCLUDE_DIR - the dependency include directory
#  EXAMPLE_PLUGIN_DEP_LIBRARIES - The dependency libraries needed.

IF (NOT EXAMPLE_PLUGIN_DEP_FOUND)
  #The following two libraries are required dependencies for this plugin: libhpcc-example-plugin-deps_1 and libhpcc-example-plugin-deps_2.
  IF (WIN32)
    SET (libdep_1 "libhpcc-example-plugin-deps_1")
    SET (libdep_1 "libhpcc-example-plugin-deps_2")
  ELSE()
    SET (libdep_1 "hpcc-example-plugin-deps_1")
    SET (libdep_2 "hpcc-example-plugin-deps_2")
  ENDIF()

  #Find the path to any required include file
  FIND_PATH (EXAMPLE_PLUGIN_DEP_INCLUDE_DIR hpcc-example-plugin-deps PATHS /usr/include /usr/share/include /usr/local/include PATH_SUFFIXES dep)
  #Find the path to any required libraries, in this example there are two.
  FIND_LIBRARY (EXAMPLE_PLUGIN_DEP_LIBRARY_1 NAMES ${libdep_1} PATHS /usr/lib /usr/share /usr/lib64 /usr/local/lib /usr/local/lib64)
  FIND_LIBRARY (EXAMPLE_PLUGIN_DEP_LIBRARY_2 NAMES ${libdep_2} PATHS /usr/lib /usr/share /usr/lib64 /usr/local/lib /usr/local/lib64)
  SET (EXAMPLE_PLUGIN_DEP_LIBRARIES ${EXAMPLE_PLUGIN_DEP_LIBRARY_1} ${EXAMPLE_PLUGIN_DEP_LIBRARY_2})

  #The required include file may contain certain definitions for the major, minor, and patch versions,
  #in which case this can be extracted such that a minimum version check can be conducted at cmake
  #configuration time. In this example the following is being extracted from a C header file:
  #"#define EXAMPLE_PLUGIN_DEP_MAJOR 5"
  #"#define EXAMPLE_PLUGIN_DEP_MINOR 4"
  #"#define EXAMPLE_PLUGIN_DEP_PATCH 2"
  #The minimum version to requirement is made visible to this cmake file from
  #HPCC/plugins/exampleplugin/CMakeLists.txt:ADD_PLUGIN(EXAMPLEPLUGIN PACKAGES EXAMPLE_PLUGIN_DEP MINVERSION 4.6.2)
  IF  (EXISTS "${EXAMPLE_PLUGIN_DEP_INCLUDE_DIR}/hpcc-example-plugin-deps.h")
    #MAJOR
    FILE (STRINGS "${EXAMPLE_PLUGIN_DEP_INCLUDE_DIR}/hpcc-example-plugin-deps.h" major REGEX "#define EXAMPLE_PLUGIN_DEP_MAJOR")
    STRING (REGEX REPLACE "#define EXAMPLE_PLUGIN_DEP_MAJOR " "" major "${major}")
    STRING (REGEX REPLACE "\"" "" major "${major}")
    #MINOR
    FILE (STRINGS "${EXAMPLE_PLUGIN_DEP_INCLUDE_DIR}/deps.h" minor REGEX "#define EXAMPLE_PLUGIN_DEP_MINOR")
    STRING (REGEX REPLACE "#define EXAMPLE_PLUGIN_DEP_MINOR " "" minor "${minor}")
    STRING (REGEX REPLACE "\"" "" minor "${minor}")
    #PATCH
    FILE (STRINGS "${EXAMPLE_PLUGIN_DEP_INCLUDE_DIR}/deps.h" patch REGEX "#define EXAMPLE_PLUGIN_DEP_PATCH")
    STRING (REGEX REPLACE "#define EXAMPLE_PLUGIN_DEP_PATCH " "" patch "${patch}")
    STRING (REGEX REPLACE "\"" "" patch "${patch}")

    SET (EXAMPLE_PLUGIN_DEP_VERSION_STRING "${major}.${minor}.${patch}")
    IF ("${EXAMPLE_PLUGIN_DEP_VERSION_STRING}" VERSION_LESS "${EXAMPLE_PLUGIN_DEP_FIND_VERSION}")
      SET(MSG "libhpcc-example-plugin-deps version '${EXAMPLE_PLUGIN_DEP_VERSION_STRING}' incompatible with min version>=${EXAMPLE_PLUGIN_DEP_FIND_VERSION}")
    ELSE()
      SET (EXAMPLE_PLUGIN_DEP_VERSION_OK 1)
      SET (MSG "${DEFAULT_MSG}")
    ENDIF()
  ENDIF()

  #The following three lines are used for building this example as part of the HPCC regression suite.
  #They are not intended to be present as part of this example itself and should be removed.
  SET (EXAMPLE_PLUGIN_DEP_LIBRARIES "jlib")
  SET (EXAMPLE_PLUGIN_DEP_INCLUDE_DIR "./")
  SET (EXAMPLE_PLUGIN_DEP_VERSION_OK 1)

  #If the following three variables (EXAMPLE_PLUGIN_DEP_LIBRARIES, EXAMPLE_PLUGIN_DEP_INCLUDE_DIR, &
  #EXAMPLE_PLUGIN_DEP_VERSION_OK) have not been set, MAKE_EXAMPLEPLUGIN will be set false when
  #returning to HPCC/plugins/exampleplugin/CMakeLists.txt and thus this plugin will not be built
  #and a message will be given explaining which of three where missing or give the above version
  #incompatible message.
  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(example_plugin_dep ${MSG}
    EXAMPLE_PLUGIN_DEP_LIBRARIES
    EXAMPLE_PLUGIN_DEP_INCLUDE_DIR
    EXAMPLE_PLUGIN_DEP_VERSION_OK
  )

  MARK_AS_ADVANCED(EXAMPLE_PLUGIN_DEP_INCLUDE_DIR EXAMPLE_PLUGIN_DEP_LIBRARIES)
ENDIF()
