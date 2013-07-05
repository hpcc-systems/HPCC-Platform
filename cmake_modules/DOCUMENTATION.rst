===============================
CMake files structure and usage
===============================

**********************************
Directory structure of CMake files
**********************************

- /
 - CMakeLists.txt - Root CMake file
 - version.cmake - common cmake file where version variables are set
 - build-config.h.cmake - cmake generation template for build-config.h

 - cmake_modules/ - Directory storing modules and configurations for CMake
  - FindXXXXX.cmake - CMake find files used to locate libraries, headers, and binaries
  - commonSetup.cmake - common configuration settings for the entire project (contains configure time options)
  - docMacros.cmake - common documentation macros used for generating fop and pdf files
  - optionDefaults.cmake - contains common variables for the platform build
  - distrocheck.sh - script that determines if the OS uses DEB or RPM
  - getpackagerevisionarch.sh - script that returns OS version and arch in format used for packaging
  - dependencies/ - Directory storing dependency files used for package dependencies
   - <OS>.cmake - File containing either DEB or RPM dependencies for the given OS

 - build-utils/ - Directory for build related utilities
  - cleanDeb.sh - script that unpacks a deb file and rebuilds with fakeroot to clean up lintain errors/warnings

Common Macros
=============

- MACRO_ENSURE_OUT_OF_SOURCE_BUILD - prevents building from with in source tree
- HPCC_ADD_EXECUTABLE - simple wrapper around add_executable
- HPCC_ADD_LIBRARY - simple wrapper around add_library
- PARSE_ARGUMENTS - macro that can be used by other macros and functions for arg list parsing
- HPCC_ADD_SUBDIRECTORY - argument controlled add subdirectory wrapper
- HPCC_ADD_SUBDIRECTORY(test t1 t2 t3) - Will add the subdirectory test if t1,t2, or t3 are set to any value other then False/OFF/0

- LOG_PLUGIN - used to log any code based plugin for the platform
- ADD_PLUGIN - adds a plugin with optional build dependencies to the build if dependencies are found

Documentation Macros
====================

- RUN_XSLTPROC - Runs xsltproc using given args
- RUN_FOP - Runs fop using given args
- CLEAN_REL_BOOK - Uses a custom xsl and xsltproc to clean relative book paths from given xml file
- CLEAN_REL_SET - Uses a custom xsl and xsltproc to clean relative set paths from given xml file
- DOCBOOK_TO_PDF - Master macro used to generate pdf, uses above macros

Initfiles macro
===============

- GENERATE_BASH - used to run processor program on given files to replace ###<REPLACE>### with given variables FindXXXXX.cmake

****************************************************
Some standard techniques used in Cmake project files
****************************************************

Common looping
==============

Use FOREACH::

  FOREACH( oITEMS
    item1
    item2
    item3
    item4
    item5
  )
    Actions on each item here.
  ENDFOREACH ( oITEMS )

Common installs over just install
=================================

- install ( FILES ... ) - installs item with 664 permissions
- install ( PROGRAMS ... ) - installs runable item with 755 permissions
- install ( TARGETS ... ) - installs built target with 755 permissions
- install ( DIRECTORY ... ) - installs directory with 777 permissions

Common settings for generated source files
==========================================

- set_source_files_properties(<file> PROPERTIES GENERATED TRUE) - Must be set on generated source files or dependency generation fails and increases build time.

Using custom commands between multiple cmake files
==================================================

- GET_TARGET_PROPERTY(<VAR from other cmake file> <var for this file> LOCATION)
- GET_TARGET_PROPERTY(ESDL_EXE esdl LOCATION) - will get from the top level cache the ESDL_EXE value and set it in esdl for your current cmake file

USE add_custom_command only when 100% needed.

All directories in a cmake project should have a CMakeLists.txt file and be called from the
upper level project with an add_subdirectory or HPCC_ADD_SUBDIRECTORY

When you have a property that will be shared between cmake projects use define_property to
set it in the top level cache.

- define_property(GLOBAL PROPERTY TEST_TARGET BRIEF_DOCS "test doc" FULL_DOCS "Full test doc")
- mark_as_advanced(TEST_TARGET)  - this is required to force the property into the top level cache.CMake Layout:

**********************
FindXXXXX.cmake format
**********************

All of our Find scripts use the following format::

  NOT XXXXX_FOUND
    Externals set
      define needed vars for finding external based libraries/headers

      Use Native set
        use FIND_PATH to locate headers
        use FIND_LIBRARY to find libs

  Include Cmake macros file for package handling
  define package handling args for find return  (This will set XXXXX_FOUND)

  XXXXX_FOUND
    perform any modifications you feel is needed for the find

  Mark defined variables used in package handling args as advanced for return

Will define when done::

  XXXXX_FOUND
  XXXXX_INCLUDE_DIR
  XXXXX_LIBRARIES

(more can be defined, but must be at min the previous unless looking for only a binary)

For an example, see FindAPR.cmake
