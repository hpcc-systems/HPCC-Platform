Common Macros for all builds:
    MACRO_ENSURE_OUT_OF_SOURCE_BUILD - prevents building from with in source tree
    HPCC_ADD_EXECUTABLE - simple wrapper around add_executable
    HPCC_ADD_LIBRARY - simple wrapper around add_library
    PARSE_ARGUMENTS - macro that can be used by other macros and functions for arg list parsing
    HPCC_ADD_SUBDIRECTORY - argument controlled add subdirectory wrapper
        HPCC_ADD_SUBDIRECTORY(test t1 t2 t3) - Will add the subdirectory test if t1,t2, or t3 are set to any value other then False/OFF/0

    LOG_PLUGIN - used to log any code based plugin for the platform
    ADD_PLUGIN - adds a plugin with optional build dependencies to the build if dependencies are found

Document Macros:
    RUN_XSLTPROC - Runs xsltproc using given args
    RUN_FOP - Runs fop using given args
    CLEAN_REL_BOOK - Uses a custom xsl and xsltproc to clean relative book paths from given xml file
    CLEAN_REL_SET - Uses a custom xsl and xsltproc to clean relative set paths from given xml file
    DOCBOOK_TO_PDF - Master macro used to generate pdf, uses above macros

Initfiles macro:
    GENERATE_BASH - used to run processer program on given files to replace ###<REPLACE>### with given variables
