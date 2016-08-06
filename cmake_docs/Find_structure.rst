Find*****.cmake

- Will define when done:
    - *****_FOUND
    - *****_INCLUDE_DIR
    - *****_LIBRARIES
    (more can be defined, but must be at min the previous unless looking for only a binary)

All of our Find scripts use the following format:

NOT *****_FOUND )
    Externals set
        define needed vars for finding external based libraries/headers

    Use Native set
        use FIND_PATH to locate headers
        use FIND_LIBRARY to find libs

    Include Cmake macros file for package handling
    define package handling args for find return  (This will set *****_FOUND)


    *****_FOUND
        perform any modifications you feel is needed for the find

    Mark defined variables used in package handling args as advanced for return


FindAPR.cmake

IF (NOT APR_FOUND)
  if (USE_NATIVE_LIBRARIES)
    # if we didn't find in externals, look in system include path
    FIND_PATH (APR_INCLUDE_DIR NAMES apr/apr.h)
    FIND_LIBRARY (APR_LIBRARIES NAMES apr)
  endif()

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(APR DEFAULT_MSG
    APR_LIBRARIES
    APR_INCLUDE_DIR
  )

  MARK_AS_ADVANCED(APR_INCLUDE_DIR APR_LIBRARIES)
ENDIF()
