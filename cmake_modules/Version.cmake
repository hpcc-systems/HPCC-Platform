  MACRO(Subversion_WC_PROJNAME dir prefix)
    EXECUTE_PROCESS(COMMAND
          ${Subversion_SVN_EXECUTABLE} pg version:project_name ${dir}
          OUTPUT_VARIABLE ${prefix}_WC_PROJNAME
          ERROR_VARIABLE Subversion_svn_log_error
          RESULT_VARIABLE Subversion_svn_log_result
          OUTPUT_STRIP_TRAILING_WHITESPACE)
    message("---- PROJNAME: ${${prefix}_WC_PROJNAME}")
  ENDMACRO(Subversion_WC_PROJNAME)

  MACRO(Subversion_WC_MAJOR dir prefix)
        EXECUTE_PROCESS(COMMAND
          ${Subversion_SVN_EXECUTABLE} pg version:major ${dir}
          OUTPUT_VARIABLE ${prefix}_WC_MAJOR
          ERROR_VARIABLE Subversion_svn_log_error
          RESULT_VARIABLE Subversion_svn_log_result
          OUTPUT_STRIP_TRAILING_WHITESPACE)
        message("---- MAJOR: ${${prefix}_WC_MAJOR}")
  ENDMACRO(Subversion_WC_MAJOR)

  MACRO(Subversion_WC_MINOR dir prefix)
        EXECUTE_PROCESS(COMMAND
          ${Subversion_SVN_EXECUTABLE} pg version:minor ${dir}
          OUTPUT_VARIABLE ${prefix}_WC_MINOR
          ERROR_VARIABLE Subversion_svn_log_error
          RESULT_VARIABLE Subversion_svn_log_result
          OUTPUT_STRIP_TRAILING_WHITESPACE)
        message("---- MINOR: ${${prefix}_WC_MINOR}")
  ENDMACRO(Subversion_WC_MINOR)

  MACRO(Subversion_WC_POINT dir prefix)
        EXECUTE_PROCESS(COMMAND
          ${Subversion_SVN_EXECUTABLE} pg version:point ${dir}
          OUTPUT_VARIABLE ${prefix}_WC_POINT
          ERROR_VARIABLE Subversion_svn_log_error
          RESULT_VARIABLE Subversion_svn_log_result
          OUTPUT_STRIP_TRAILING_WHITESPACE)
        message("---- POINT: ${${prefix}_WC_POINT}")
  ENDMACRO(Subversion_WC_POINT)

  MACRO(Subversion_WC_SUFFIX dir prefix)
        EXECUTE_PROCESS(COMMAND
          ${Subversion_SVN_EXECUTABLE} pg version:suffix ${dir}
          OUTPUT_VARIABLE ${prefix}_WC_SUFFIX
          ERROR_VARIABLE Subversion_svn_log_error
          RESULT_VARIABLE Subversion_svn_log_result
          OUTPUT_STRIP_TRAILING_WHITESPACE)
        message("---- SUFFIX: ${${prefix}_WC_SUFFIX}")
  ENDMACRO(Subversion_WC_SUFFIX)

  MACRO(Subversion_WC_MATURITY dir prefix)
        EXECUTE_PROCESS(COMMAND
          ${Subversion_SVN_EXECUTABLE} pg version:maturity ${dir}
          OUTPUT_VARIABLE ${prefix}_WC_MATURITY
          ERROR_VARIABLE Subversion_svn_log_error
          RESULT_VARIABLE Subversion_svn_log_result
          OUTPUT_STRIP_TRAILING_WHITESPACE)
    if( ${prefix}_WC_MATURITY STREQUAL "")
        set(${prefix}_WC_MATURITY "dev")
    endif()
        message("---- MATURITY: ${${prefix}_WC_MATURITY}")
  ENDMACRO(Subversion_WC_MATURITY)

  MACRO(Subversion_WC_SEQUENCE dir prefix)
        EXECUTE_PROCESS(COMMAND
          ${Subversion_SVN_EXECUTABLE} pg version:sequence ${dir}
          OUTPUT_VARIABLE ${prefix}_WC_SEQUENCE
          ERROR_VARIABLE Subversion_svn_log_error
          RESULT_VARIABLE Subversion_svn_log_result
          OUTPUT_STRIP_TRAILING_WHITESPACE)
        message("---- SEQUENCE: ${${prefix}_WC_SEQUENCE}")
  ENDMACRO(Subversion_WC_SEQUENCE)

  MACRO(Subversion_WC_PG dir prefix)
    Subversion_WC_PROJNAME(${dir} ${prefix})
    Subversion_WC_MAJOR(${dir} ${prefix})
    Subversion_WC_MINOR(${dir} ${prefix})
    Subversion_WC_POINT(${dir} ${prefix})
    Subversion_WC_SUFFIX(${dir} ${prefix})
    Subversion_WC_MATURITY(${dir} ${prefix})
    Subversion_WC_SEQUENCE(${dir} ${prefix})
  ENDMACRO(Subversion_WC_PG)

