
function(configure_windows_signing target_name package_file_path)
    if(NOT WIN32)
        message(STATUS "Code signing configuration skipped - not a Windows build")
        return()
    endif()

    if(PLATFORM OR PLUGIN)
        message(STATUS "Code signing configuration skipped - PLATFORM or PLUGIN build")
        return()
    endif()

    # Check for signing passphrase file
    set(DO_CODE_SIGNING FALSE)
    if(NOT "${DIGICERT_KEYPAIR_ALIAS}" STREQUAL "")
        message("-- Using DIGICERT_KEYPAIR_ALIAS for code signing")
        set(DO_CODE_SIGNING TRUE)
    endif()

    if(DO_CODE_SIGNING)
        # Configure NSIS installer signing
        set(CPACK_NSIS_FINALIZE_CMD "smctl sign --simple --keypair-alias \\\"${DIGICERT_KEYPAIR_ALIAS}\\\" --dynamic-auth --timestamp --verbose --exit-non-zero-on-fail --failfast --input")
        
        set(CPACK_NSIS_DEFINES "
            !define MUI_STARTMENUPAGE_DEFAULTFOLDER \\\"${CPACK_PACKAGE_VENDOR}\\\\${version}\\\\${CPACK_NSIS_DISPLAY_NAME}\\\"
            !define MUI_FINISHPAGE_NOAUTOCLOSE
            !finalize '${CPACK_NSIS_FINALIZE_CMD} \\\"%1\\\"'
            !uninstfinalize '${CPACK_NSIS_FINALIZE_CMD} \\\"%1\\\"'
        " PARENT_SCOPE)

        # Create custom target for package signing
        message("-- Signing package: ${package_file_path}")
        add_custom_target(${target_name}
            COMMAND smctl sign --simple --input "${package_file_path}" --keypair-alias "${DIGICERT_KEYPAIR_ALIAS}" --dynamic-auth --timestamp --verbose --exit-non-zero-on-fail --failfast
            COMMENT "Digital Signature"
        )
        add_dependencies(${target_name} PACKAGE)
        set_property(TARGET ${target_name} PROPERTY FOLDER "CMakePredefinedTargets")
        
        message(STATUS "Code signing configured for target: ${target_name}")
    else()
        # No password available, configure basic NSIS settings without signing
        set(CPACK_NSIS_DEFINES "
            !define MUI_STARTMENUPAGE_DEFAULTFOLDER \\\"${CPACK_PACKAGE_VENDOR}\\\\${version}\\\\${CPACK_NSIS_DISPLAY_NAME}\\\"
            !define MUI_FINISHPAGE_NOAUTOCLOSE
        " PARENT_SCOPE)
        
        message(STATUS "Code signing keypair not found - basic NSIS configuration applied")
    endif()
endfunction()