
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
    if(EXISTS "${PROJECT_SOURCE_DIR}/../sign/passphrase.txt")
        file(STRINGS "${PROJECT_SOURCE_DIR}/../sign/passphrase.txt" PFX_PASSWORD LIMIT_COUNT 1)
        message("-- Using passphrase from file: ${PROJECT_SOURCE_DIR}/../sign/passphrase.txt")
    endif()

    if(PFX_PASSWORD)
        # Configure NSIS installer signing
        set(CPACK_NSIS_FINALIZE_CMD "signtool sign /f \\\"${PROJECT_SOURCE_DIR}/../sign/hpcc_code_signing.pfx\\\" /fd SHA256 /p \\\"${PFX_PASSWORD}\\\" /tr http://timestamp.digicert.com /td SHA256")
        
        set(CPACK_NSIS_DEFINES "
            !define MUI_STARTMENUPAGE_DEFAULTFOLDER \\\"${CPACK_PACKAGE_VENDOR}\\\\${version}\\\\${CPACK_NSIS_DISPLAY_NAME}\\\"
            !define MUI_FINISHPAGE_NOAUTOCLOSE
            !finalize '${CPACK_NSIS_FINALIZE_CMD} \\\"%1\\\"'
            !uninstfinalize '${CPACK_NSIS_FINALIZE_CMD} \\\"%1\\\"'
        " PARENT_SCOPE)

        # Create custom target for package signing
        message("-- Signing package: ${package_file_path}")
        add_custom_target(${target_name}
            COMMAND signtool sign /f "${PROJECT_SOURCE_DIR}/../sign/hpcc_code_signing.pfx" /fd "SHA256" /p "${PFX_PASSWORD}" /tr "http://timestamp.digicert.com" /td "SHA256" "${package_file_path}"
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
        
        message(STATUS "Code signing passphrase not found - basic NSIS configuration applied")
    endif()
endfunction()