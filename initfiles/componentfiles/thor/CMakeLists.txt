SET(outFiles)
GENERATE_BASH(processor ${bash-vars} "start_backupnode.in" outFiles)
ADD_CUSTOM_TARGET(ProcessFiles-initfiles-componentfiles-thor ALL DEPENDS ${outFiles})

FOREACH( iFILES
    ${outFiles}
    ${CMAKE_CURRENT_SOURCE_DIR}/get_thor_options
    ${CMAKE_CURRENT_SOURCE_DIR}/multiscp
    ${CMAKE_CURRENT_SOURCE_DIR}/multissh
    ${CMAKE_CURRENT_SOURCE_DIR}/scpslaves
    ${CMAKE_CURRENT_SOURCE_DIR}/setup_nfs
    ${CMAKE_CURRENT_SOURCE_DIR}/setup_one_nfs
    ${CMAKE_CURRENT_SOURCE_DIR}/sshslaves
)
    Install ( 
        PROGRAMS ${iFILES} 
        DESTINATION ${EXEC_DIR} 
        COMPONENT Runtime
    )
ENDFOREACH ( iFILES )
