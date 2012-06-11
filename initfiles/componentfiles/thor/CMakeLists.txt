SET(outFiles)
GENERATE_BASH(processor ${bash-vars} "start_backupnode.in" outFiles)
ADD_CUSTOM_TARGET(ProcessFiles-initfiles-componentfiles-thor ALL DEPENDS ${outFiles})

FOREACH( iFILES
    ${outFiles}
    ${CMAKE_CURRENT_SOURCE_DIR}/get_thor_options
    ${CMAKE_CURRENT_SOURCE_DIR}/makethorgroup
    ${CMAKE_CURRENT_SOURCE_DIR}/multiscp
    ${CMAKE_CURRENT_SOURCE_DIR}/multissh
    ${CMAKE_CURRENT_SOURCE_DIR}/scpslaves
    ${CMAKE_CURRENT_SOURCE_DIR}/setup_nfs
    ${CMAKE_CURRENT_SOURCE_DIR}/setup_one_nfs
    ${CMAKE_CURRENT_SOURCE_DIR}/sshslaves
    ${CMAKE_CURRENT_SOURCE_DIR}/start_slave
    ${CMAKE_CURRENT_SOURCE_DIR}/start_slaves
    ${CMAKE_CURRENT_SOURCE_DIR}/start_thor
    ${CMAKE_CURRENT_SOURCE_DIR}/run_thor
    ${CMAKE_CURRENT_SOURCE_DIR}/stop_thor
    ${CMAKE_CURRENT_SOURCE_DIR}/stop_slaves
)
    Install ( PROGRAMS ${iFILES} DESTINATION ${OSSDIR}/bin )
ENDFOREACH ( iFILES )
