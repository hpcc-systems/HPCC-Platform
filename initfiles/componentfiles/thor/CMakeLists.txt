SET(outFiles)
GENERATE_BASH(processor ${bash-vars} "start_backupnode.in" outFiles)
ADD_CUSTOM_TARGET(ProcessFiles-initfiles-componentfiles-thor ALL DEPENDS ${outFiles})
