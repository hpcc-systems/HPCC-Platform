## Copyright © 2011 HPCC Systems.  All rights reserved.

SET(outFiles)
GENERATE_BASH(processor ${bash-vars} "init_dafilesrv.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "init_eclagent.in" outFiles)
ADD_CUSTOM_TARGET(ProcessFiles-initfiles-bin ALL DEPENDS ${outFiles})

