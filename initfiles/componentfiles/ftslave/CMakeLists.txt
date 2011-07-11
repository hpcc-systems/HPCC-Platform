## Copyright © 2011 HPCC Systems.  All rights reserved.

SET(outFiles)
GENERATE_BASH(processor ${bash-vars} "run_ftslave.in" outFiles)
ADD_CUSTOM_TARGET(ProcessFiles-initfiles-componentfiles-ftslave ALL DEPENDS ${outFiles})
