## Copyright © 2011 HPCC Systems.  All rights reserved.
SET(outFiles)
GENERATE_BASH(processor ${bash-vars} "postinst.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "postrm.in" outFiles)
ADD_CUSTOM_TARGET(ProcessFiles-initfiles-bash-sbin-deb ALL DEPENDS ${outFiles})

