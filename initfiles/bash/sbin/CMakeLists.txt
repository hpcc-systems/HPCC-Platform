## Copyright © 2011 HPCC Systems.  All rights reserved.

ADD_SUBDIRECTORY(deb)
SET(outFiles)
GENERATE_BASH(processor ${bash-vars} "bash_postinst.in" outFiles)
ADD_CUSTOM_TARGET(ProcessFiles-initfiles-bash-sbin ALL DEPENDS ${outFiles})

