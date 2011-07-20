## Copyright © 2011 HPCC Systems.  All rights reserved.

SET(outFiles)
GENERATE_BASH(processor ${bash-vars} "add_conf_settings.sh.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "hpcc_setenv.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "keygen.sh.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "prerm.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "rm_conf_settings.sh.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "hpcc-push.sh.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "hpcc-run.sh.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "install-cluster.sh.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "remote-install-engine.sh.in" outFiles)
ADD_CUSTOM_TARGET(ProcessFiles-initfiles-sbin ALL DEPENDS ${outFiles})

