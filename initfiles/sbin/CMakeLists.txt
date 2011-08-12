################################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
################################################################################

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

