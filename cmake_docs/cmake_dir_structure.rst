CMake Layout:

/
- CMakeLists.txt - Root CMake file
- version.cmake - common cmake file where version variables are set
- build-config.h.cmake - cmake generation template for build-config.h

- cmake_modules/ - Directory storing modules and configurations for CMake
-- Find*****.cmake - CMake find file used to locate libraries, headers, and binaries
-- commonSetup.cmake - common configuration settings for the entire project (contains configure time options)
-- docMacros.cmake - common documentation macros used for generating fop and pdf files
-- optionDefaults.cmake - contains common variables for the platform build
-- distrocheck.sh - script that determines if the OS uses DEB or RPM
-- getpackagerevisionarch.sh - script that returns OS version and arch in format used for packaging
-- dependencies/ - Directory storing dependency files used for package dependencies
--- <OS>.cmake - File containing either DEB or RPM dependencies for the given OS

- build-utils/ - Directory for build related utilities
-- cleanDeb.sh - script that unpacks a deb file and rebuilds with fakeroot to clean up lintain errors/warnings

- initfiles/ - Directory containing init and install based code
-- CMakeLists.txt - initfiles cmake file which defines GENERATE_BASH macro
-- bash-vars.in - cmake template file for bash configuration variables that are injected into all installed bash scripts
-- processor.cpp - simple application used at build time to search and replace ###<REPLACE>### in bash scripts

-- sbin/ - Directory containing administration based scripts
--- add_conf_settings.sh.in - used to add sudoers and limits.conf settings on package install
--- alter_confs.sh - contains functions used by add_conf_settings.sh.in and rm_conf_settings.sh.in
--- complete-uninstall.sh.in - script to remove package and all directories from platform
--- configmgr.in - configmgr start script
--- get_ip_address.sh.in - script to get base ip address of server
--- hpcc-push.sh.in - script to push files to servers defined in environment using ssh keys
--- hpcc-run.sh.in - script to run init commands on servers defined in environment using ssh keys
--- hpcc_setenv.in - source-able file that defines hpcc env vars, used by init system
--- install-cluster.sh.in - script to install platform on a cluster using environment file and expect
--- keygen.sh.in - script to generate ssh keys for hpcc user
--- killconfigmgr - script used to kill configmgr when running (used by configmgr start script)
--- prerm.in - script run pre-remove of the installed DEB or RPM
--- regex.awk.in.cmake - regex awk code used by configmgr
--- remote-install-engine.sh.in - payload install script used by install-cluster.sh
--- rm_conf_settings.sh.in - remove sudoers and limits.conf settings on package uninstall

-- etc/
--- bash_completion/ - contains bash completion scripts used by the bash shell
---- ecl - ecl cmd completion
---- eclagent - eclagent cmd completion
---- eclcc - eclcc cmd completion
---- eclplus - eclplus cmd completion

--- DIR_NAME/ - Directory used to generate /etc/<DIR_NAME> on package install (name is important)
---- environment.conf.in - environment.conf template
---- environment.xml.in - all in one single node template
---- genenvrules.conf - environment generation rules used by config wizard
---- version.in - version file template used by configmgr and esp

---- configmgr/ - Directory containing configmgr based configs
----- configmgr.conf.in - configmgr config file
----- esp.xml.in - esp config used to start the configmgr esp process


--- sshkey/ - contains base ssh keys included in platform packages
---- .ssh.md5 - md5 sums of .ssh dir allowing for comparision at platform start/stop for security check
---- .ssh/ - directory containing key files
----- authorized_keys - file containing keys for hpcc that can be used for auth
----- id_rsa - private ssh key
----- id_rsa.pub - public ssh key

-- bin/ - Directory containing the scripts used to start and stop component processes
--- init_configesp
--- init_dafilesrv.in
--- init_dali
--- init_dfuserver
--- init_eclagent.in
--- init_eclccserver
--- init_eclscheduler
--- init_esp
--- init_ftslave
--- init_roxie
--- init_roxie_cluster
--- init_sasha
--- init_thor

-- componentfiles/ - Directory containing subdirectories of things used by other components/installed items
--- configxml - files used by configmgr
--- ftslave - files used by ftslace
--- launcher - files used by the unity launcher
--- thor - files used by thor

-- bash/
--- etc/
---- init.d/
----- dafilesrv.in - dafilesrv init script
----- export-path
----- hpcc_common.in - common functions for hpcc scripts
----- hpcc-init.in - hpcc-init init script
----- hpcc-init.install -  hpcc-init install script used by package install
----- hpcc-init.uninstall - hpcc-init uninstall script used by package uninstall
----- init-functions - common functions related completely to init
----- install-init.in - script used to install hpcc init system on package install
----- lock.sh - common functions related to lock files
----- lsb-base-logging.sh - common functions related to logging to terminal
----- pid.sh - common functions related to pid files
----- uninstall-init.in - script used to uninstall hpcc init system on package uninstall

--- sbin/ - Directory containing bash based package install sripts
---- bash_postinst.in - post install used by RPM package
---- deb/ - Directory containing DEB specific scripts
----- postinst.in - post install used by DEB package
----- postrm.in - post remove used by DEB package


- docs/ - Directory for documentation building
-- bin/ - Directory containing scripts used by documentation team
-- BuildTools/ - Directory containing xsl files and xsl file templates used by documentation team
-- common/ - Directory containing common files used by all documents built
-- resources/ - directory containing docbook resources needed to build documentation