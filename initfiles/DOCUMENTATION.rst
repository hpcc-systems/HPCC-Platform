===================================================
Initialization and installation structure and usage
===================================================

***************************************
Installation and uninstallation process
***************************************

Init install
============

All items are installed to /opt/HPCCSystems by default (can be changed at configure time).

Part of the install copies files to /etc/HPCCSystems. This process will not replace files
currently in the directory. It will replace files in /etc/HPCCSystems/rpmnew.

Symlinks are installed in /usr/bin pointing to common command-line utilities in /opt/HPCCSystems,
and in /etc/init.d pointing to initialization scripts

Install process internals
=========================

The install-init process makes use of 3 bash functions along with the hpcc_common functions.

*installConfs*
  Used to copy config files into place if they do not exist. It will not replacean existing file.

*installFile*
  Copies a file from one location to another. This will replace an existing file and also has
  an option to create as a symlink instead of copying.

*fileCheck*
  Used to check for ssh keys for the hpcc user along with warning the user if we
  are installing or they are still using (upgrade) our publicly provided ssh keys.

Most install procedures are handled in install-init directly, but install-init also supports
sub-installs using install files that are placed in /opt/HPCCSystems/etc/init.d/install/

The final steps of the install are to set permissions correctly for the hpcc user, along with
calling add_conf_settings.sh to add the sudoers and limits.conf changes.

Init uninstall
==============

This removes all symlinks created during the install process. This also supports sub-installs
using uninstall files that are placed in /opt/HPCCSystems/etc/init.d/uninstall/

********************************
Directory structure of initfiles
********************************

- initfiles/ - Directory containing init and install based code
 - CMakeLists.txt - initfiles cmake file which defines GENERATE_BASH macro
 - bash-vars.in - cmake template file for bash configuration variables that are injected into all installed bash scripts
 - processor.cpp - simple application used at build time to search and replace ###<REPLACE>### in bash scripts

 - sbin/ - Directory containing administration based scripts
  - add_conf_settings.sh.in - used to add sudoers and limits.conf settings on package install
  - alter_confs.sh - contains functions used by add_conf_settings.sh.in and rm_conf_settings.sh.in
  - complete-uninstall.sh.in - script to remove package and all directories from platform
  - configmgr.in - configmgr start script
  - get_ip_address.sh.in - script to get base ip address of server
  - hpcc-push.sh.in - script to push files to servers defined in environment using ssh keys
  - hpcc-run.sh.in - script to run init commands on servers defined in environment using ssh keys
  - hpcc_setenv.in - source-able file that defines hpcc env vars, used by init system
  - install-cluster.sh.in - script to install platform on a cluster using environment file and expect
  - keygen.sh.in - script to generate ssh keys for hpcc user
  - killconfigmgr - script used to kill configmgr when running (used by configmgr start script)
  - prerm.in - script run pre-remove of the installed DEB or RPM
  - regex.awk.in.cmake - regex awk code used by configmgr
  - remote-install-engine.sh.in - payload install script used by install-cluster.sh
  - rm_conf_settings.sh.in - remove sudoers and limits.conf settings on package uninstall

 - etc/
  - bash_completion/ - contains bash completion scripts used by the bash shell
   - ecl - ecl cmd completion
   - eclagent - eclagent cmd completion
   - eclcc - eclcc cmd completion
   - eclplus - eclplus cmd completion
  - DIR_NAME/ - Directory used to generate /etc/<DIR_NAME> on package install (name is important)
   - environment.conf.in - environment.conf template
   - environment.xml.in - all in one single node template
   - genenvrules.conf - environment generation rules used by config wizard
   - version.in - version file template used by configmgr and esp

   - configmgr/ - Directory containing configmgr based configs
    - configmgr.conf.in - configmgr config file
    - esp.xml.in - esp config used to start the configmgr esp process

  - sshkey/ - contains base ssh keys included in platform packages
   - .ssh.md5 - md5 sums of .ssh dir allowing for comparision at platform start/stop for security check
   - .ssh/ - directory containing key files
    - authorized_keys - file containing keys for hpcc that can be used for auth
    - id_rsa - private ssh key
    - id_rsa.pub - public ssh key

 - bin/ - Directory containing the scripts used to start and stop component processes

 - componentfiles/ - Directory containing subdirectories of things used by other components/installed items
  - configxml - files used by configmgr
  - ftslave - files used by ftslace
  - launcher - files used by the unity launcher
  - thor - files used by thor

 - bash/
  - etc/
   - init.d/
    - dafilesrv.in - dafilesrv init script
    - export-path
    - hpcc_common.in - common functions for hpcc scripts
    - hpcc-init.in - hpcc-init init script
    - hpcc-init.install -  hpcc-init install script used by package install
    - hpcc-init.uninstall - hpcc-init uninstall script used by package uninstall
    - init-functions - common functions related completely to init
    - install-init.in - script used to install hpcc init system on package install
    - lock.sh - common functions related to lock files
    - lsb-base-logging.sh - common functions related to logging to terminal
    - pid.sh - common functions related to pid files
    - uninstall-init.in - script used to uninstall hpcc init system on package uninstall

  - sbin/ - Directory containing bash based package install sripts
   - bash_postinst.in - post install used by RPM package
   - deb/ - Directory containing DEB specific scripts
    - postinst.in - post install used by DEB package
    - postrm.in - post remove used by DEB package
