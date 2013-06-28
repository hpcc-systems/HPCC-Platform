Init install:

All items are installed to /opt/HPCCSystems by default (can be changed at configure time).

Part of the install copies files to /etc/HPCCSystems. This process will not replace files currently in the directory. It will replace files in /etc/HPCCSystems/rpmnew.

The install-init process makes use of 3 bash functions along with the hpcc_common functions:
    installConfs
    installFile
    fileCheck

installConfs is used to copy config files into place if they do not exist. It will not replace an existing file.

installFile will copy a file from one location to another. This will replace an existing file and also has an option to create as a symlink instead of copying.

fileCheck is used to check for ssh keys for the hpcc user along with warning the user if we are installing or they are still using (upgrade) our publicly provided ssh keys.

Most install procedures are handled in install-init directly, but install-init also supports sub installs using install files that are placed in /opt/HPCCSystems/etc/init.d/install/

(Most files are installed as symlinks)

The final steps of the install are to set permissions correctly for the hpcc user along with calling add_conf_settings.sh to add the sudoers and limits.conf changes.


Init uninstall:

This removes all symlinks created during the install process. This also supports sub installs using uninstall files that are placed in /opt/HPCCSystems/etc/init.d/uninstall/