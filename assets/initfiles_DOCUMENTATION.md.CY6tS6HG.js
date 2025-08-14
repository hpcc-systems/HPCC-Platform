import{_ as i,a as t,o as e,ag as s}from"./chunks/framework.Do1Zayaf.js";const h=JSON.parse('{"title":"Initialization and installation structure and usage","description":"","frontmatter":{},"headers":[],"relativePath":"initfiles/DOCUMENTATION.md","filePath":"initfiles/DOCUMENTATION.md","lastUpdated":1755191786000}'),a={name:"initfiles/DOCUMENTATION.md"};function l(o,n,c,r,p,d){return e(),t("div",null,n[0]||(n[0]=[s(`<h1 id="initialization-and-installation-structure-and-usage" tabindex="-1">Initialization and installation structure and usage <a class="header-anchor" href="#initialization-and-installation-structure-and-usage" aria-label="Permalink to &quot;Initialization and installation structure and usage&quot;">​</a></h1><h2 id="installation-and-uninstallation-process" tabindex="-1">Installation and uninstallation process <a class="header-anchor" href="#installation-and-uninstallation-process" aria-label="Permalink to &quot;Installation and uninstallation process&quot;">​</a></h2><h3 id="init-install" tabindex="-1">Init install <a class="header-anchor" href="#init-install" aria-label="Permalink to &quot;Init install&quot;">​</a></h3><p>All items are installed to /opt/HPCCSystems by default (can be changed at configure time).</p><p>Part of the install copies files to /etc/HPCCSystems. This process will not replace files currently in the directory. It will replace files in /etc/HPCCSystems/rpmnew.</p><p>Symlinks are installed in /usr/bin pointing to common command-line utilities in /opt/HPCCSystems, and in /etc/init.d pointing to initialization scripts</p><h3 id="install-process-internals" tabindex="-1">Install process internals <a class="header-anchor" href="#install-process-internals" aria-label="Permalink to &quot;Install process internals&quot;">​</a></h3><p>The install-init process makes use of 3 bash functions along with the hpcc_common functions.</p><p><em>installConfs</em></p><p>: Used to copy config files into place if they do not exist. It will not replacean existing file.</p><p><em>installFile</em></p><p>: Copies a file from one location to another. This will replace an existing file and also has an option to create as a symlink instead of copying.</p><p><em>fileCheck</em></p><p>: Used to check for ssh keys for the hpcc user along with warning the user if we are installing or they are still using (upgrade) our publicly provided ssh keys.</p><p>Most install procedures are handled in install-init directly, but install-init also supports sub-installs using install files that are placed in /opt/HPCCSystems/etc/init.d/install/</p><p>The final steps of the install are to set permissions correctly for the hpcc user, along with calling add_conf_settings.sh to add the limits.conf changes.</p><h3 id="init-uninstall" tabindex="-1">Init uninstall <a class="header-anchor" href="#init-uninstall" aria-label="Permalink to &quot;Init uninstall&quot;">​</a></h3><p>This removes all symlinks created during the install process. This also supports sub-installs using uninstall files that are placed in /opt/HPCCSystems/etc/init.d/uninstall/</p><h2 id="directory-structure-of-initfiles" tabindex="-1">Directory structure of initfiles <a class="header-anchor" href="#directory-structure-of-initfiles" aria-label="Permalink to &quot;Directory structure of initfiles&quot;">​</a></h2><p>- initfiles/ - Directory containing init and install based code</p><p>: - CMakeLists.txt - initfiles cmake file which defines GENERATE_BASH macro - bash-vars.in - cmake template file for bash configuration variables that are injected into all installed bash scripts - processor.cpp - simple application used at build time to search and replace ###&lt;REPLACE&gt;### in bash scripts</p><pre><code>\\- sbin/ - Directory containing administration based scripts

:   -   add\\_conf\\_settings.sh.in - used to add limits.conf settings
        on package install
    -   alter\\_confs.sh - contains functions used by
        add\\_conf\\_settings.sh.in and rm\\_conf\\_settings.sh.in
    -   complete-uninstall.sh.in - script to remove package and all
        directories from platform
    -   configmgr.in - configmgr start script
    -   hpcc-push.sh.in - script to push files to servers defined in
        environment using ssh keys
    -   hpcc-run.sh.in - script to run init commands on servers
        defined in environment using ssh keys
    -   hpcc\\_setenv.in - source-able file that defines hpcc env
        vars, used by init system
    -   install-cluster.sh.in - script to install platform on a
        cluster using environment file and expect
    -   keygen.sh.in - script to generate ssh keys for hpcc user
    -   killconfigmgr - script used to kill configmgr when running
        (used by configmgr start script)
    -   prerm.in - script run pre-remove of the installed DEB or RPM
    -   regex.awk.in.cmake - regex awk code used by configmgr
    -   remote-install-engine.sh.in - payload install script used by
        install-cluster.sh
    -   rm\\_conf\\_settings.sh.in - remove limits.conf settings on
        package uninstall

\\- etc/

:   

    \\- bash\\_completion/ - contains bash completion scripts used by the bash shell

    :   -   ecl - ecl cmd completion
        -   eclagent - eclagent cmd completion
        -   eclcc - eclcc cmd completion
        -   eclplus - eclplus cmd completion

    \\- DIR\\_NAME/ - Directory used to generate /etc/\\&lt;DIR\\_NAME\\&gt; on package install (name is important)

    :   -   environment.conf.in - environment.conf template
        -   environment.xml.in - all in one single node template
        -   genenvrules.conf - environment generation rules used by
            config wizard
        -   version.in - version file template used by configmgr and
            esp

        \\- configmgr/ - Directory containing configmgr based configs

        :   -   configmgr.conf.in - configmgr config file
            -   esp.xml.in - esp config used to start the configmgr
                esp process

    \\- sshkey/ - contains base ssh keys included in platform packages

    :   -   .ssh.md5 - md5 sums of .ssh dir allowing for comparision
            at platform start/stop for security check

        \\- .ssh/ - directory containing key files

        :   -   authorized\\_keys - file containing keys for hpcc
                that can be used for auth
            -   id\\_rsa - private ssh key
            -   id\\_rsa.pub - public ssh key

-   bin/ - Directory containing the scripts used to start and stop
    component processes

\\- componentfiles/ - Directory containing subdirectories of things used by other components/installed items

:   -   configxml - files used by configmgr
    -   ftslave - files used by ftslace
    -   launcher - files used by the unity launcher
    -   thor - files used by thor

\\- bash/

:   

    \\- etc/

    :   

        \\- init.d/

        :   -   dafilesrv.in - dafilesrv init script
            -   export-path
            -   hpcc\\_common.in - common functions for hpcc scripts
            -   hpcc-init.in - hpcc-init init script
            -   hpcc-init.install - hpcc-init install script used by
                package install
            -   hpcc-init.uninstall - hpcc-init uninstall script
                used by package uninstall
            -   init-functions - common functions related completely
                to init
            -   install-init.in - script used to install hpcc init
                system on package install
            -   lock.sh - common functions related to lock files
            -   lsb-base-logging.sh - common functions related to
                logging to terminal
            -   pid.sh - common functions related to pid files
            -   uninstall-init.in - script used to uninstall hpcc
                init system on package uninstall

    \\- sbin/ - Directory containing bash based package install sripts

    :   -   bash\\_postinst.in - post install used by RPM package

        \\- deb/ - Directory containing DEB specific scripts

        :   -   postinst.in - post install used by DEB package
            -   postrm.in - post remove used by DEB package
</code></pre>`,22)]))}const m=i(a,[["render",l]]);export{h as __pageData,m as default};
