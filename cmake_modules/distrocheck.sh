#!/bin/sh

#############################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#
#    limitations under the License.
#    along with All rights reserved. This program is free software: you can redistribute program.  If not, see <http://www.gnu.org/licenses/>.
#############################################

if [ -e /etc/debian_version ]; then
    echo -n "DEB"
    exit 2;
elif [ -e /etc/redhat-release ]; then
    echo -n "RPM"
    exit 1;
elif [ -e /etc/SuSE-release ]; then
    echo -n "RPM"
    exit 1;
elif [ -e /etc/system-release ]; then
    grep -q -i "Amazon Linux" /etc/system-release
    if [ $? = 0 ]; then
        echo -n "RPM"
        exit 1;
    fi
fi

cat /etc/*release > temp.txt

############### RPM DISTROS ##################

# Distribution is openSuse
VALUE=`grep -c -i 'suse' temp.txt` 
if [ $VALUE -ge 1 ]; then
    echo -n "RPM"
    rm temp.txt
    exit 1;
fi

# Distribution is Centos
VALUE=`grep -c -i 'centos' temp.txt` 
if [ $VALUE -ge 1 ]; then
    echo -n "RPM"
    rm temp.txt
    exit 1;
fi

# Distribution is Mandriva 
VALUE=`grep -c -i 'mandr' temp.txt` 
if [ $VALUE -ge 1 ]; then
    echo -n "RPM"
    rm temp.txt
    exit 1;
fi

# Distribution is Redhat
VALUE=`grep -c -i 'redhat' temp.txt` 
if [ $VALUE -ge 1 ]; then
    echo -n "RPM"
    rm temp.txt
    exit 1;
fi

# Distribution is Rocky
VALUE=`grep -c -i 'rocky' temp.txt`
if [ $VALUE -ge 1 ]; then
    echo -n "RPM"
    rm temp.txt
    exit 1;
fi

############### DEB DISTROS ##################

# Distribution is Ubuntu
VALUE=`grep -c -i 'ubuntu' temp.txt` 
if [ $VALUE -ge 1 ]; then
    echo -n "DEB"
    rm temp.txt
    exit 2;
fi

# Distribution is Debian
VALUE=`grep -c -i 'DEBian' temp.txt` 
if [ $VALUE -ge 1 ]; then
    echo -n "DEB"
    rm temp.txt
    exit 2;
fi


############## OTHER DISTROS #################

# Distribution is Gentoo
VALUE=`grep -c -i 'gentoo' temp.txt` 
if [ $VALUE -ge 1 ]; then
    echo -n "TGZ"
    rm temp.txt
    exit 4;
fi

