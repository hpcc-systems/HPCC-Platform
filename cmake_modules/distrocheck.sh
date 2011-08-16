#!/bin/sh

#############################################
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

