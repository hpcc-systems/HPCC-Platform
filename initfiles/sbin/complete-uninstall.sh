#!/bin/bash
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
    dpkg --purge hpccsystems-platform
    dpkg --purge hpccsystems-clienttools
    dpkg --purge hpccsystems-graphcontrol
    dpkg --purge hpccsystems-documentation
elif [ -e /etc/redhat-release -o -e /etc/SuSE-release ]; then
    echo "Removing RPM"
    rpm -e hpccsystems-clienttools
    rpm -e hpccsystems-graphcontrol
    rpm -e hpccsystems-documentation
    rpm -e hpccsystems-platform
fi

echo "Removing Directory - /opt/HPCCSystems/*"
rm -rf /opt/HPCCSystems/*

echo "Removing Directory - /etc/HPCCSystems/*"
rm -rf /etc/HPCCSystems/*

for i in lock run log lib; do 
    echo "Removing Directory - /var/$i/HPCCSystems/*"
    rm -rf /var/$i/HPCCSystems/*; 
done

echo "Removing user"
if [ -e /usr/sbin/userdel ]; then
    /usr/sbin/userdel -r hpcc
elif [ -e /usr/bin/userdel ]; then
    /usr/bin/userdel -r hpcc
elif [ -e /bin/userdel ]; then
    /bin/userdel -r hpcc
fi

exit 0
