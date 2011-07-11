#!/bin/bash
################################################################################
## Copyright © 2011 HPCC Systems.  All rights reserved.
################################################################################

echo "Removing RPM"
rpm -e hpccsystems-clienttools
rpm -e hpccsystems-graphcontrol
rpm -e hpccsystems-documentation
rpm -e hpccsystems-platform

echo "Removing user"
if [ -e /usr/sbin/userdel ]; then
    /usr/sbin/userdel hpcc
elif [ -e /usr/bin/userdel ]; then
    /usr/bin/userdel hpcc
elif [ -e /bin/userdel ]; then
    /bin/userdel hpcc
fi

rm -rf /Users/hpcc

echo "Removing Directory - /opt/HPCCSystems/*"
rm -rf /opt/HPCCSystems/*

echo "Removing Directory - /etc/HPCCSystems/*"
rm -rf /etc/HPCCSystems/*

for i in lock run log lib; do 
    echo "Removing Directory - /var/$i/HPCCSystems/*"
    rm -rf /var/$i/HPCCSystems/*; 
done

exit 0
