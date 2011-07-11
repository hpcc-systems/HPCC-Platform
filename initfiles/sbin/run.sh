#!/bin/bash
################################################################################
## Copyright © 2011 HPCC Systems.  All rights reserved.
################################################################################

#################################################################################
# Description:
#        This is a script to run a command from one machine over to the cluster
#        of machines. 
#
#################################################################################

username=$1
password=$2
cmd=$3

EXPECTEDARGS=3

if [ $# -ne $EXPECTEDARGS ]; then
    echo "USAGE:./run.sh <username> <password> <command to run> "
    exit
fi
 
instance_1_server=10.239.219.1
instance_2_server=10.239.219.2
instance_3_server=10.239.219.3
instance_4_server=10.239.219.4
 
servers=("$instance_1_server" "$instance_2_server" "$instance_3_server" "$instance_4_server")
i=1
for server in ${servers[@]}; do
    expect -c "
              set timeout -1
              exp_internal 1 # uncomment for debugging
              spawn /usr/bin/ssh $username@$server $cmd
              expect { 
                "*?assword:*" { send $password\r\n; interact } 
            eof { exit }
              }
              exit
              "
    let "i=i+1"
done
