#!/bin/bash
################################################################################
## Copyright © 2011 HPCC Systems.  All rights reserved.
################################################################################

#################################################################################
# Description:
#        This is a script to run a command from one machine. 
#
#################################################################################

server=$1
username=$2
password=$3
cmd=$4

EXPECTEDARGS=4

if [ $# -ne $EXPECTEDARGS ]; then
    echo "USAGE:./run_cmd.sh <server> <username> <password> <command to run> "
    exit
fi
 
expect -c "
          set timeout -1
          # exp_internal 1 # uncomment for debugging
          spawn /usr/bin/ssh $username@$server $cmd
          expect { 
            "*?assword:*" { send $password\r\n; interact } 
            eof { exit }
          }
          exit
          "

exit

