#!/bin/bash
## Copyright © 2011 HPCC Systems.  All rights reserved.
#. ./init-functions

checkLockDir () {
    # Checks if Lock Directory exists
    LOCKPATH=$1
    #echo -n "Checking if Lock path exists "
    if [ -d $LOCKPATH ]; then
        log_success_msg "$LOCKPATH ..."
    else 
        log_failure_msg "$LOCKPATH ..."
        echo "Creating Lock Path ..."
        /bin/mkdir $LOCKPATH
        if [ !-d $LOCKPATH ]; then
            log_failure_msg "Can not create Lock Path $LOCKPATH ..."
        fi  
    fi

    exit 1

}


lock () {
    # Creates the /var/lock/subsystem file
    FILE=$1
    #echo -n "Creating a Lock file $FILE "
    locked $FILE
    if [ $flagLocked -eq 1 ]; then
        __lockCreated=0
        if [ ${DEBUG} != "NO_DEBUG" ]; then
            log_failure_msg "Lock file $FILE already exists"
        fi
    else
        /bin/touch $FILE
        locked $FILE
        if [ $flagLocked -eq 1 ]; then
            #log_success_msg 
            __lockCreated=1
        else
            if [ ${DEBUG} != "NO_DEBUG" ]; then
                log_failure_msg "Failed to create file $FILE"
            fi
            __lockCreated=0
        fi
    fi          

}

locked () {

    # Returns True if the lock file exists
    FILE=$1
    if [ -e $FILE ]; then
        flagLocked=1
    else
        flagLocked=0
    fi

}

unlock () {

    # Removes the /var/lock/subsys file
    FILE=$1
    #echo -n "Removing lock file $1 "
    if [ ! -e $FILE ]; then
        if [ ${DEBUG} != "NO_DEBUG" ]; then
            log_failure_msg "Lock file $FILE does not exist"
        fi
        __lockRemoved=0
    else
        /bin/rm -rf $FILE
        if [ -e $FILE ]; then
            if [ ${DEBUG} != "NO_DEBUG" ]; then
                log_failure_msg "File $FILE can not be removed"
            fi
            __lockRemoved=0
        else
            __lockRemoved=1
        fi
    fi

}


