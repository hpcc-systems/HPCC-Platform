#!/bin/bash
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################
#. ./init-functions

checkLockDir () {
    # Checks if Lock Directory exists
    LOCKPATH=$1
    #echo -n "Checking if Lock path exists "
    if [ -d $LOCKPATH ]; then
        log "$LOCKPATH ..."
    else 
        log "$LOCKPATH ..."
        log "Creating Lock Path ..."
        mkdir $LOCKPATH
        if [ !-d $LOCKPATH ]; then
            log "Can not create Lock Path $LOCKPATH ..."
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
        log "Lock file $FILE already exists"
    else
        touch $FILE
        locked $FILE
        if [ $flagLocked -eq 1 ]; then
            #log_success_msg 
            __lockCreated=1
        else
            log "Failed to create file $FILE"
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
        log "Lock file $FILE does not exist"
        __lockRemoved=0
    else
        rm -rf $FILE
        if [ -e $FILE ]; then
            log "File $FILE can not be removed"
            __lockRemoved=0
        else
            __lockRemoved=1
        fi
    fi

}


