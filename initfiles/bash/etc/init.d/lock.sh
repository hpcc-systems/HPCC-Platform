#!/bin/bash
################################################################################
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
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
################################################################################
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


