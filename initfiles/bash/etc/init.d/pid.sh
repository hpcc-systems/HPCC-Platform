#!/bin/bash
## Copyright © 2011 HPCC Systems.  All rights reserved.
checkPidDir () {
    
    # Checks if Pid Directory exists and creates if it doesn't exist
    PIDFILEPATH=$1
    #echo -n "Pid Path exists"
    if [ -e ${PIDFILEPATH} ]; then
        log_success_msg ""
    else
        log_failure_msg "" 
        echo "Creating a Pid directory"
        /bin/mkdir -P ${PIDFILEPATH} 
        if [ !-e ${PIDFILEPATH} ]; then
            echo "Can not create a Pid directory $PIDFILEPATH"
        else
            log_success_msg
        fi
    fi

}

createPid () {

    # Creats a Pid file
    PIDFILEPATH=$1
    PIDNO=$2
    #echo -n "Creating Pid file $PIDFILEPATH"
    checkPid ${PIDFILEPATH}
    if [ $__flagPid -eq 1 ]; then
        if [ ${DEBUG} != "NO_DEBUG" ]; then
            log_failure_msg "Pid file already exists"
        fi
        __pidCreated=0
    else
        echo $PIDNO > ${PIDFILEPATH} 
        checkPid ${PIDFILEPATH}
        if [ $__flagPid -eq 1 ]; then
            if [ ${DEBUG} != "NO_DEBUG" ]; then
                log_success_msg 
            fi
            __pidCreated=1
        else
            if [ ${DEBUG} != "NO_DEBUG" ]; then
                log_failure_msg "Failed to create Pid"
            fi
            __pidCreated=0
        fi
    fi

}

checkPid () {

    # Checks if Pid file exists
    PIDFILEPATH=$1
    if [ -e ${PIDFILEPATH} ]; then
        __flagPid=1
    else
        __flagPid=0
    fi

}

getPid () {
    
    # Reads the Pid file if Pidfile exists
    PIDFILEPATH=$1
    checkPid ${PIDFILEPATH}
    if [ $__flagPid -eq 1 ]; then
        __pidValue=`/bin/cat $PIDFILEPATH`
    else
        __pidValue=0
    fi

}

removePid () {

    # Removes a Pid file 
    PIDFILEPATH=$1
    #echo -n "Removing Pid file $PIDFILEPATH"
    checkPid ${PIDFILEPATH}
    if [ $__flagPid -eq 0 ]; then
        if [ ${DEBUG} != "NO_DEBUG" ]; then
            log_failure_msg "Pidfile doesn't exist"
        fi
        __pidRemoved=0
    else
        /bin/rm -rf ${PIDFILEPATH}
        if [ ! -e ${PIDFILEPATH} ]; then
            __pidRemoved=1
        else
            if [ ${DEBUG} != "NO_DEBUG" ]; then
                log_failure_msg "Failed to remove pid"
            fi
            __pidRemoved=0
        fi
    fi

 
}
    
checkPidExist() {
        PIDFILEPATH=$1
    getPid ${PIDFILEPATH}
    if [ $__pidValue -ne 0 ]; then
        ! /bin/kill -0 $__pidValue > /dev/null 2>&1 
        __pidExists=$?
    else
        __pidExists=0
    fi
}

check_status() {
    PIDFILEPATH=$1
    LOCKFILEPATH=$2
    COMPPIDFILEPATH=$3
    SENTINELFILECHK=$4
    checkPid ${PIDFILEPATH}
    locked ${LOCKFILEPATH}
    if [ $__flagPid -eq 1 ] && [ $flagLocked -eq 1 ]; then
        checkPidExist ${PIDFILEPATH}
        if [ $__pidExists -eq  1 ] ;then
            checkPid ${COMPPIDFILEPATH}
            if [ $__flagPid -eq 1 ]; then
                checkPidExist ${COMPPIDFILEPATH}
                if [ $__pidExists -eq 1 ];then 
                   if [ ${SENTINELFILECHK} -eq 1 ];then
                      checkSentinelFile ${compName}
                      if [ $? -eq 0 ];then
                         return 0;
                      else 
                         return 1;
                      fi
                   else
                      return 0
                   fi
                else 
                    return 3
                fi
            else
               return 3
            fi
        else
            return 1 
        fi
    elif [ $__flagPid -eq 1 ]; then
        checkPidExist ${PIDFILEPATH}
        if [ $__pidExists -eq  1 ] ;then
            checkPid ${COMPPIDFILEPATH}
            if [ $__flagPid -eq 1 ]; then
                checkPidExist ${COMPPIDFILEPATH}
                if [ $__pidExists -eq 1 ]; then 
                    if [ ${SENTINELFILECHK} -eq 1 ];then
                      checkSentinelFile ${compName}
                      if [ $? -eq 0 ];then
                          return 0;
                      else
                          return 3;
                      fi
                    else
                      return 0 
                    fi
                else 
                   return 3
                fi
            else
               return 3 
            fi
        else
            return 1 
        fi
    elif [ $flagLocked -eq 1 ];then
        return 2
    else
        return 3
    fi
}

checkSentinelFile() {
    FILEPATH="${runtime}/${compName}"
    if [ -d ${FILEPATH} ];then
       fileCheckOP=`find ${FILEPATH} -name "*senti*"`
       if [ ! -z "${fileCheckOP}" ]; then
         return 0
       else
         return 3
       fi
    else
       return 3
    fi
}
