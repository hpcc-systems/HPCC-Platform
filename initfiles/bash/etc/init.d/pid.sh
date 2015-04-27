#!/bin/bash
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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

#    check_status
#    Parameters:
#       1: PID FILE PATH       (string)
#       2: LOCK FILE PATH      (string)
#       3: COMP PID FILE PATH  (string)
#       4: SENTINEL FILE CHECK (bool)
#               1 = check
#    Return:
#       0: Running Healthy
#       1: Stopped Healthy
#       2: Running w/ no sentinel file
#       3: Stopped except sentinel orphaned
#       4: not yet in up state, details in debug mode
check_status() {
    PIDFILEPATH=$1
    LOCKFILEPATH=$2
    COMPPIDFILEPATH=$3
    SENTINELFILECHK=$4

    checkPid $PIDFILEPATH
    local pidfilepathExists=$__flagPid
    checkPid $COMPPIDFILEPATH
    local comppidfilepathExists=$__flagPid
    locked $LOCKFILEPATH
    local componentLocked=$flagLocked
    checkPidExist $PIDFILEPATH
    local initRunning=$__pidExists
    checkPidExist $COMPPIDFILEPATH
    local compRunning=$__pidExists
    checkSentinelFile
    local sentinelFlag=$?

    # check if running and healthy
    if [ $pidfilepathExists -eq 1 ] && [ $comppidfilepathExists -eq 1 ] && [ $componentLocked -eq 1 ] && [ $initRunning -eq 1 ] && [ $compRunning -eq 1 ]; then
      if [ ${DEBUG} != "NO_DEBUG" ]; then
        echo "everything is up except sentinel"
      fi
      if [ ${SENTINELFILECHK} -eq 1 ]; then
        if [ ${sentinelFlag} -eq 0 ]; then
          if [ ${DEBUG} != "NO_DEBUG" ]; then
            echo "Sentinel is now up"
          fi
          return 0
        else
          if [ ${DEBUG} != "NO_DEBUG" ]; then
            echo "Sentinel not yet located, process currently unhealthy"
          fi
          return 2
        fi
      else
        return 0
      fi
    # check if shutdown and healthy
    elif [ $pidfilepathExists -eq 0 ] && [ $comppidfilepathExists -eq 0 ] && [ $componentLocked -eq 0 ] && [ $initRunning -eq 0 ] && [ $compRunning -eq 0 ]; then
      if [ ${SENTINELFILECHK} -eq 1 ]; then
        if [ ${sentinelFlag} -eq 0 ]; then
          if [ ${DEBUG} != "NO_DEBUG" ]; then
            echo "Sentinel is up but orphaned"
          fi
          return 3
        else
          if [ ${DEBUG} != "NO_DEBUG" ]; then
            echo "Sentinel is now down"
          fi
          return 1
        fi
      else
        return 1
      fi
    else
      if [ "${DEBUG}" != "NO_DEBUG" ]; then
        [ $pidfilepathExists -eq 0 ]     && log_failure_msg "pid file path does not exist: $1"
        [ $comppidfilepathExists -eq 0 ] && log_failure_msg "comp pid file path does not exist: $3"
        [ $componentLocked -eq 0 ]       && log_failure_msg "component is not locked: $2"
        [ $initRunning -eq 0 ]           && log_failure_msg "process for ${compName}_init.pid is not running"
        [ $compRunning -eq 0 ]           && log_failure_msg "process for ${compName}.pid is not running"
      fi
      return 4
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
