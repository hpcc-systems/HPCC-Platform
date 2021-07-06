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

# Checks if Pid Directory exists and creates if it doesn't exist
checkPidDir () {
    PIDFILEPATH=$1
    if [[ -e ${PIDFILEPATH} ]]; then
        log_success_msg ""
    else
        log_failure_msg "" 
        echo "Creating a Pid directory"
        mkdir -P ${PIDFILEPATH} 
        if [[ ! -e ${PIDFILEPATH} ]]; then
            log  "Can not create a Pid directory $PIDFILEPATH"
        else
            log_success_msg
        fi
    fi
}

# Creats a Pid file
createPid () {
    PIDFILEPATH=$1
    PIDNO=$2
    checkPid ${PIDFILEPATH}
    if [[ $__flagPid -eq 1 ]]; then
        [[ ${DEBUG} != "NO_DEBUG" ]] && log_failure_msg "Pid file already exists"
        log "Pid file already exists"
        __pidCreated=0
    else
        echo $PIDNO > ${PIDFILEPATH}
        checkPid ${PIDFILEPATH}
        if [[ $__flagPid -eq 1 ]]; then
            [[ ${DEBUG} != "NO_DEBUG" ]] && log_success_msg
            __pidCreated=1
        else
            [[ ${DEBUG} != "NO_DEBUG" ]] && log_failure_msg "Failed to create Pid"
            log "Failed to create Pid"
            __pidCreated=0
        fi
    fi
}

# Checks if Pid file exists
checkPid () {
    PIDFILEPATH=$1
    if [[ -e ${PIDFILEPATH} ]]; then
        __flagPid=1
    else
        __flagPid=0
    fi
}

# Reads the Pid file if Pidfile exists
getPid () {
    PIDFILEPATH=$1
    checkPid ${PIDFILEPATH}
    if [[ $__flagPid -eq 1 ]]; then
        __pidValue=$(cat $PIDFILEPATH)
    else
        __pidValue=0
    fi
}

# Removes a Pid file 
removePid () {
    PIDFILEPATH=$1
    checkPid ${PIDFILEPATH}
    if [[ $__flagPid -eq 0 ]]; then
        [[ ${DEBUG} != "NO_DEBUG" ]] && log_failure_msg "Pidfile doesn't exist"
        log "Pid file doesn't exist"
        __pidRemoved=0
    else
        rm -rf ${PIDFILEPATH} > /dev/null 2>&1
        if [[ ! -e ${PIDFILEPATH} ]]; then
            __pidRemoved=1
        else
            [[ ${DEBUG} != "NO_DEBUG" ]] && log_failure_msg "Failed to remove pid"
            log "Failed to remove pid"
            __pidRemoved=0
        fi
    fi
}

checkPidExist() {
    PIDFILEPATH=$1
    getPid ${PIDFILEPATH}
    if [[ $__pidValue -ne 0 ]]; then
        ! kill -0 $__pidValue > /dev/null 2>&1 
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

    locked $LOCKFILEPATH
    local componentLocked=$flagLocked
    checkPidExist $PIDFILEPATH
    local initRunning=$__pidExists
    checkPidExist $COMPPIDFILEPATH
    local compRunning=$__pidExists
    checkSentinelFile
    local sentinelFlag=$?

    # check if running and healthy
    if [[ $componentLocked -eq 1 ]] && [[ $initRunning -eq 1 ]] && [[ $compRunning -eq 1 ]]; then
        [[ ${DEBUG} != "NO_DEBUG" ]] && echo "everything is up except sentinel"
        log "$compName ---> Waiting on Sentinel"
        if [[ ${SENTINELFILECHK} -eq 1 ]]; then
            if [[ ${sentinelFlag} -eq 0 ]]; then
                [[ ${DEBUG} != "NO_DEBUG" ]] && echo "Sentinel not yet located, process currently unhealthy"
                log "$compName ---> Currently Unhealthy"
                return 2 
            fi
            [[ ${DEBUG} != "NO_DEBUG" ]] && echo "Sentinel is now up"
            log "$compName ---> Sentinel Up"
        fi
        return 0
    # check if shutdown and healthy
    elif [[ $componentLocked -eq 0 ]] && [[ $initRunning -eq 0 ]] && [[ $compRunning -eq 0 ]]; then
        if [[ ${SENTINELFILECHK} -eq 1 ]]; then
            if [[ ${sentinelFlag} -eq 1 ]]; then
                [[ ${DEBUG} != "NO_DEBUG" ]] && echo "Sentinel is up but orphaned"
                log "$compName ---> Orphaned State"
                return 3
            fi
            [[ ${DEBUG} != "NO_DEBUG" ]] && echo "Sentinel is now down"
            log "$compName ---> Sentinel Down"
        fi
        return 1
    else
        if [[ "${DEBUG}" != "NO_DEBUG" ]]; then
            [[ $componentLocked -eq 0 ]] && log "$compName ---> component is not locked: $LOCKFILEPATH"
            [[ $initRunning -eq 0 ]]     && log "$compName ---> process for init_${compName}.pid is not running"
            [[ $compRunning -eq 0 ]]     && log "$compName ---> process for ${compName}.pid is not running"
        fi
        return 4
    fi
}

checkSentinelFile() {
    FILEPATH="${runtime}/${compName}"
    if [[ -d ${FILEPATH} ]]; then
       fileCheckOP=$(find ${FILEPATH} -maxdepth 1 -name "*senti*")
       [[ ! -z "${fileCheckOP}" ]] && return 1
    fi
    return 0
}
