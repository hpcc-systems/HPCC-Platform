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


checkPidExists() {
    PIDFILEPATH=$1
    getPid ${PIDFILEPATH}
    [[ ${__pidValue} -ne 0 ]] && kill -0 ${__pidValue} >/dev/null 2>&1
    return $?
}

checkSentinelFile() {
    FILEPATH="${runtime}/${compName}"
    if [[ -d ${FILEPATH} ]]; then
       fileCheckOP=$(find ${FILEPATH} -name "*senti*")
       [[ ! -z "${fileCheckOP}" ]] && return 1
    fi
    return 0
}
