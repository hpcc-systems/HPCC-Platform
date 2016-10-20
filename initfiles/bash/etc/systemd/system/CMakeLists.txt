################################################################################
#    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.
#
#    All rights reserved. This program is free software: you can redistribute it
#    and/or modify
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

## Generate and setup instance service files
set(componentList
    hpcc-conf
    thor-conf
    dafilesrv
    eclagent
    dali
    dfuserver
    eclccserver
    eclscheduler
    esp
    roxie
    sasha
    thor
    thorslave)

set(dafilesrv "dafilesrv" "-L ${LOG_PATH} -I %i -D" "" "Facilitate file transfers across a hpcc cluster.")
set(dali "daserver" "--daemon %i" "" "Stores metadata on queries, workunits, data.")
set(dfuserver "dfuserver" "--daemon %i" "" "data file information store.")
set(eclagent "agentexec" "--daemon %i" "" "ecl workunit controller.")
set(eclccserver "eclccserver" "--daemon %i" "" "compilation server for ecl to c++ to machine code.")
set(eclscheduler "eclscheduler" "--daemon %i" "" "scheduler for ecl workunits.")
#TODO: look into snmpid=$$ for esp processArgs
set(esp "esp" "--daemon %i" "" "Handles remote calls to the hpccsystems-platform.")
set(roxie "roxie" "--topology=RoxieTopology.xml --logfile --restarts=2 --stdlog=0 --daemon %i" "" "Allows for queries of structured data.")
set(sasha "saserver" "--daemon %i" "" "Metadata in-memory store.")
#TODO: thor environment variables
set(thor "thormaster_lcr" "--daemon %i MASTER=\${THORMASTER}:\${THORMASTERPORT}" "" "Coalesces raw data into structured data that can be effectiently served up by roxie.")
set(thorslave "thorslave_lcr" "--daemon %i master=\${THORMASTER}:\${THORMASTERPORT} slave=.:\${SLAVEPORT} slavenum=\${SLAVENUM} logDir=${LOG_PATH}/\${THORNAME}" "" "Slave for thor processing")

foreach(component ${componentList})
    list(GET "${component}" 0 processName)
    list(GET "${component}" 1 processArgs)
    list(GET "${component}" 2 dependencies)
    list(GET "${component}" 3 description)
    if("${component}" STREQUAL "thor" OR "${component}" STREQUAL "thorslave" OR "${component}" STREQUAL "hpcc-conf" OR "${component}" STREQUAL "thor-conf")
        configure_file(${component}@instance.service.in ${component}@.service @ONLY)
    else()
        configure_file(component@instance.service.in ${component}@.service @ONLY)
    endif()
    configure_file(service-unit.install.in ${component}-service.install @ONLY)
    configure_file(service-unit.uninstall.in ${component}-service.uninstall @ONLY)
    install(FILES ${CMAKE_CURRENT_BINARY_DIR}/${component}-service.install DESTINATION etc/init.d/install COMPONENT Systemd)
    install(FILES ${CMAKE_CURRENT_BINARY_DIR}/${component}-service.uninstall DESTINATION etc/init.d/uninstall COMPONENT Systemd)
    install(FILES ${CMAKE_CURRENT_BINARY_DIR}/${component}@.service DESTINATION etc/systemd/system COMPONENT Systemd)
endforeach()

## Generate and setup standalone service files
set(standaloneList
    configmgr
    configmgr-conf)

foreach(component ${standaloneList})
    configure_file(${component}.service.in ${component}.service @ONLY)
    configure_file(standalone-service-unit.install.in ${component}-standalone-service.install @ONLY)
    configure_file(standalone-service-unit.uninstall.in ${component}-standalone-service.uninstall @ONLY)
    install(FILES ${CMAKE_CURRENT_BINARY_DIR}/${component}.service DESTINATION etc/systemd/system COMPONENT Systemd)
    install(FILES ${CMAKE_CURRENT_BINARY_DIR}/${component}-standalone-service.install DESTINATION etc/init.d/install COMPONENT Systemd)
    install(FILES ${CMAKE_CURRENT_BINARY_DIR}/${component}-standalone-service.uninstall DESTINATION etc/init.d/uninstall COMPONENT Systemd)
endforeach()

## Install helper scripts for thorslaves
configure_file(thorslaves-launch.sh.in thorslaves-launch.sh @ONLY)
configure_file(thorslaves-exec.sh.in thorslaves-exec.sh @ONLY)
install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/thorslaves-launch.sh ${CMAKE_CURRENT_BINARY_DIR}/thorslaves-exec.sh DESTINATION ${ADMIN_PATH} COMPONENT Systemd)
