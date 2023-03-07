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
#    See the License for the specific language governing permissions and #    limitations under the License.
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
    thorslave
    backupnode
    toposerver)

set(dafilesrv "dafilesrv" "--logDir=${LOG_PATH} --name=%i --daemon" "")
set(dali "daserver" "--daemon %i" "dafilesrv.service")
set(dfuserver "dfuserver" "--daemon %i" "dafilesrv.service")
set(eclagent "agentexec" "--daemon %i" "dafilesrv.service")
set(eclccserver "eclccserver" "--daemon %i" "dafilesrv.service")
set(eclscheduler "eclscheduler" "--daemon %i" "dafilesrv.service")
set(esp "esp" "--daemon %i" "dafilesrv.service")
set(roxie "roxie" "--topology=RoxieTopology.xml --logfile --restarts=2 --stdlog=0 --daemon %i" "dafilesrv.service")
set(sasha "saserver" "--daemon %i" "dafilesrv.service")
set(thor "thormaster_lcr" "--daemon %i MASTER=\${THORMASTER}:\${THORMASTERPORT}" "dafilesrv.service")
set(thorslave "thorslave_lcr" "--daemon %i --master=\${THORMASTER}:\${THORMASTERPORT} --slave=.:\${SLAVEPORT} --slavenum=\${SLAVENUM} --logDir=${LOG_PATH}/\${THORNAME}" "dafilesrv.service")
set(backupnode "backup.sh" "" "")
set(toposerver "toposerver" "--daemon %i" "dafilesrv.service")

foreach(component ${componentList})
    list(GET "${component}" 0 processName)
    list(GET "${component}" 1 processArgs)
    list(GET "${component}" 2 dependencies)
    if("${component}" STREQUAL "thor" OR "${component}" STREQUAL "thorslave" OR "${component}" STREQUAL "hpcc-conf" OR "${component}" STREQUAL "thor-conf" OR "${component}" STREQUAL "backupnode")
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
    dropzone
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

configure_file(hpcc-environment-monitor.service.in hpcc-environment-monitor.service @ONLY)
configure_file(hpcc-environment-monitor.path.in hpcc-environment-monitor.path @ONLY)
configure_file(hpcc-environment-monitor.install.in hpcc-environment-monitor.install @ONLY)
configure_file(hpcc-environment-monitor.uninstall.in hpcc-environment-monitor.uninstall @ONLY)
configure_file(generate-hpccsystems-target.install.in generate-hpccsystems-target.install @ONLY)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/hpcc-environment-monitor.service ${CMAKE_CURRENT_BINARY_DIR}/hpcc-environment-monitor.path
	DESTINATION etc/systemd/system
	COMPONENT Systemd)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/hpcc-environment-monitor.install ${CMAKE_CURRENT_BINARY_DIR}/generate-hpccsystems-target.install DESTINATION etc/init.d/install COMPONENT Systemd)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/hpcc-environment-monitor.uninstall DESTINATION etc/init.d/uninstall COMPONENT Systemd)

## Install helper scripts for thorslaves
configure_file(thorslaves-launch.sh.in thorslaves-launch.sh @ONLY)
configure_file(thorslaves-exec.sh.in thorslaves-exec.sh @ONLY)
configure_file(generate-hpccsystems-target.in generate-hpccsystems-target.sh @ONLY)
install(PROGRAMS
    ${CMAKE_CURRENT_BINARY_DIR}/thorslaves-launch.sh
    ${CMAKE_CURRENT_BINARY_DIR}/thorslaves-exec.sh
    ${CMAKE_CURRENT_BINARY_DIR}/generate-hpccsystems-target.sh
    DESTINATION ${ADMIN_PATH} COMPONENT Systemd)
