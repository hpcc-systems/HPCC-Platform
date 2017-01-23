################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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
configure_file("configmgr.uninstall.in" "configmgr.uninstall" @ONLY)
configure_file("configmgr.install.in" "configmgr.install" @ONLY)
configure_file("configmgr.conf.in" "configmgr.conf" @ONLY)
configure_file("esp.xml.in" "esp.xml" @ONLY)
install(FILES
	${CMAKE_CURRENT_BINARY_DIR}/configmgr.conf
	${CMAKE_CURRENT_BINARY_DIR}/esp.xml
	DESTINATION etc/${DIR_NAME}/configmgr
	COMPONENT Runtime)
install(FILES
	${CMAKE_CURRENT_BINARY_DIR}/configmgr.install
	DESTINATION etc/init.d/install
	COMPONENT Runtime)
install(FILES
	${CMAKE_CURRENT_BINARY_DIR}/configmgr.uninstall
	DESTINATION etc/init.d/uninstall
	COMPONENT Runtime)
