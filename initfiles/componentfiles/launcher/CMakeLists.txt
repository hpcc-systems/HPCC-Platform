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

install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/hpcc-systems.desktop DESTINATION share COMPONENT Runtime )
install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/LN.png DESTINATION share COMPONENT Runtime )

if(PLATFORM AND NOT CONTAINERIZED)
    SET_DEPENDENCIES(CPACK_DEBIAN_PACKAGE_DEPENDS xterm)
    configure_file(hpcc-systems-desktop.install.in hpcc-systems-desktop.install @ONLY)
    configure_file(hpcc-systems-desktop.uninstall.in hpcc-systems-desktop.uninstall @ONLY)
    install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/hpcc-systems-desktop.install DESTINATION etc/init.d/install COMPONENT Runtime)
    install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/hpcc-systems-desktop.uninstall DESTINATION etc/init.d/uninstall COMPONENT Runtime)
endif()
