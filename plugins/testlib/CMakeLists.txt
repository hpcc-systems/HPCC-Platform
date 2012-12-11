###############################################################################
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

#####################################################
# Description:
# ------------
#    Cmake example template for fictional plugin testlib.
#
# * This is a non-working example.
#####################################################
cmake_minimum_required(VERSION 2.8)
project( testlib )

###
## Version Information
###
set ( HPCC_PROJECT "plugin" )
set ( HPCC_MAJOR 0 )
set ( HPCC_MINOR 0 )
set ( HPCC_POINT 1 )
set ( HPCC_MATURITY "release" )
set ( HPCC_SEQUENCE 1 )
###


# Using the Hpcc FIND_PACKAGE config step 3 and 4.
# 4a
find_package( Hpcc PATHS /opt/HPCCSystems/lib )

# 4b
find_package( Hpcc PATHS /home/username/<build directory of platform> )

# Defines CMAKE_MODULES_PATH to include the Platform's cmake_modules directory.
set( CMAKE_MODULE_PATH ${Hpcc_CMAKE_MODULE_PATH} )

# Include the HpccUse.cmake file to setup base needs of the system.
include( ${Hpcc_USE_FILE} )

# Optional line.
# Enables the `make package` command to create an RPM or DEB file for the plugin.
include( ${CMAKE_MODULE_PATH}/cpackSetup.cmake )

set (    SRCS
         testlib.cpp
    )

# Note the use of Hpcc_INCLUDE_DIRS to get the base include path as defined by the FIND_PACKAGE.
include_directories (
    ${Hpcc_INCLUDE_DIRS}/system/include
    ${Hpcc_INCLUDE_DIRS}/system/jlib
)


# Usage of the HPCC_ADD_LIBRARY macro to correctly add a build object for a library for the Hpcc Platform.
HPCC_ADD_LIBRARY( testlib SHARED ${SRCS} )

# Option line.
# Install directive to allow the installation of the built target to the correct plugins directory for the Hpcc Platform.
# This is used with the the call to include cpackSetup.cmake in order to install the plugin via a Linux package.
install ( TARGETS testlib DESTINATION ${DIR_NAME}/plugins )
