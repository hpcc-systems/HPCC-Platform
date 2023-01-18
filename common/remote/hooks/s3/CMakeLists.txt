################################################################################
#    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.
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


# Component: s3file
#####################################################
# Description:
# ------------
#    Cmake Input File for amazon s3 direct reading hook
#####################################################

project( s3file )

set(CRT_BUILD_SHARED_LIBS ON)
find_package(aws-cpp-sdk-core REQUIRED)
find_package(aws-cpp-sdk-s3 REQUIRED)

set (    SRCS
         s3file.cpp
         s3file.hpp
    )

include_directories (
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/system/jlib
         aws-cpp-sdk-core
         aws-cpp-sdk-s3
    )

ADD_DEFINITIONS( -D_USRDLL -DS3FILE_EXPORTS )

HPCC_ADD_LIBRARY( s3file SHARED ${SRCS}  )
install ( TARGETS s3file DESTINATION filehooks CALC_DEPS)

target_link_libraries ( s3file
    jlib
    aws-cpp-sdk-core
    aws-cpp-sdk-s3
)
