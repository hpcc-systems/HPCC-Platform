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


# Component: gitfile
#####################################################
# Description:
# ------------
#    Cmake Input File for git repository direct reading hook
#####################################################

project( gitfile )

find_package(unofficial-git2 CONFIG REQUIRED)

set (    SRCS
         gitfile.cpp
         gitfile.hpp
    )

include_directories (
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/system/jlib
         ${HPCC_SOURCE_DIR}/common/remote/hooks/git/libgit2/include
    )

ADD_DEFINITIONS( -D_USRDLL -DGITFILE_EXPORTS )

HPCC_ADD_LIBRARY( gitfile SHARED ${SRCS}  )
install ( TARGETS gitfile DESTINATION filehooks CALC_DEPS)

target_link_libraries ( gitfile
    jlib
    unofficial::git2::git2
    )
