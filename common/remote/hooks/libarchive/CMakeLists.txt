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


# Component: archivefile
#####################################################
# Description:
# ------------
#    Cmake Input File for archivefile
#####################################################

project( archivefile )

set (    SRCS
         archive.cpp
         archive.hpp
    )

include_directories (
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/system/jlib
    )

include_directories ( ${LIBARCHIVE_INCLUDE_DIR} )

ADD_DEFINITIONS( -D_USRDLL -DARCHIVEFILE_EXPORTS )
if ( WIN32 )
  ADD_DEFINITIONS( -DLIBARCHIVE_STATIC )
endif ( WIN32 )

HPCC_ADD_LIBRARY( archivefile SHARED ${SRCS}  )
install ( TARGETS archivefile RUNTIME DESTINATION filehooks LIBRARY DESTINATION filehooks )

target_link_libraries ( archivefile
    jlib
    ${LIBARCHIVE_LIBRARIES}
    )

if (WIN32)
target_link_libraries ( archivefile
    ${ZLIB_LIBRARIES}
    ${BZIP2_LIBRARIES}
    )
endif (WIN32)