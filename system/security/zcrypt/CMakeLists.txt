################################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
################################################################################

# Component: zcrypt 

#####################################################
# Description:
# ------------
#    Cmake Input File for zcrypt
#####################################################


project( zcrypt ) 

set ( SRCS
      aes.cpp
      base64.cpp
      mutex.cpp
      ioapi.c
      zip.c
      unzip.c
      zcrypt.cpp
    )

IF (WIN32)
  set ( SRCS ${SRCS} iowin32.c )
ENDIF()

include_directories ( 
         ${ZLIB_INCLUDE_DIR}
         ${OPENSSL_INCLUDE_DIR}
         ./../../include 
    )

ADD_DEFINITIONS ( -D_USRDLL -DZCRYPT_EXPORTS )

HPCC_ADD_LIBRARY( zcrypt SHARED ${SRCS} )
install ( TARGETS zcrypt DESTINATION ${OSSDIR}/lib )
target_link_libraries ( zcrypt
         ${ZLIB_LIBRARIES} 
         ${OPENSSL_LIBRARIES} 
    )


