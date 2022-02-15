# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# - Find Apache Portable Runtime
# Find the APR includes and libraries
# This module defines
#  APRUTIL_INCLUDE_DIR, where to find apr.h, etc.
#  APRUTIL_LIBRARIES, the libraries needed to use APR.
#  APRUTIL_FOUND, If false, do not try to use APR.
# also defined, but not for general use are
#  APRUTIL_LIBRARY, where to find the APR library.

FIND_PATH(APRUTIL_INCLUDE_DIR apu.h
   PATH_SUFFIXES apr-1 apr-1.0
)

SET(APRUTIL_NAMES ${APRUTIL_NAMES} libaprutil-1 aprutil-1)
FIND_LIBRARY(APRUTIL_LIBRARY
  NAMES ${APRUTIL_NAMES}
)

find_package_handle_standard_args(APRUTIL DEFAULT_MSG
   APRUTIL_LIBRARY APRUTIL_INCLUDE_DIR)

MARK_AS_ADVANCED(APRUTIL_INCLUDE_DIR APRUTIL_LIBRARY)
