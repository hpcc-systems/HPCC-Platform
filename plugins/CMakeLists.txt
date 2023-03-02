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
add_subdirectory (auditlib)
add_subdirectory (debugservices)
add_subdirectory (dmetaphone)
add_subdirectory (eclblas)
add_subdirectory (fileservices)
add_subdirectory (h3)
add_subdirectory (nlp)
add_subdirectory (logging)
add_subdirectory (parselib)
add_subdirectory (stringlib)
add_subdirectory (timelib)
add_subdirectory (unicodelib)
add_subdirectory (workunitservices)
add_subdirectory (proxies)
add_subdirectory (sqlite3)
add_subdirectory (mysql)
add_subdirectory (v8embed)
HPCC_ADD_SUBDIRECTORY (py3embed "USE_PYTHON3")
HPCC_ADD_SUBDIRECTORY (pyembed "USE_PYTHON2")
add_subdirectory (javaembed)
add_subdirectory (Rembed)
add_subdirectory (cassandra)
add_subdirectory (memcached)
add_subdirectory (redis)
add_subdirectory (kafka)
add_subdirectory (exampleplugin)
add_subdirectory (couchbase)
add_subdirectory (sqs)
add_subdirectory (mongodb)
IF ( INCLUDE_EE_PLUGINS )
add_subdirectory (eeproxies)
ENDIF()
IF (USE_OPENSSL)
add_subdirectory (cryptolib)
ENDIF()
