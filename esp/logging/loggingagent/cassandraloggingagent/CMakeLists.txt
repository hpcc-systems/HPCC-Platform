################################################################################
#    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems.
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

# Component: cassandralogagent

if(USE_CASSANDRA)
  project( cassandralogagent )

  include_directories (
     ${HPCC_SOURCE_DIR}/system/include
     ${HPCC_SOURCE_DIR}/system/jlib
     ${HPCC_SOURCE_DIR}/system/security/shared
     ${HPCC_SOURCE_DIR}/system/xmllib
     ${HPCC_SOURCE_DIR}/system/mp
     ${HPCC_SOURCE_DIR}/system/security/securesocket
     ${HPCC_SOURCE_DIR}/rtl/include
     ${HPCC_SOURCE_DIR}/rtl/eclrtl
     ${HPCC_SOURCE_DIR}/rtl/nbcd
     ${HPCC_SOURCE_DIR}/roxie/roxiemem
     ${HPCC_SOURCE_DIR}/common/deftype
     ${HPCC_SOURCE_DIR}/common/environment
     ${HPCC_SOURCE_DIR}/plugins/cassandra/cpp-driver/include
     ${HPCC_SOURCE_DIR}/plugins/cassandra
     ${HPCC_SOURCE_DIR}/esp/platform
     ${HPCC_SOURCE_DIR}/esp/bindings
     ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/xpp
     ${HPCC_SOURCE_DIR}/esp/logging/logginglib
     ${HPCC_SOURCE_DIR}/common/thorhelper
  )

  ADD_DEFINITIONS( -D_USRDLL -DCASSANDRALOGAGENT_EXPORTS )

  set ( SRCS
    cassandralogagent.cpp
  )

  HPCC_ADD_LIBRARY( cassandralogagent SHARED ${SRCS} )

  install ( TARGETS cassandralogagent RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
  add_dependencies (cassandralogagent espscm)
  target_link_libraries ( cassandralogagent
    cassandraembed
    eclrtl
    esphttp
    jlib
    logginglib
  )
endif(USE_CASSANDRA)
