/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#ifndef PORTLIST_H
#define PORTLIST_H

// Main LN HPCC default port list 
// ==============================

#define SNMP_PORT                       161       // standard port for SNMP
#define SNMP_TRAP_PORT                  162    // standard port for SNMP traps

#define LDAP_PORT                       389
#define SECURE_LDAP_PORT                636 

#define PROTOCOLX_DEFAULT_PORT      2001

#define HOAGENT_PORT                    5050
#define HOLE_PORT                       5051
#define HOLE_CLIENT_PORT                5052
#define SERVER_PORT                     5055
#define WATCHDOG_PORT                   5056
#define MONITOR_PORT                    5057
#define GEvent_PORT                     5058
#define HOLE_SOCKET_PORT                5059  

#define FSPLIT_PORT                     5080
#define BSPAWN_PORT                     5081

#define COLLATE_PORT                    5100 //..5099
#define PROCESS_PORT                    5140 //..5139
#define LAST_HOLE_PORT                  5180

#define SLAVE_CONNECT_PORT              6400 //..6439    [ uses numSlaveKinds * NUM_SLAVE_CONNECT_PORT ]
#define NUM_SLAVE_CONNECT_PORT          20   // 20 for dfu, 20 for dkc

#define WUJOBQ_BASE_PORT                6700 // ..6999  // overlapped with Thor slaves
#define WUJOBQ_PORT_NUM                 300

#define DALI_SERVER_PORT                7070 
#define DATA_TRANSFER_PORT              7080
#define DAFILESRV_PORT                  7100 // aka daliservix
#define MP_START_PORT                   7101 // Default range for MP ports
#define MP_END_PORT                     7500

#define SECURE_DAFILESRV_PORT           7600 // aka daliservix

//ESP SERVICES
//INSECURE
#define WS_ECL_DEFAULT_PORT             8002
#define ECL_DIRECT_DEFAULT_PORT         8008
#define ECL_WATCH_DEFAULT_PORT          8010
#define WS_ACCOUNT_DEFAULT_PORT         8010
#define WS_SSN_DEFAULT_PORT             8020
#define WS_THING_FINDER_DEFAULT_PORT    8030
#define WS_SNA_DEFAULT_PORT             8045
#define WS_AUTO_UPDATE_DEFAULT_PORT     8045
#define WS_FCIC_QUERY_DEFAULT_PORT      8046
#define WS_FCIC_REPORT_DEFAULT_PORT     8047
#define WS_ROXIE_CONFIG_DEFAULT_PORT    8050
#define WS_RISKVIEW_DEFAULT_PORT       8055
#define WS_FCIC_ECLQUERY_DEFAULT_PORT   8065
#define WS_ACCURINT_AUTH_DEFAULT_PORT   8066
#define WS_ZIP_RESOLVER_DEFAULT_PORT    8075
#define WS_DATA_ACCESS_DEFAULT_PORT     8080
#define WS_JABBER_DEFAULT_PORT          8080
#define WS_REF_TRACKER_DEFAULT_PORT     8080
#define WS_ATTRIBUTES_DEFAULT_PORT      8145
#define WS_LOGGING_DEFAULT_PORT         8146
#define WS_ITS_DEFAULT_PORT             8888
#define WS_FACTS_DEFAULT_PORT           8999
#define WS_MOXIE_DEFAULT_PORT           8999
#define WS_DISTRIX_DEFAULT_PORT         8999

#define DEFAULT_SASHA_PORT              8877

#define CCD_MULTICAST_PORT              8887
#define CCD_SERVER_FLOW_PORT            9000
#define CCD_DATA_PORT                   9001
#define CCD_CLIENT_FLOW_PORT            9002
#define CCD_SNIFFER_PORT                9003

#define ROXIE_SERVER_PORT               9876

#define THOR_DEBUG_PORT                 16000
#define HTHOR_DEBUG_BASE_PORT           17000
#define HTHOR_DEBUG_PORT_RANGE          10

//ESP SERVICES
//SECURE
#define WS_ECL_SECURE_PORT              18002
#define ECL_DIRECT_SECURE_PORT          18008
#define ECL_WATCH_SECURE_PORT           18010
#define WS_ACCOUNT_SECURE_PORT          18010
#define WS_SSN_SECURE_PORT              18020
#define WS_THING_FINDER_SECURE_PORT     18030
#define WS_SNA_SECURE_PORT              18045
#define WS_AUTO_UPDATE_SECURE_PORT      18045
#define WS_FCIC_QUERY_SECURE_PORT       18046
#define WS_FCIC_REPORT_SECURE_PORT      18047
#define WS_FCIC_ECLQUERY_SECURE_PORT    18065
#define WS_ACCURINT_AUTH_SECURE_PORT    18066
#define WS_ZIP_RESOLVER_SECURE_PORT     18075
#define WS_DATA_ACCESS_SECURE_PORT      18080
#define WS_JABBER_SECURE_PORT           18080
#define WS_REF_TRACKER_SECURE_PORT      18080
#define WS_ATTRIBUTES_SECURE_PORT       18145
#define WS_IAS_SECURE_PORT              18299
#define WS_ITS_SECURE_PORT              18888
#define WS_FACTS_SECURE_PORT            18999
#define WS_MOXIE_SECURE_PORT            18999
#define WS_DISTRIX_SECURE_PORT          18999

#define THOR_BASE_PORT                  20000 //..~20099
#define THOR_BASESLAVE_PORT             20100 //..~20199

/* ESP PLUG-IN SERVICES */
//Insecure
#define WS_SQL_DEFAULT_PORT             8510
//Secure
#define WS_SQL_SECURE_PORT              18510
#endif
