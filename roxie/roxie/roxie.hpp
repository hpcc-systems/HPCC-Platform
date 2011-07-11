/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#ifndef _ROXIE_INCL
#define _ROXIE_INCL

#include "errorlist.h"

// roxie error codes
#define ROXIE_INVALID_ACTION        ROXIE_ERROR_START
#define ROXIE_INVALID_TOPOLOGY      ROXIE_ERROR_START+1
#define ROXIE_QUERY_SUSPENDED       ROXIE_ERROR_START+2
#define ROXIE_UNKNOWN_QUERY         ROXIE_ERROR_START+3
#define ROXIE_XML_ERROR             ROXIE_ERROR_START+4
#define ROXIE_INVALID_FLAGS         ROXIE_ERROR_START+5
#define ROXIE_MULTICAST_ERROR       ROXIE_ERROR_START+6
#define ROXIE_TIMEOUT               ROXIE_ERROR_START+7
#define ROXIE_TOO_MANY_RESULTS      ROXIE_ERROR_START+8
#define ROXIE_DATA_ERROR            ROXIE_ERROR_START+9
#define ROXIE_FILE_FAIL             ROXIE_ERROR_START+10
#define ROXIE_MISMATCH              ROXIE_ERROR_START+11
#define ROXIE_MEMORY_ERROR          ROXIE_ERROR_START+12
#define ROXIE_INTERNAL_ERROR        ROXIE_ERROR_START+13
#define ROXIE_PIPE_ERROR            ROXIE_ERROR_START+14
#define ROXIE_LOCK_ERROR            ROXIE_ERROR_START+15
#define ROXIE_SNMP_ERROR            ROXIE_ERROR_START+16
#define ROXIE_UNIMPLEMENTED_ERROR   ROXIE_ERROR_START+17
#define ROXIE_ACCESS_ERROR          ROXIE_ERROR_START+18
#define ROXIE_TOO_MANY_QUERIES      ROXIE_ERROR_START+19
#define ROXIE_FILE_OPEN_FAIL        ROXIE_ERROR_START+20
#define ROXIE_TOO_MANY_EMPTY_LOOP   ROXIE_ERROR_START+21
#define ROXIE_TOO_MANY_GRAPH_LOOP   ROXIE_ERROR_START+22
#define ROXIE_NOT_SORTED            ROXIE_ERROR_START+23
#define ROXIE_DALI_ERROR            ROXIE_ERROR_START+24
#define ROXIE_UNKNOWN_PACKAGE       ROXIE_ERROR_START+25
#define ROXIE_UNKNOWN_ALGORITHM     ROXIE_ERROR_START+26
#define ROXIE_PACKAGE_ERROR         ROXIE_ERROR_START+27
#define ROXIE_PACKAGES_ACTIVE       ROXIE_ERROR_START+28
#define ROXIE_DISKSPACE_ERROR       ROXIE_ERROR_START+29
#define ROXIE_RECORD_FETCH_ERROR    ROXIE_ERROR_START+30
#define ROXIE_MISMATCH_GROUP_ERROR  ROXIE_ERROR_START+31
#define ROXIE_MISSING_GROUP_ERROR   ROXIE_ERROR_START+32
#define ROXIE_NWAY_INPUT_ERROR      ROXIE_ERROR_START+33
#define ROXIE_JOIN_ERROR            ROXIE_ERROR_START+34
#define ROXIE_TOPN_ROW_ERROR        ROXIE_ERROR_START+35
#define ROXIE_GRAPH_PROCESSING_ERROR  ROXIE_ERROR_START+36
#define ROXIE_FORCE_SHUTDOWN        ROXIE_ERROR_START+37
#define ROXIE_CODEGEN_ERROR         ROXIE_ERROR_START+38
#define ROXIE_ACL_ERROR             ROXIE_ERROR_START+39
#define ROXIE_ADDDEPENDENCY_ERROR   ROXIE_ERROR_START+40
#define ROXIE_RCD_LAYOUT_TRANSLATOR ROXIE_ERROR_START+41
#define ROXIE_INVALID_INPUT         ROXIE_ERROR_START+42
#define ROXIE_QUERY_MODIFICATION    ROXIE_ERROR_START+43
#define ROXIE_CONTROL_MSG_ERROR     ROXIE_ERROR_START+44
#define ROXIE_ON_DEMAND_QUERIES     ROXIE_ERROR_START+45
#define ROXIE_PACKET_ERROR          ROXIE_ERROR_START+46
#define ROXIE_SET_INPUT             ROXIE_ERROR_START+47
#define ROXIE_SINK                  ROXIE_ERROR_START+48
#define ROXIE_MISSING_PARAMS        ROXIE_ERROR_START+49
#define ROXIE_MEMORY_LIMIT_EXCEEDED     ROXIE_ERROR_START+50
#define ROXIE_MEMORY_POOL_EXHAUSTED     ROXIE_ERROR_START+51
#define ROXIE_INVALID_MEMORY_ALIGNMENT  ROXIE_ERROR_START+52
#define ROXIE_HEAP_ERROR                ROXIE_ERROR_START+53
#define ROXIE_CALLBACK_ERROR        ROXIE_ERROR_START+54
#define ROXIE_UDP_ERROR             ROXIE_ERROR_START+55
#define ROXIE_LIBRARY_ERROR         ROXIE_ERROR_START+56
#define ROXIE_CLUSTER_SYNC_ERROR    ROXIE_ERROR_START+57  // needed when syncCluster is set to false
#define ROXIE_DEBUG_ERROR           ROXIE_ERROR_START+58
#define ROXIE_CHANNEL_SUSPENDED     ROXIE_ERROR_START+59
#define ROXIE_UNKNOWN_SERVER        ROXIE_ERROR_START+60
#define ROXIE_CLIENT_CLOSED         ROXIE_ERROR_START+61
#define ROXIE_ABORT_ERROR           ROXIE_ERROR_START+62
#define ROXIE_OPT_REPORTING         ROXIE_ERROR_START+63




// MORE: move back to ccd.hpp when no longer public (used by roxieconfig)
#define ROXIE_SLA_LOGIC

#define ROXIE_SLA_PRIORITY 0x40000000    // mask in activityId indicating it goes SLA priority queue
#define ROXIE_HIGH_PRIORITY 0x80000000   // mask in activityId indicating it goes on the fast queue
#define ROXIE_LOW_PRIORITY 0x00000000    // mask in activityId indicating it goes on the slow queue (= default)
#ifdef ROXIE_SLA_LOGIC
#define ROXIE_PRIORITY_MASK (ROXIE_SLA_PRIORITY | ROXIE_HIGH_PRIORITY | ROXIE_LOW_PRIORITY)
#else
#define (ROXIE_PRIORITY_MASK ROXIE_HIGH_PRIORITY | ROXIE_LOW_PRIORITY )
#endif  


#endif
