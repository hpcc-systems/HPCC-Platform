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

#ifndef _DEFTAG
#define _DEFTAG
#ifndef DEFTAG
#define DEFTAG(t)   t ,
#define TAGENUM     enum mptag_t { MPTAGbase = 100,
#define TAGENUMEND  MPTAGend };
#define DEFSTDTAG(t,v) t = v, 

#endif

TAGENUM     

    DEFTAG ( MPTAG_DALI_LOCK_REQUEST )
    DEFTAG ( MPTAG_DALI_SESSION_REQUEST )
    DEFTAG ( MPTAG_DALI_COVEN_REQUEST )
    DEFTAG ( MPTAG_DALI_SUBSCRIPTION_REQUEST )
    DEFTAG ( MPTAG_DALI_SDS_REQUEST )
    DEFTAG ( MPTAG_RMT_SPAWN )
    DEFTAG ( MPTAG_JLOG_CHILD_TO_PARENT )
    DEFTAG ( MPTAG_JLOG_PARENT_TO_CHILD )
    DEFTAG ( MPTAG_JLOG_CONNECT_TO_PARENT )
    DEFTAG ( MPTAG_JLOG_CONNECT_TO_CHILD )
    DEFTAG ( MPTAG_DALI_SUBSCRIPTION_FULFILL )
    DEFTAG ( MPTAG_DALI_NAMED_QUEUE_REQUEST )
    DEFTAG ( MPTAG_FT_SLAVE )
    DEFTAG ( MPTAG_DALI_DIAGNOSTICS_REQUEST )
    DEFTAG ( MPTAG_TEST )                           // for general use when testing
    DEFTAG ( MPTAG_DKC_SLAVE )
    DEFTAG ( MPTAG_ROXIE_PACKET )
    DEFTAG ( MPTAG_PACKET_STORE_REQUEST )
    DEFTAG ( MPTAG_DFS_REQUEST )
    DEFTAG ( MPTAG_SASHA_REQUEST )
    DEFTAG ( MPTAG_FILEVIEW )
    DEFTAG ( MPTAG_KEYDIFF )
    DEFTAG ( MPTAG_DALI_AUDIT_REQUEST )
    DEFTAG ( MPTAG_THORREGISTRATION )
    DEFTAG ( MPTAG_THOR )
    DEFTAG ( MPTAG_THORRESOURCELOCK )
    DEFTAG ( MPTAG_MPTX )
    DEFTAG ( MPTAG_THORWATCHDOG )
    DEFTAG ( MPTAG_BARRIER )

    // new static tags go above here

    // Ranges
    DEFSTDTAG ( MPTAG_THORGLOBAL_BASE,      0x10000 )   // only allocated in master

    //Internal from here on
    DEFSTDTAG ( TAG_NULL,      (unsigned) -1 )
    DEFSTDTAG ( TAG_ALL,       (unsigned) -2 )
    DEFSTDTAG ( TAG_CANCEL,    (unsigned) -3 )                  // internal use
    DEFSTDTAG ( TAG_SYS_MULTI, (unsigned) -4 )
    DEFSTDTAG ( TAG_SYS_PING,  (unsigned) -5 )
    DEFSTDTAG ( TAG_SYS_PING_REPLY,  (unsigned) -6 )
    DEFSTDTAG ( TAG_SYS_BCAST, (unsigned) -7 )
    DEFSTDTAG ( TAG_SYS_FORWARD, (unsigned) -8 )
    DEFSTDTAG ( TAG_SYS_PING_REPLY_ID, (unsigned) -9 )
    DEFSTDTAG ( TAG_REPLY_BASE,(unsigned) -1000)                // internal use

TAGENUMEND
#endif

// range counts
#define MPTAG_THORPORT_COUNT        0x10000
#define MPTAG_THORGLOBAL_COUNT      0x10000



#ifndef MPTAG_HPP
#define MPTAG_HPP

#ifndef mp_decl
#define mp_decl DECL_IMPORT
#endif

#include "jstring.hpp"

// This is the system wide location for Message Passing Tags
// All MP Tags should be added here in sequence and should not be deleted or reordered
// (mark unwanted tags as legacy for (eventual) reuse)


inline MemoryBuffer &serializeMPtag(MemoryBuffer &mb,mptag_t t) { return mb.append((int)t); }
inline MemoryBuffer &deserializeMPtag(MemoryBuffer &mb,mptag_t &t) { int it; mb.read(it); t = (mptag_t)it; return mb; }


interface IMPtagAllocator: extends IInterface
{
    virtual mptag_t alloc()=0;
    virtual void release(mptag_t tag)=0;
}; 

extern mp_decl IMPtagAllocator *createMPtagRangeAllocator(mptag_t base,unsigned count);

#endif
