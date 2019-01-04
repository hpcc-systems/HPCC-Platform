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

#include "udplib.hpp"
#include "udpsha.hpp"
#include "jsocket.hpp"
#include "jlog.hpp"
#include "roxie.hpp"
#include "roxiemem.hpp"

#ifdef _WIN32
#include <winsock2.h>
#else
#include <sys/socket.h>
#endif

using roxiemem::DataBuffer;
using roxiemem::IDataBufferManager;

IDataBufferManager *bufferManager;

unsigned udpTraceLevel = 0;
unsigned udpTraceCategories = (unsigned) -1;
unsigned udpFlowSocketsSize = 131072;
unsigned udpLocalWriteSocketSize = 1024000;
unsigned udpSnifferReadThreadPriority = 3;
unsigned udpSnifferSendThreadPriority = 3;

unsigned multicastTTL = 1;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    bufferManager = roxiemem::createDataBufferManager(roxiemem::DATA_ALIGNMENT_SIZE);
    return true;
}

MODULE_EXIT()
{ 
    bufferManager->Release();
}

// Maintaining a table so each node in the system has a unique index

static IpAddressArray nodeTable;

extern UDPLIB_API const IpAddress &getNodeAddress(unsigned index)
{
    return nodeTable.item(index);
}

extern UDPLIB_API unsigned addRoxieNode(const char *ipString)
{
    IpAddress ip(ipString);
    if (ip.isNull())
        throw MakeStringException(ROXIE_UDP_ERROR, "Could not resolve address %s", ipString);
    ForEachItemIn(idx, nodeTable)
    {
        if (ip.ipequals(nodeTable.item(idx)))
            return idx;
    }
    nodeTable.append(ip);
    return nodeTable.ordinality()-1;
}

extern UDPLIB_API unsigned getNumNodes()
{
    assertex(nodeTable.ordinality());
    return nodeTable.ordinality();
}

//---------------------------------------------------------------------------------------------

void queue_t::set_queue_size(unsigned int queue_s) 
{
    queue_size = queue_s;
    element_count = queue_size;
    elements = new queue_element[queue_size];
    free_space.signal(queue_size);
    active_buffers = 0;
    first = 0;
    last = 0;
}

queue_t::queue_t(unsigned int queue_s) 
{
    set_queue_size(queue_s);
    signal_free_sl = 0;
}

queue_t::queue_t() 
{
    signal_free_sl = 0;
    queue_size = 0;
    element_count = 0;
    elements = nullptr;
    active_buffers = 0;
    first = 0;
    last = 0;
}

queue_t::~queue_t() 
{
    delete [] elements; 
}

bool queue_t::empty() 
{
    c_region.enter();
    bool res = (active_buffers == 0);
    c_region.leave();
    return res;
}

int queue_t::free_slots() 
{
    int res=0;
    while (!res) 
    {
        c_region.enter();
        res = queue_size - active_buffers;
        if (!res) 
            signal_free_sl++;
        c_region.leave();
        if (!res) 
        {
            while (!free_sl.wait(3000))
            {
                if (udpTraceLevel >= 1)
                    DBGLOG("queue_t::free_slots blocked for 3 seconds waiting for free_sl semaphore");
            }
        }
    }
    return res;
}

void queue_t::interrupt()
{
    data_avail.interrupt();
}

void queue_t::pushOwn(DataBuffer *buf)
{
    while (!free_space.wait(3000))
    {
        if (udpTraceLevel >= 1)
            DBGLOG("queue_t::pushOwn blocked for 3 seconds waiting for free_space semaphore, activeBuffers == %d", active_buffers);
    }
    c_region.enter();
    int next = (last + 1) % element_count;
    elements[last].data = buf;
    last = next;
    active_buffers++;
    c_region.leave();
    data_avail.signal();
}

DataBuffer *queue_t::pop()
{
    data_avail.wait();
    DataBuffer *ret = NULL; 
    bool must_signal;
    {
        CriticalBlock b(c_region);
        if (!active_buffers) 
            return NULL;
        ret = elements[first].data;
        first = (first + 1) % element_count;
        active_buffers--;
        must_signal = signal_free_sl>0;
        if (must_signal) 
            signal_free_sl--;
    }
    free_space.signal();
    if (must_signal) 
        free_sl.signal();
    return ret;
}


bool queue_t::removeData(void *key, PKT_CMP_FUN pkCmpFn) 
{
    bool ret = false;
    CriticalBlock b(c_region);
    if (active_buffers) 
    {
        unsigned ix = first;
        for (;;)
        {
            if (elements[ix].data && 
                ((key == NULL) || (pkCmpFn == NULL) || pkCmpFn((void*) elements[ix].data, key)))
            {
                ::Release(elements[ix].data);
                elements[ix].data = NULL;  // safer than trying to remove it and close up queue - race conditions with code elsewhere
                ret = true;
            }
            ix++;
            if (ix==element_count)
                ix = 0;
            if (ix == last)
                break;
        }           
    }
    return ret;
}


bool queue_t::dataQueued(void *key, PKT_CMP_FUN pkCmpFn) 
{
    bool ret = false;
    CriticalBlock b(c_region);
    if (active_buffers) 
    {
        unsigned ix = first;
        for (;;)
        {
            if (elements[ix].data && pkCmpFn((void*) elements[ix].data, key))
            {
                ret = true;
                break;
            }
            ix++;
            if (ix==element_count)
                ix = 0;
            if (ix==last)
                break;
        }           
    }
    return ret;
}


#ifndef _WIN32
#define HOSTENT hostent
#include <netdb.h>
#endif

int check_set(const char *path, int value)
{
#ifdef __linux__
    FILE *f = fopen(path,"r");
    char res[32];
    char *r = 0;
    int si = 0;
    if (f) {
        r = fgets(res, sizeof(res), f);
        fclose(f);
    }
    if (r)
        si = atoi(r);
    if (!si)
    {
        OWARNLOG("WARNING: Failed to read value for %s", path);
        return 0;
    }
    else if (si<value)
        return -1;
#endif
    return 0;
}

int check_max_socket_read_buffer(int size) {
    return check_set("/proc/sys/net/core/rmem_max", size);
}
int check_max_socket_write_buffer(int size) {
    return check_set("/proc/sys/net/core/wmem_max", size);
}

#ifdef __linux__
void setLinuxThreadPriority(int level)
{
    pthread_t self = pthread_self();
    int policy;
    sched_param param;
    int rc;
    if (( rc = pthread_getschedparam(self, &policy, &param)) != 0) 
        DBGLOG("pthread_getschedparam error: %d", rc);
    if (level < 0)
        UNIMPLEMENTED;
    else if (!level)
    {
        param.sched_priority = 0;
        policy = SCHED_OTHER;
    }
    else
    {
        policy = SCHED_RR;
        param.sched_priority = level;
    }
    if(( rc = pthread_setschedparam(self, policy, &param)) != 0) 
        DBGLOG("pthread_setschedparam error: %d policy=%i pr=%i id=%" I64F "i TID=%i", rc, policy, param.sched_priority, (unsigned __int64) self, threadLogID());
    else
        DBGLOG("priority set id=%" I64F "i policy=%i pri=%i TID=%i", (unsigned __int64) self, policy, param.sched_priority, threadLogID());
}
#endif


extern UDPLIB_API void queryMemoryPoolStats(StringBuffer &memStats)
{
    if (bufferManager)
        bufferManager->poolStats(memStats);
}

/*
Crazy thoughts on network-wide flow control

Avoid sending data that clashes with other outbound or inbound data
    is outbound really an issue?
    if only inbound, should be easier
        can have each inbound node police its own, for a start
            udplib already tries to do this
        when sending permission to send, best to pick someone that is not sending to anyone else
            udplib already tries to do this
            but it can still lead to idleness - id node 1 sending to node 2, and node2 to node 1, node3 can't find anyone idle.


If you do need global:
  Every bit of data getting sent (perhaps over a certain size threshold?) gets permission from central traffic cop
  Outbound packet says source node, target node size
  Reply says source,target,size
  Cop allows immediately if nothing inflight between those pairs
  Cop assumes completion 
  Cop redundancy
   - a backup cop is listening in?
     - use multicast for requests and replies?
   - no reply implies what?
   - backup cop just needs heartbeat from active cop
   - permission expires
   - multiple cops for blocks of targets?
    - but I want global view of who is sending 


*/
