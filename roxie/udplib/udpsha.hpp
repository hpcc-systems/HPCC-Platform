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

#ifndef updsha_include
#define updsha_include

#include "jmutex.hpp"
#include "roxiemem.hpp"
#include "jcrc.hpp"

extern roxiemem::IDataBufferManager *bufferManager;

typedef bool (*PKT_CMP_FUN) (const void *pkData, const void *key);


// Flag bits in pktSeq field
#define UDP_PACKET_COMPLETE           0x80000000  // Packet completes a single agent request
#define UDP_PACKET_RESERVED           0x40000000  // Not used - could move UDP_SEQUENCE_COMPLETE here?
#define UDP_PACKET_SEQUENCE_MASK      0x3fffffff

struct UdpPacketHeader 
{
    unsigned short length;      // total length of packet including the header, data, and meta
    unsigned short metalength;  // length of metadata (comes after header and data)
    ServerIdentifier  node;        // Node this message came from
    unsigned       msgSeq;      // sequence number of messages ever sent from given node, used with ruid to tell which packets are from same message
    unsigned       pktSeq;      // sequence number of this packet within the message (top bit signifies final packet)
    // information below is duplicated in the Roxie packet header - we could remove? However, would make aborts harder, and at least ruid is needed at receive end
    ruid_t         ruid;        // The uid allocated by the server to this agent transaction
    unsigned       msgId;       // sub-id allocated by the server to this request within the transaction
};

class queue_t 
{

    class queue_element 
    {
    public:
        roxiemem::DataBuffer  *data;
        queue_element() 
        {
            data = NULL;
        }
    };

    queue_element   *elements;
    unsigned int    element_count;
    
    unsigned        first;
    unsigned        last;
    CriticalSection c_region;
    int             active_buffers;
    int             queue_size;
    InterruptableSemaphore data_avail;
    Semaphore       free_space;
    Semaphore       free_sl;
    unsigned        signal_free_sl;

    void removeElement(int ix);
    
public: 
    void interrupt();
    void pushOwn(roxiemem::DataBuffer *buffer);
    roxiemem::DataBuffer *pop(bool block);
    bool dataQueued(const void *key, PKT_CMP_FUN pkCmpFn);
    unsigned removeData(const void *key, PKT_CMP_FUN pkCmpFn);
    int  free_slots(); //block if no free slots
    void set_queue_size(unsigned int queue_size); //must be called immediately after constructor if default constructor is used
    queue_t(unsigned int queue_size);
    queue_t();
    ~queue_t();
    inline int capacity() const { return queue_size; }
};


template < class _et >
class simple_queue
{
    _et             *elements;
    unsigned int    element_count;
    int             first;
    int             last;
    int             active_buffers;
    CriticalSection c_region;
    Semaphore       data_avail;
    Semaphore       free_space;
    
public: 
    void push(const _et &element)
    {
        free_space.wait();
        c_region.enter();
        int next = (last + 1) % element_count;
        elements[last] = element;
        last = next;
        active_buffers++;
        c_region.leave();
        data_avail.signal();
    }
    
    bool push(const _et &element,long timeout)
    {
        if (free_space.wait(timeout) ) {
            c_region.enter();
            int next = (last + 1) % element_count;
            elements[last] = element;
            last = next;
            active_buffers++;
            c_region.leave();
            data_avail.signal();
            return true;
        }
        return false;
    }
    
    void pop (_et &element) 
    {
        data_avail.wait();
        c_region.enter();
        element = elements[first];
        first = (first + 1) % element_count;
        active_buffers--;
        c_region.leave();
        free_space.signal();
    }
    
    unsigned in_queue() {
        c_region.enter();
        unsigned res = active_buffers;
        c_region.leave();
        return res;
    }
    
    bool empty() 
    {
        c_region.enter();
        bool res = (active_buffers == 0);
        c_region.leave();
        return res;
    }

    simple_queue(unsigned int queue_size) 
    {
        element_count = queue_size;
        elements = new _et[element_count];
        free_space.signal(element_count);
        active_buffers = 0;
        first = 0;
        last = 0;
    }
    
    ~simple_queue() {
        delete [] elements;
    }

};

#ifndef _WIN32
#define HANDLE_PRAGMA_PACK_PUSH_POP
#endif

class flowType {
public:
    enum flowCmd : unsigned short { ok_to_send, request_received, request_to_send, send_completed, request_to_send_more };
    static const char *name(flowCmd m)
    {
        switch (m)
        {
        case ok_to_send: return "ok_to_send";
        case request_received: return "request_received";
        case request_to_send: return "request_to_send";
        case send_completed: return "send_completed";
        case request_to_send_more: return "request_to_send_more";
        default:
            assert(false);
            return "??";
        }
    };

};

class sniffType {
public:
    enum sniffCmd : unsigned short { busy, idle };
};

#pragma pack(push,1)
struct UdpPermitToSendMsg
{
    flowType::flowCmd cmd;
    unsigned short max_data;
    ServerIdentifier destNode;
};

struct UdpRequestToSendMsg
{
    flowType::flowCmd cmd;
    unsigned short packets;
    ServerIdentifier sourceNode;
};

struct sniff_msg
{
    sniffType::sniffCmd cmd;
    ServerIdentifier nodeIp;
};
#pragma pack(pop)

int check_max_socket_read_buffer(int size);
int check_max_socket_write_buffer(int size);
int check_set_max_socket_read_buffer(int size);
int check_set_max_socket_write_buffer(int size);

#define TRACE_RETRY_DATA 0x08
#define TRACE_MSGPACK 0x10

inline bool checkTraceLevel(unsigned category, unsigned level)
{
    return (udpTraceLevel >= level);
}
#endif
