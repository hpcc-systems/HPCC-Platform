/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

//#define CRC_MESSAGES
extern roxiemem::IDataBufferManager *bufferManager;

typedef bool (*PKT_CMP_FUN) (void *pkData, void *key);

#define UDP_SEQUENCE_COMPLETE 0x80000000
#define UDP_SEQUENCE_MORE 0x40000000
#define UDP_SEQUENCE_BITS (UDP_SEQUENCE_COMPLETE|UDP_SEQUENCE_MORE)

struct UdpPacketHeader 
{
    unsigned short length;      // total length of packet including the header, data, and meta
    unsigned short metalength;  // length of metadata (comes after header and data)
    unsigned       nodeIndex;   // Node this message came from
    unsigned       msgSeq;      // sequence number of messages ever sent from given node, used with ruid to tell which packets are from same message
    unsigned       pktSeq;      // sequence number of this packet within the message (top bit signifies final packet)
    unsigned       udpSequence; // packet sequence from this slave to this server - used to detect lost packets and resend (independent of how formed into messages). Top bits used for flow control info
    // information below is duplicated in the Roxie packet header - we could remove? However, would make aborts harder, and at least ruid is needed at receive end
    ruid_t         ruid;        // The uid allocated by the server to this slave transaction
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
    
    int             first;
    int             last;
    CriticalSection c_region;
    int             active_buffers;
    int             queue_size;
    InterruptableSemaphore data_avail;
    Semaphore       free_space;
    Semaphore       free_sl;
    unsigned        signal_free_sl;
    int             total_data;

    void removeElement(int ix);
    
public: 
    void interrupt();
    void pushOwn(roxiemem::DataBuffer *buffer);
    roxiemem::DataBuffer *pop();
    bool empty() ;
    bool dataQueued(void *key, PKT_CMP_FUN pkCmpFn);
    bool removeData(void *key, PKT_CMP_FUN pkCmpFn);
    int  free_slots(); //block if no free slots
    void set_queue_size(unsigned int queue_size); //must be called after immitialy after contructor if default contructor is used
    queue_t(unsigned int queue_size);
    queue_t();
    ~queue_t();
};


template < class _et >
class simple_queue {
    
    _et             *elements;
    unsigned int    element_count;
    int             first;
    int             last;
    int             active_buffers;
    CriticalSection c_region;
    Semaphore       data_avail;
    Semaphore       free_space;
    
public: 
    void push(_et element) 
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
    
    bool push(_et element,long timeout) 
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

#define MAX_RESEND_TABLE_SIZE 256 

#pragma pack(push,1)
struct UdpPermitToSendMsg
{
    // New static fields must be included inside this block, so that
    // size calculations work correctly
    struct MsgHeader {
        unsigned short  length;
        unsigned short  cmd;
        unsigned short  destNodeIndex;
        unsigned short  max_data;
        unsigned        lastSequenceSeen;
        unsigned        missingCount;
#ifdef CRC_MESSAGES
        unsigned        crc;
#endif
    } hdr;
    // WARNING: Do not add any field below this line, if it must be included in the message
    unsigned        missingSequences[MAX_RESEND_TABLE_SIZE]; // only [missingCount] actually sent

    StringBuffer &toString(StringBuffer &str) const
    {
        str.appendf("lastSeen=%u missingCount=%u", hdr.lastSequenceSeen, hdr.missingCount);
        if (hdr.missingCount)
        {
            str.append(" missing=");
            for (unsigned j = 0; j < hdr.missingCount; j++)
                str.appendf(j?",%u":"%u", missingSequences[j]);
        }
        return str;
    }

#ifdef CRC_MESSAGES
    unsigned calcCRC()
    {
        size_t len = sizeof(MsgHeader) - sizeof(hdr.crc);
        unsigned expectedCRC = crc32((const char *) this, len, 0);
        if (missingCount)
            expectedCRC = crc32((const char *) &missingSequences, missingCount * sizeof(missingSequences[0]), expectedCRC); 
        return expectedCRC;
    }
#endif

    UdpPermitToSendMsg()
    {
        hdr.length = hdr.cmd = hdr.destNodeIndex = hdr.max_data = 0;
        hdr.lastSequenceSeen = 0;
        hdr.missingCount = 0;
#ifdef CRC_MESSAGES
        hdr.crc = calcCRC();
#endif
    }

    UdpPermitToSendMsg(const UdpPermitToSendMsg &from)
    {
        hdr = from.hdr;
        memcpy(missingSequences, from.missingSequences, from.hdr.missingCount * sizeof(missingSequences[0]));
    }
};

struct UdpRequestToSendMsg
{
    unsigned short  length;
    unsigned short  cmd;
    unsigned short  sourceNodeIndex;
    unsigned short  max_data; // Not filled in or used at present
    unsigned        firstSequenceAvailable;
    unsigned        lastSequenceAvailable;
};

struct sniff_msg 
{
    unsigned short length;
    unsigned short cmd;
    unsigned short nodeIndex;
};
#pragma pack(pop)

class flow_t {
public:
    enum flowmsg_t { ok_to_send, request_to_send, send_completed, request_to_send_more, busy, idle };
};

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
