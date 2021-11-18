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
#include <limits>
#include <queue>
#include <map>

typedef unsigned sequence_t;
#define SEQF

extern roxiemem::IDataBufferManager *bufferManager;

typedef bool (*PKT_CMP_FUN) (const void *pkData, const void *key);


// Flag bits in pktSeq field
#define UDP_PACKET_COMPLETE           0x80000000  // Packet completes a single agent request
#define UDP_PACKET_RESENT             0x40000000  // Packet is a repeat of one that the server may have missed
#define UDP_PACKET_SEQUENCE_MASK      0x3fffffff

struct UdpPacketHeader
{
    unsigned short length;      // total length of packet including the header, data, and meta
    unsigned short metalength;  // length of metadata (comes after header and data)
    ServerIdentifier  node;     // Node this message came from
    unsigned       msgSeq;      // sequence number of messages ever sent from given node, used with ruid to tell which packets are from same message
    unsigned       pktSeq;      // sequence number of this packet within the message (top bit signifies final packet)
    sequence_t     sendSeq;     // sequence number of this packet among all those send from this node to this target
    // information below is duplicated in the Roxie packet header - we could remove? However, would make aborts harder, and at least ruid is needed at receive end
    ruid_t         ruid;        // The uid allocated by the server to this agent transaction
    unsigned       msgId;       // sub-id allocated by the server to this request within the transaction
};

constexpr unsigned TRACKER_BITS=1024;      // Power of two recommended
constexpr unsigned TRACKER_DWORDS=(TRACKER_BITS+63)/64;

// Some more things we can consider:
// 1. sendSeq gives us some insight into lost packets that might help is get inflight calcuation right (if it is still needed)
// 2. If we can definitively declare that a packet is lost, we can fail that messageCollator earlier (and thus get the resend going earlier)
// 3. Worth seeing why resend doesn't use same collator. We could skip sending (though would still need to calculate) the bit we already had...

class PacketTracker
{
    // This uses a circular buffer indexed by seq to store information about which packets we have seen
private:
    sequence_t base = 0;                           // Sequence number of first packet represented in the array
    sequence_t hwm = (sequence_t) -1;              // Sequence number of highest sequence number ever seen
    unsigned __int64 seen[TRACKER_DWORDS] = {0};  // bitmask representing whether we have seen (base+n)
    void dump() const;

public:
    // Note that we have seen this packet, and return indicating whether we had already seen it
    bool noteSeen(UdpPacketHeader &hdr);
    const PacketTracker copy() const;
    inline sequence_t lastSeen() const { return hwm; }
    bool hasSeen(sequence_t seq) const;
    bool canRecord(sequence_t seq) const;
    bool hasGaps() const;
};

using roxiemem::DataBuffer;

// queue_t is used to hold a fifo queue of DataBuffer elements to be sent or collated.
// Originally implemented as a circular buffer, but we don't want to block adding even if full (we do however want to avoid requesting more if full)
// so now reimplemented as a single-linked list. There is a field in the DataBuffers that can be used for chaining them together that is used
// in a few other places, e.g. collator

class queue_t 
{
    DataBuffer *head = nullptr;      // We add at tail and remove from head
    DataBuffer *tail = nullptr;

    unsigned count = 0;
    unsigned limit = 0;
    
    CriticalSection c_region;
    InterruptableSemaphore data_avail;
    Semaphore       free_sl;              // Signalled when (a) someone is waiting for it and (b) count changes from >= limit to < limit
    unsigned        signal_free_sl = 0;   // Number of people waiting in free_sl. Only updated within critical section
    
public: 
    void interrupt();
    void pushOwn(DataBuffer *buffer);
    DataBuffer *pop(bool block);
    bool dataQueued(const void *key, PKT_CMP_FUN pkCmpFn);
    unsigned removeData(const void *key, PKT_CMP_FUN pkCmpFn);
    unsigned available();                // non-blocking
    int  free_slots();                   // block if no free slots
    void set_queue_size(unsigned limit); //must be called immediately after constructor if default constructor is used
    queue_t(unsigned int queue_size);
    queue_t() {};
    ~queue_t();
    inline int capacity() const { return limit; }
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
    sequence_t flowSeq;
    ServerIdentifier destNode;
    PacketTracker seen;
};

struct UdpRequestToSendMsg
{
    flowType::flowCmd cmd;
    unsigned short packets;
    sequence_t sendSeq;
    sequence_t flowSeq;
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
#define SOCKET_SIMULATION
#define SOCKET_SIMULATION_UDP

#ifdef SOCKET_SIMULATION
extern bool isUdpTestMode;

class CSocketSimulator : public CInterfaceOf<ISocket>
{
private:
    virtual void   read(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read,
                        unsigned timeoutsecs = WAIT_FOREVER) override { UNIMPLEMENTED; }
    virtual void   readtms(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read,
                           unsigned timeout) override { UNIMPLEMENTED; }
    virtual void   read(void* buf, size32_t size) override { UNIMPLEMENTED; }
    virtual size32_t write(void const* buf, size32_t size) override { UNIMPLEMENTED; }
    virtual size32_t writetms(void const* buf, size32_t size, unsigned timeoutms=WAIT_FOREVER) override { UNIMPLEMENTED; }

    virtual size32_t get_max_send_size() override { UNIMPLEMENTED; }
    virtual ISocket* accept(bool allowcancel=false, SocketEndpoint *peerEp = nullptr) override { UNIMPLEMENTED; }
    virtual int logPollError(unsigned revents, const char *rwstr) override { UNIMPLEMENTED; }
    virtual int wait_read(unsigned timeout) override { UNIMPLEMENTED; }
    virtual int wait_write(unsigned timeout) override { UNIMPLEMENTED; }
    virtual bool set_nonblock(bool on) override { UNIMPLEMENTED; }
    virtual bool set_nagle(bool on) override { UNIMPLEMENTED; }
    virtual void set_linger(int lingersecs) override { UNIMPLEMENTED; }
    virtual void  cancel_accept() override { UNIMPLEMENTED; }
    virtual void  shutdown(unsigned mode=SHUTDOWN_READWRITE) override { UNIMPLEMENTED; }
    virtual int name(char *name,size32_t namemax) override { UNIMPLEMENTED; }
    virtual int peer_name(char *name,size32_t namemax) override { UNIMPLEMENTED; }
    virtual SocketEndpoint &getPeerEndpoint(SocketEndpoint &ep) override { UNIMPLEMENTED; }
    virtual IpAddress &getPeerAddress(IpAddress &addr) override { UNIMPLEMENTED; }
    virtual SocketEndpoint &getEndpoint(SocketEndpoint &ep) const override { UNIMPLEMENTED; }
    virtual bool connectionless() override { UNIMPLEMENTED; }

    virtual void set_return_addr(int port,const char *name) override { UNIMPLEMENTED; }
    virtual void  set_block_mode (             // must be called before block operations
                            unsigned flags,    // BF_* flags (must match receive_block)
                          size32_t recsize=0,  // record size (required for rec compression)
                            unsigned timeoutms=0 // timeout in milisecs (0 for no timeout)
                  ) override { UNIMPLEMENTED; }



    virtual bool  send_block(
                            const void *blk,   // data to send
                            size32_t sz          // size to send (0 for eof)
                  ) override { UNIMPLEMENTED; }

    virtual size32_t receive_block_size () override { UNIMPLEMENTED; }

    virtual size32_t receive_block(
                            void *blk,         // receive pointer
                            size32_t sz          // max size to read (0 for sync eof)
                                               // if less than block size truncates block
                  ) override { UNIMPLEMENTED; }

    virtual void  close() override { UNIMPLEMENTED; }

    virtual unsigned OShandle() const override { UNIMPLEMENTED; }
    virtual size32_t avail_read() override { UNIMPLEMENTED; }

    virtual size32_t write_multiple(unsigned num,void const**buf, size32_t *size) override { UNIMPLEMENTED; }

    virtual size32_t get_send_buffer_size() override { UNIMPLEMENTED; }
    virtual void set_send_buffer_size(size32_t sz) override { UNIMPLEMENTED; }

    virtual bool join_multicast_group(SocketEndpoint &ep) override { UNIMPLEMENTED; }
    virtual bool leave_multicast_group(SocketEndpoint &ep) override { UNIMPLEMENTED; }

    virtual void set_ttl(unsigned _ttl) override { UNIMPLEMENTED; }

    virtual size32_t get_receive_buffer_size() override { UNIMPLEMENTED; }
    virtual void set_receive_buffer_size(size32_t sz) override { UNIMPLEMENTED; }

    virtual void set_keep_alive(bool set) override { UNIMPLEMENTED; }

    virtual size32_t udp_write_to(const SocketEndpoint &ep,void const* buf, size32_t size) override { UNIMPLEMENTED; }
    virtual bool check_connection() override { UNIMPLEMENTED; }

    virtual bool isSecure() const override { UNIMPLEMENTED; }
    virtual bool isValid() const override { UNIMPLEMENTED; }

};

class CSimulatedQueueReadSocket : public CSocketSimulator
{
    friend class CSimulatedQueueWriteSocket;

    CSimulatedQueueReadSocket(const SocketEndpoint &_me);
    ~CSimulatedQueueReadSocket();

    std::queue<unsigned> packetSizes;
    std::queue<const void *> packets;
    unsigned max = 131072;
    unsigned used = 0;
    SocketEndpoint me;
    CriticalSection crit;
    Semaphore avail;

    void writeSimulatedPacket(void const* buf, size32_t size);
    static std::map<SocketEndpoint, CSimulatedQueueReadSocket *> allReaders;
    static CriticalSection allReadersCrit;

public:
    static CSimulatedQueueReadSocket* udp_create(const SocketEndpoint &_me);
    static CSimulatedQueueReadSocket* connectSimulatedSocket(const SocketEndpoint &ep);

    virtual size32_t get_receive_buffer_size() override { return max; }
    virtual void set_receive_buffer_size(size32_t sz) override { max = sz; }
    virtual void read(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read,
                      unsigned timeoutsecs = WAIT_FOREVER) override;
    virtual void readtms(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read,
                         unsigned timeout) override;
    virtual int wait_read(unsigned timeout) override;
    virtual void close() override {}

};

class CSimulatedQueueWriteSocket : public CSocketSimulator
{
    CSimulatedQueueWriteSocket( const SocketEndpoint &ep) : destEp(ep) {}
    const SocketEndpoint destEp;
public:
    static CSimulatedQueueWriteSocket*  udp_connect( const SocketEndpoint &ep);
    virtual size32_t write(void const* buf, size32_t size) override;
    virtual void set_send_buffer_size(size32_t sz) override {};
    virtual void close() override {};
};


class CSimulatedUdpSocket : public CSocketSimulator
{
protected:
    Owned<ISocket> realSocket;
};
class CSimulatedUdpReadSocket : public CSimulatedUdpSocket
{
    CSimulatedUdpReadSocket(const SocketEndpoint &_me);

public:
    static CSimulatedUdpReadSocket* udp_create(const SocketEndpoint &_me);

    virtual size32_t get_receive_buffer_size() override;
    virtual void set_receive_buffer_size(size32_t sz) override;
    virtual void read(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read, unsigned timeoutsecs = WAIT_FOREVER) override;
    virtual void readtms(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read, unsigned timeout) override;
    virtual int wait_read(unsigned timeout) override;
    virtual void close() override;

};

class CSimulatedUdpWriteSocket : public CSimulatedUdpSocket
{
    CSimulatedUdpWriteSocket( const SocketEndpoint &ep);
public:
    static CSimulatedUdpWriteSocket*  udp_connect( const SocketEndpoint &ep);
    virtual size32_t write(void const* buf, size32_t size) override;
    virtual void set_send_buffer_size(size32_t sz) override;
    virtual void close() override;
};

#ifdef SOCKET_SIMULATION_UDP
using CSimulatedWriteSocket = CSimulatedUdpWriteSocket;
using CSimulatedReadSocket = CSimulatedUdpReadSocket;
#else
using CSimulatedWriteSocket = CSimulatedQueueWriteSocket;
using CSimulatedReadSocket = CSimulatedQueueReadSocket;
#endif

#endif

#endif
