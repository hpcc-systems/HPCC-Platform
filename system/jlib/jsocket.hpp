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



// Socket abstraction

#ifndef __JSOCKIO_H__
#define __JSOCKIO_H__

#ifndef _VER_C5
#include <time.h>
#endif

#include "jiface.hpp"
#include "jexcept.hpp"
#include "jthread.hpp"

#if 0 // spurious mplink closed tracing info
# define _TRACELINKCLOSED
#endif

#ifdef _WIN32
#define DEFAULT_LISTEN_QUEUE_SIZE    200            // maximum for windows 2000 server
#else
#define DEFAULT_LISTEN_QUEUE_SIZE    600
#endif
#define DEFAULT_LINGER_TIME          1000 // seconds
#ifndef WAIT_FOREVER
#define WAIT_FOREVER                 ((unsigned)-1)
#endif

enum JSOCKET_ERROR_CODES {
        JSOCKERR_ok                    = 0, 
        JSOCKERR_not_opened            = -1,    // accept,name,peer_name,read,write
        JSOCKERR_bad_address           = -2,    // connect
        JSOCKERR_connection_failed     = -3,    // connect
        JSOCKERR_broken_pipe           = -4,    // read,write
        JSOCKERR_invalid_access_mode   = -5,    // accept
        JSOCKERR_timeout_expired       = -6,    // read
        JSOCKERR_port_in_use           = -7,    // create
        JSOCKERR_cancel_accept         = -8,    // accept
        JSOCKERR_connectionless_socket = -9,    // accept, cancel_accept
        JSOCKERR_graceful_close        = -10,   // read,send
        JSOCKERR_handle_too_large      = -11,   // select, connect etc (linux only)
        JSOCKERR_bad_netaddr           = -12,   // get/set net address
        JSOCKERR_ipv6_not_implemented  = -13,   // various
        JSOCKERR_small_udp_packet      = -14    // small udp packet
};

// Block operation flags
#define BF_ASYNC_TRANSFER       0 // send_block sends immediately (default)
#define BF_SYNC_TRANSFER_PULL   1 // send_block waits until receiver ready (i.e. receives first)
#define BF_LZW_COMPRESS         2   // compress using LZW compression   
#define BF_REC_COMPRESS         4 // compress using record difference compression
#define BF_RELIABLE_TRANSFER    8 // retries on socket failure
#define BF_SYNC_TRANSFER_PUSH   16 // send_block pushes that has data (i.e. sends first)

// shutdown options
#define SHUTDOWN_READ       0
#define SHUTDOWN_WRITE      1
#define SHUTDOWN_READWRITE  2

#ifndef _WIN32
#define BLOCK_POLLED_SINGLE_CONNECTS  // NB this is much slower in windows
#else
#define USERECVSEM      // to singlethread BF_SYNC_TRANSFER_PUSH
#endif

//
// Abstract socket interface
//
class jlib_decl IpAddress
{
    unsigned netaddr[4] = { 0, 0, 0, 0 };
    StringAttr hostname; // not currently serialized

protected:
    StringBuffer &getHostText(StringBuffer & out, bool ip) const;
public:
    IpAddress() = default;
    explicit IpAddress(const char *text)                { ipset(text); }
    
    bool ipset(const char *text, unsigned timeoutms=INFINITE); // sets to NULL if fails or text=NULL
    void ipset(const IpAddress& other) { *this = other; }
    bool ipequals(const IpAddress & other) const;       
    int  ipcompare(const IpAddress & other) const;      // depreciated 
    unsigned iphash(unsigned prev=0) const;
    unsigned fasthash() const;
    bool isNull() const;                                // is null
    bool isHost() const;                                // is primary host NIC ip
    bool isLoopBack() const;                            // is loopback (localhost: 127.0.0.1 or ::1)
    bool isLocal() const;                               // matches local interface 
    bool isIp4() const;
    StringBuffer &getIpText(StringBuffer &out) const;
    StringBuffer &getHostText(StringBuffer & out) const;
    void ipserialize(MemoryBuffer & out) const;         
    void ipdeserialize(MemoryBuffer & in);          
    unsigned ipdistance(const IpAddress &ip,unsigned offset=0) const;       // network order distance (offset: 0-3 word (leat sig.), 0=Ipv4)
    unsigned getIP4() const;
    void setIP4(unsigned);
    bool ipincrement(unsigned count,byte minoctet=0,byte maxoctet=255,unsigned short minipv6piece=0,unsigned maxipv6piece=0xffff);
    unsigned ipsetrange( const char *text); // e.g. 10.173.72.1-65  ('-' may be omitted)
                                            // returns number in range (use ipincrement to iterate through)

    size32_t getNetAddress(size32_t maxsz,void *dst) const;     // for internal use - returns 0 if address doesn't fit
    void setNetAddress(size32_t sz,const void *src);            // for internal use
    const char * queryHostname() const { return hostname.get(); }

    inline bool operator == ( const IpAddress & other) const { return ipequals(other); }
};

struct IpComparator
{
    bool operator()(const IpAddress &a, const IpAddress &b) const
    {
        // return true if the first argument goes before the second argument, and false otherwise
        return a.ipcompare(b) < 0;
    }
};

class jlib_decl IpAddressArray : public StructArrayOf<IpAddress>
{ 
public:
    StringBuffer &getText(StringBuffer &text);
    void fromText(const char *s,unsigned defport);
};

                                         
extern jlib_decl IpAddress & queryHostIP();
extern jlib_decl IpAddress & queryLocalIP();
extern jlib_decl const char * GetCachedHostName();
inline StringBuffer & GetHostName(StringBuffer &str) { return str.append(GetCachedHostName()); }
extern jlib_decl IpAddress &GetHostIp(IpAddress &ip);
extern jlib_decl IpAddress &localHostToNIC(IpAddress &ip);  

extern jlib_decl bool queryKeepAlive(int &time, int &intvl, int &probes);
extern jlib_decl void setKeepAlive(bool enabled, int time=0, int intvl=0, int probes=0);

class jlib_decl SocketEndpoint : extends IpAddress
{
public:
    SocketEndpoint() = default;
    SocketEndpoint(const char *name,unsigned short _port=0, unsigned timeoutms=INFINITE)     { set(name,_port,timeoutms); };
    SocketEndpoint(unsigned short _port)                        { setLocalHost(_port); };
    SocketEndpoint(unsigned short _port, const IpAddress & _ip) { set(_port,_ip); };          
    SocketEndpoint(const SocketEndpoint &other) = default;

    void deserialize(MemoryBuffer & in);
    void serialize(MemoryBuffer & out) const;

    bool set(const char *name,unsigned short _port=0, unsigned timeoutms=INFINITE);
    inline void set(const SocketEndpoint & value)               { ipset(value); port = value.port; }
    inline void setLocalHost(unsigned short _port)              { port = _port; GetHostIp(*this); } // NB *not* localhost(127.0.0.1)
    inline void set(unsigned short _port, const IpAddress & _ip) { ipset(_ip); port = _port; };
    inline bool equals(const SocketEndpoint &ep) const          { return ((port==ep.port)&&ipequals(ep)); }
    StringBuffer &getEndpointIpText(StringBuffer &str) const;
    void getEndpointHostText(char * str, size32_t len) const;             // in form ip4:port or [ip6]:port
    StringBuffer &getEndpointHostText(StringBuffer &str) const;           // in form ip4:port or [ip6]:port

    inline SocketEndpoint & operator = ( const SocketEndpoint &other )
    {
        ipset(other);
        port = other.port;
        return *this;
    }
    bool operator == (const SocketEndpoint &other) const { return equals(other); }
    bool operator != (const SocketEndpoint &other) const { return !equals(other); }
    bool operator < (const SocketEndpoint &other) const { int cp =  ipcompare(other); return cp<0 || (cp==0 && (port < other.port)); }
    unsigned hash(unsigned prev) const;
    
    unsigned short port = 0;
    // Ensure that all the bytes in the data structure are initialised to avoid complains from valgrind when it is written to a socket
    unsigned short portPadding = 0;
};

// Conditionally return endpoint hostname or resolved IP (may want condition to differ in future, e.g. depending on dns configuration)
// In k8s by default pod hostnames are not resolvable from other pods, use this function when serializing the text of a host to another host
extern jlib_decl StringBuffer &getRemoteAccessibleHostText(StringBuffer &str, const SocketEndpoint &ep);

class jlib_decl SocketEndpointArray : public StructArrayOf<SocketEndpoint>
{ 
public:
    StringBuffer &getText(StringBuffer &text) const;
    bool fromName(const char *name, unsigned defport);
    void fromText(const char *s,unsigned defport);
};

interface ISocketEndpointHashTable: implements IInterface
{
    virtual void add(const SocketEndpoint &ep, IInterface *i)=0; // takes ownership
    virtual void remove(const SocketEndpoint &ep)=0;            // releases
    virtual IInterface *find(const SocketEndpoint &ep)=0;       // does not link
};

extern jlib_decl ISocketEndpointHashTable *createSocketEndpointHashTable();



class jlib_decl IpSubNet
{
    unsigned net[4];
    unsigned mask[4];
public:
    IpSubNet()                                      {set(NULL,NULL); }
    IpSubNet(const char *_net,const char *_mask)    { set(_net,_mask); }
    bool set(const char *_net,const char *_mask); // _net NULL means match everything
                                                  // _mask NULL means match exact
    bool test(const IpAddress &ip) const;
    StringBuffer &getNetText(StringBuffer &text) const;
    StringBuffer &getMaskText(StringBuffer &text) const;
    bool isNull() const;
    bool operator==(IpSubNet const &other) const
    {
        if ((0 == memcmp(net, other.net, sizeof(net))) && (0 == memcmp(mask, other.mask, sizeof(mask))))
            return true;
        return false;
    }
};

struct SocketStats
{
    cycle_t ioReadCycles = 0;
    cycle_t ioWriteCycles = 0;
    __uint64 ioReadBytes = 0;
    __uint64 ioWriteBytes = 0;
    __uint64 ioReads = 0;
    __uint64 ioWrites = 0;

    unsigned __int64 getStatistic(StatisticKind kind) const
    {
        switch (kind)
        {
        case StCycleSocketReadIOCycles:
            return ioReadCycles;
        case StCycleSocketWriteIOCycles:
            return ioWriteCycles;
        case StTimeSocketReadIO:
            return cycle_to_nanosec(ioReadCycles);
        case StTimeSocketWriteIO:
            return cycle_to_nanosec(ioWriteCycles);
        case StSizeSocketRead:
            return ioReadBytes;
        case StSizeSocketWrite:
            return ioWriteBytes;
        case StNumSocketReads:
            return ioReads;
        case StNumSocketWrites:
            return ioWrites;
        default:
            return 0;
        }
    }
};

class jlib_decl ISocket : extends IInterface
{
public:
    //
    // Create client socket connected to a TCP server socket


    static ISocket*  connect( const SocketEndpoint &ep );
    // general connect 


    static ISocket*  connect_timeout( const SocketEndpoint &ep , unsigned timeout);
    // connect where should must take longer than timeout (in ms) to connect

    
    static ISocket*  connect_wait( const SocketEndpoint &ep, unsigned timems);
    // connect where should try connecting for *at least* time specified
    // (e.g. if don't know that server listening yet)
    // if 0 specified for time then does single (blocking) connect try
    
    
    // Create client socket connected to a UDP server socket
    //
    static ISocket*  udp_connect( unsigned short port, char const* host);
    static ISocket*  udp_connect( const SocketEndpoint &ep);

    //
    // Create server TCP socket
    //
    static ISocket*  create( unsigned short port,
                                       int listen_queue_size =  DEFAULT_LISTEN_QUEUE_SIZE);

    //
    // Create server TCP socket listening a specific IP
    //
    static ISocket*  create_ip( unsigned short port,
                                          const char *host,
                                          int listen_queue_size =   DEFAULT_LISTEN_QUEUE_SIZE);

    //
    // Create server UDP socket
    //
    static ISocket*  udp_create( unsigned short port);


    // Create client socket connected to a multicast server socket
    //
    static ISocket*  multicast_connect( unsigned short port, const char *mcgroupip, unsigned _ttl);
    static ISocket*  multicast_connect( const SocketEndpoint &ep, unsigned _ttl);

    //
    // Create server multicast socket
    //
    static ISocket*  multicast_create( unsigned short port, const char *mcgroupip, unsigned _ttl);
    static ISocket*  multicast_create( unsigned short port, const IpAddress &mcgroupip, unsigned _ttl);

    //
    // Creates an ISocket for an already created socket
    //
    static ISocket*  attach(int s,bool tcpip=true);

    // suppresGCIfMinSize - if true, will suppress graceful close if size_read >= min_size
    // This is the default behavior for backwards compatibility.
    // Set to false, to allow caller to see graceful close even if size_read >= min_size
    virtual void   read(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read,
                        unsigned timeoutsecs = WAIT_FOREVER, bool suppresGCIfMinSize = true) = 0;
    virtual void   readtms(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read,
                           unsigned timeout, bool suppresGCIfMinSize = true) = 0;
    virtual void   read(void* buf, size32_t size) = 0;
    virtual size32_t write(void const* buf, size32_t size) = 0;
    virtual size32_t writetms(void const* buf, size32_t minSize, size32_t size, unsigned timeoutms=WAIT_FOREVER) = 0;

    virtual size32_t get_max_send_size() = 0;

    //
    // This method is called by server to accept client connection
    //
    virtual ISocket* accept(bool allowcancel=false) = 0; // not needed for UDP

    //
    // log poll() errors
    //
    virtual int logPollError(unsigned revents, const char *rwstr) = 0;

    //
    // This method is called to check whether a socket has data ready
    // 
    virtual int wait_read(unsigned timeout) = 0;

    //
    // This method is called to check whether a socket is ready to write (i.e. some free buffer space)
    // 
    virtual int wait_write(unsigned timeout) = 0;

    //
    // can be used with write to allow it to return if it would block
    // be sure and restore to old state before calling other functions on this socket
    //
    virtual bool set_nonblock(bool on) = 0; // returns old state

    // enable 'nagling' - small packet coalescing (implies delayed transmission)
    //
    virtual bool set_nagle(bool on) = 0; // returns old state


    // set 'linger' time - time close will linger so that outstanding unsent data will be transmitted
    //
    virtual void set_linger(int lingersecs) = 0;  


    //
    // Cancel accept operation and close socket
    //
    virtual void  cancel_accept() = 0; // not needed for UDP

    //
    // Shutdown socket: prohibit write and/or read operations on socket
    //
    virtual void  shutdown(unsigned mode=SHUTDOWN_READWRITE) = 0; // not needed for UDP

    // Same as shutdown, but never throws an exception (to call from closedown destructors)
    virtual void  shutdownNoThrow(unsigned mode=SHUTDOWN_READWRITE) = 0; // not needed for UDP

    // Get local name of accepted (or connected) socket and returns port
    virtual int name(char *name,size32_t namemax)=0;

    // Get peer name of socket and returns port - in UDP returns return addr
    virtual int peer_name(char *name,size32_t namemax)=0;

    // Get peer endpoint of socket - in UDP returns return addr
    virtual SocketEndpoint &getPeerEndpoint(SocketEndpoint &ep)=0;

    // Get peer ip of socket - in UDP returns return addr
    virtual IpAddress &getPeerAddress(IpAddress &addr)=0;

    // Get local endpoint of socket
    virtual SocketEndpoint &getEndpoint(SocketEndpoint &ep) const = 0;

    //
    // Close socket
    //
    virtual bool connectionless()=0; // true if accept need not be called (i.e. UDP)

    virtual void set_return_addr(int port,const char *name) = 0; // used for UDP servers only

    // Block functions 

    virtual void  set_block_mode (             // must be called before block operations
                            unsigned flags,    // BF_* flags (must match receive_block)
                          size32_t recsize=0,  // record size (required for rec compression)
                            unsigned timeoutms=0 // timeout in milisecs (0 for no timeout)
                  )=0; 



    virtual bool  send_block( 
                            const void *blk,   // data to send 
                            size32_t sz          // size to send (0 for eof)
                  )=0;

    virtual size32_t receive_block_size ()=0;      // get size of next block (always must call receive_block after) 

    virtual size32_t receive_block(
                            void *blk,         // receive pointer 
                            size32_t sz          // max size to read (0 for sync eof) 
                                               // if less than block size truncates block
                  )=0;

    virtual void  close() = 0;

    virtual unsigned OShandle() const = 0;              // for internal use
    virtual size32_t avail_read() = 0;           // called after wait_read to see how much data available

    virtual size32_t write_multiple(unsigned num,void const**buf, size32_t *size) = 0; // same as write except writes multiple blocks

    virtual size32_t get_send_buffer_size() =0;             // get OS send buffer
    virtual void set_send_buffer_size(size32_t sz) =0;      // set OS send buffer size

    virtual bool join_multicast_group(SocketEndpoint &ep) = 0;  // for udp multicast
    virtual bool leave_multicast_group(SocketEndpoint &ep) = 0; // for udp multicast

    virtual void set_ttl(unsigned _ttl) = 0; // set TTL

    virtual size32_t get_receive_buffer_size() = 0;             // get OS receive buffer
    virtual void set_receive_buffer_size(size32_t sz) = 0;      // set OS receive buffer size

    virtual void set_keep_alive(bool set) = 0;                  // set option SO_KEEPALIVE

    virtual size32_t udp_write_to(const SocketEndpoint &ep,void const* buf, size32_t size) = 0;
    virtual bool check_connection() = 0;

    virtual bool isSecure() const = 0;
    virtual bool isValid() const = 0;

    virtual unsigned __int64 getStatistic(StatisticKind kind) const = 0;

/*
Exceptions raised: (when set_raise_exceptions(TRUE))
    create
        sys:(socket, bind, listen)
    udp_create
        sys:(socket, bind, listen)
    accept
        JSOCKERR_not_opened, sys:(accept,setsockopt), JSOCKERR_invalid_access_mode, JSOCKERR_cancel_accept, JSOCKERR_connectionless_socket
    name
        JSOCKERR_not_opened, sys:(getsockname)
    peer_name
        JSOCKERR_not_opened, sys:(getpeername)
    cancel_accept
        {connect}, sys:(gethostname), JSOCKERR_connectionless_socket
    connect
        JSOCKERR_bad_address, JSOCKERR_connection_failed, sys:(socket, connect, setsockopt)
    udp_connect
        JSOCKERR_bad_address, sys:(socket, connect, setsockopt)
    read (timeout)
        JSOCKERR_not_opened, JSOCKERR_broken_pipe, JSOCKERR_timeout_expired ,sys:(select, read), JSOCKERR_graceful_close
    read (no timeout)
        JSOCKERR_not_opened, JSOCKERR_broken_pipe, sys:(read), JSOCKERR_graceful_close
    write
        JSOCKERR_not_opened, JSOCKERR_broken_pipe, sys:(write), JSOCKERR_graceful_close
    close
        sys:(write)
    shutdown
        sys:(shutdown),JSOCKERR_broken_pipe


*/

};

// helper function that allows a graceful close on a readtms to return with less than min_size.
// A common pattern is to read >=1 byte(s), but allow graceful close to return less (e.g. 0)
// NB: returns true if graceful close detected during read
extern jlib_decl bool readtmsAllowClose(ISocket *sock, void* buf, size32_t min_size, size32_t max_size, size32_t &sizeRead, unsigned timeoutMs);

interface jlib_thrown_decl IJSOCK_Exception: extends IException
{
};

extern jlib_decl IJSOCK_Exception *IPv6NotImplementedException(const char *filename,unsigned lineno);
#define IPV6_NOT_IMPLEMENTED() throw IPv6NotImplementedException(sanitizeSourceFile(__FILE__), __LINE__)


#ifdef USERECVSEM
class CSemProtect
{
    Semaphore *sem;
    bool *owned;
public:
    CSemProtect() { clear(); }
    ~CSemProtect()
    {
        if (sem&&*owned) {
            *owned = false;
            sem->signal();
        }
    }
    void set(Semaphore *_sem,bool *_owned)
    {
        sem = _sem;
        owned = _owned;
    }
    bool wait(Semaphore *_sem,bool *_owned,unsigned timeout) {
        if (!*_owned&&!_sem->wait(timeout))
            return false;
        *_owned = true;
        set(_sem,_owned);
        return true;
    }
    void clear() { sem = NULL; owned = NULL; }
};
#endif


//---------------------------------------------------------------------------

// These classes are useful for compressing a list of ip:ports to pass around.

class jlib_decl SocketListCreator
{
public:
    SocketListCreator();

    void addSocket(const SocketEndpoint &ep);
    void addSocket(const char * ip, unsigned port);
    const char * getText();

    void addSockets(SocketEndpointArray &array);

protected:
    StringBuffer fullText;
    StringAttr lastIp;
    unsigned lastPort;
};

class jlib_decl SocketListParser 
// This class depreciated - new code should use SocketEndpointArray::fromText and getText
{
public:
    SocketListParser(const char * text);

    void first(unsigned defport=0);
    bool get(StringAttr & ip, unsigned & port, unsigned index, unsigned defport=0);  // alternative to iterating..
    bool next(StringAttr & ip, unsigned & port);

    unsigned getSockets(SocketEndpointArray &array,unsigned defport=0);

protected:
    StringAttr fullText;
    StringAttr lastIp;
    const char * cursor;
    unsigned lastPort;
};

struct JSocketStatistics
{
    cycle_t readtimecycles;
    cycle_t writetimecycles;
    unsigned connects;          // successful
    unsigned connecttime;       // all times in microsecs
    unsigned failedconnects;
    unsigned failedconnecttime;
    unsigned reads;
    __int64  readsize;          // all sizes in bytes
    unsigned writes;
    __int64  writesize;
    unsigned activesockets;
    unsigned numblockrecvs;
    unsigned numblocksends;
    __int64  blockrecvsize; 
    __int64  blocksendsize; 
    unsigned blockrecvtime;     // not including initial handshake  
    unsigned blocksendtime; 
    unsigned longestblocksend; 
    unsigned longestblocksize; 
};

extern jlib_decl JSocketStatistics *getSocketStatPtr();
extern jlib_decl void getSocketStatistics(JSocketStatistics &stats);
extern jlib_decl void resetSocketStatistics();
extern jlib_decl StringBuffer &getSocketStatisticsString(JSocketStatistics &stats,StringBuffer &buf);

// Select Thread

#define SELECTMODE_READ      1
#define SELECTMODE_WRITE     2
#define SELECTMODE_EXCEPT    4


interface ISocketSelectNotify: extends IInterface
{
    virtual bool notifySelected(ISocket *sock,unsigned selected)=0;       // return false to continue to next selected, true to re-select
};

interface ISocketSelectHandler: extends IInterface
{
public:
    virtual void start()=0;
    virtual void add(ISocket *sock,unsigned mode,ISocketSelectNotify *nfy)=0;
    virtual void remove(ISocket *sock)=0;
    virtual void stop(bool wait)=0;
};

extern jlib_decl ISocketSelectHandler *createSocketSelectHandler(const char *trc=NULL, unsigned hdlPerThrd=0);

extern jlib_decl ISocketSelectHandler *createSocketEpollHandler(const char *trc=NULL, unsigned hdlPerThrd=0);


class MemoryBuffer;
// sends/receives length as well as contents.
extern jlib_decl void readBuffer(ISocket * socket, MemoryBuffer & buffer);
extern jlib_decl void readBuffer(ISocket * socket, MemoryBuffer & buffer, unsigned timeoutms);
extern jlib_decl void writeBuffer(ISocket * socket, MemoryBuffer & buffer);

// ditto but catches any exceptions
extern jlib_decl bool catchReadBuffer(ISocket * socket, MemoryBuffer & buffer);
extern jlib_decl bool catchReadBuffer(ISocket * socket, MemoryBuffer & buffer, unsigned timeoutms);
extern jlib_decl bool catchWriteBuffer(ISocket * socket, MemoryBuffer & buffer);


// utility interface for simple conversations 
// conversation is always between two ends, 
// at any given time one end must be receiving and other sending (though these may swap during the conversation)
interface IConversation: extends IInterface
{
    virtual bool accept(unsigned timeoutms)=0;                      // one side accepts
    virtual void set_keep_alive(bool keepalive)=0;                  // enable keepalive for socket
    virtual bool connect(unsigned timeoutms)=0;                     // other side connects
    virtual bool send(MemoryBuffer &mb)=0;                          // 0 length buffer can be sent
    virtual bool recv(MemoryBuffer &mb, unsigned timeoutms)=0;      // up to protocol to terminate conversation (e.g. by zero length buffer)
    virtual void cancel()=0;                                        // cancels above methods (from separate thread)
    virtual unsigned short setRandomPort(unsigned short base, unsigned num)=0; // sets a random unique port for accept use
};

extern jlib_decl IConversation *createSingletonSocketConnection(unsigned short port,SocketEndpoint *ep=NULL);
// the end that listens may omit ep
// this function does not connect so raises no socket exceptions 



// interface for reading from multiple sockets using the BF_SYNC_TRANSFER_PUSH protocol 
interface ISocketBufferReader: extends IInterface
{
public:
    virtual void init(unsigned num,ISocket **sockets,size32_t buffermax=(unsigned)-1)=0;
    virtual unsigned get(MemoryBuffer &mb)=0;
    virtual void done(bool wait)=0;
};

extern jlib_decl ISocketBufferReader *createSocketBufferReader(const char *trc=NULL);

interface ISocketConnectNotify
{
public:
    virtual void connected(unsigned idx,const SocketEndpoint &ep,ISocket *socket)=0; // must link socket if kept
    virtual void failed(unsigned idx,const SocketEndpoint &ep,int err)=0;
};

extern jlib_decl void multiConnect(const SocketEndpointArray &eps,ISocketConnectNotify &inotify,unsigned timeout);
extern jlib_decl void multiConnect(const SocketEndpointArray &eps,IPointerArrayOf<ISocket> &retsockets,unsigned timeout);

interface ISocketConnectWait: extends IInterface
{
public:
    virtual ISocket *wait(unsigned waittimems)=0;  // return NULL if time expired, throws exception if connect failed
    // releasing ISocketConnectWait cancels the connect iff wait has never returned socket
};

extern jlib_decl ISocketConnectWait *nonBlockingConnect(SocketEndpoint &ep,unsigned connectimeoutms=0);

// buffered socket
interface IBufferedSocket : implements IInterface
{
    virtual int readline(char* buf, int maxlen, IMultiException *me) = 0;
    virtual int read(char* buf, int maxlen) = 0;
    virtual int readline(char* buf, int maxlen, bool keepcrlf, IMultiException *me) = 0;
    virtual void setReadTimeout(unsigned int timeout) = 0;
};

#define BSOCKET_READ_TIMEOUT 600
#define BSOCKET_CLIENT_READ_TIMEOUT 7200

extern jlib_decl IBufferedSocket* createBufferedSocket(ISocket* socket);

#define MAX_NET_ADDRESS_SIZE (16)

extern jlib_decl IpSubNet &queryPreferredSubnet(); // preferred subnet when resolving multiple NICs
extern jlib_decl bool setPreferredSubnet(const char *ip,const char *mask); // also resets cached host IP

extern jlib_decl StringBuffer &lookupHostName(const IpAddress &ip,StringBuffer &ret);

extern jlib_decl bool isInterfaceIp(const IpAddress &ip, const char *ifname);
extern jlib_decl bool getInterfaceIp(IpAddress &ip, const char *ifname);
extern jlib_decl bool getInterfaceName(StringBuffer &ifname);

//Given a list of server sockets, wait until any one or more are ready to be read/written (wont block)
//return array of ready sockets
extern jlib_decl int wait_read_multiple(UnsignedArray  &socks,      //IN   sockets to be checked for read readiness
                                        unsigned timeoutMS,         //IN   timeout
                                        UnsignedArray  &readySocks);//OUT  sockets ready to be read
extern jlib_decl int wait_write_multiple(UnsignedArray  &socks,     //IN   sockets to be checked for write readiness
                                        unsigned timeoutMS,         //IN   timeout
                                        UnsignedArray  &readySocks);//OUT  sockets ready to be written

extern jlib_decl IJSOCK_Exception* createJSocketException(int jsockErr, const char *_msg, const char *file, unsigned line);
extern jlib_decl void throwJSockException(int jsockErr, const char *_msg, const char *file, unsigned line);
#define THROWJSOCKEXCEPTION(exc) throwJSockException(exc, nullptr, __FILE__, __LINE__)
#define THROWJSOCKEXCEPTION_MSG(exc, msg) throwJSockException(exc, msg, __FILE__, __LINE__)

extern jlib_decl bool isIPV4(const char *ip);
extern jlib_decl bool isIPV6(const char *ip);
extern jlib_decl bool isIPAddress(const char *ip);

interface IAllowListHandler : extends IInterface
{
    virtual bool isAllowListed(const char *ip, unsigned __int64 role, StringBuffer *responseText=nullptr) const = 0;
    virtual StringBuffer &getAllowList(StringBuffer &out) const = 0;
    virtual void refresh() = 0;
};

interface IAllowListWriter : extends IInterface
{
    virtual void add(const char *ip, unsigned __int64 role) = 0;
    virtual void setAllowAnonRoles(bool tf) = 0;
};

typedef std::function<bool(IAllowListWriter &)> AllowListPopulateFunction;
typedef std::function<StringBuffer &(StringBuffer &, unsigned __int64)> AllowListFormatFunction;
extern jlib_decl IAllowListHandler *createAllowListHandler(AllowListPopulateFunction populateFunc, AllowListFormatFunction roleFormatFunc = {}); // format function optional

// utility interface for simple conversations
// conversation is always between two ends,
// at any given time one end must be receiving and other sending (though these may swap during the conversation)
class jlib_decl CSingletonSocketConnection: implements IConversation, public CInterface
{
public:
    Owned<ISocket> sock;
    Owned<ISocket> listensock;
    enum { Snone, Saccept, Sconnect, Srecv, Ssend, Scancelled } state;
    bool cancelling = false;
    SocketEndpoint ep;
    CriticalSection crit;
    IMPLEMENT_IINTERFACE;

    CSingletonSocketConnection() {}
    CSingletonSocketConnection(SocketEndpoint &_ep);
    virtual ~CSingletonSocketConnection();
    void set_keep_alive(bool keepalive);
    virtual bool connect(unsigned timeoutms);
    bool send(MemoryBuffer &mb);
    unsigned short setRandomPort(unsigned short base, unsigned num);
    virtual bool accept(unsigned timeoutms);
    bool recv(MemoryBuffer &mb, unsigned timeoutms);
    virtual void cancel();
};

extern jlib_decl void shutdownAndCloseNoThrow(ISocket * optSocket);     // Safely shutdown and close a socket without throwing an exception.


#ifdef _WIN32
#define SOCKETERRNO() WSAGetLastError()
#else
#define SOCKETERRNO() (errno)
#endif

#endif

