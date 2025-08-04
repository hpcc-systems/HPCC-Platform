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


// New IPv6 Version - IN PROGRESS

/*
    TBD IPv6 connect 
    multicast
    look at loopback
*/


/* ISocket implementation/semantic details:
 *
 * 1. All sockets are created in non-blocking state. All implementations should cope with non-blocking, i.e. wait/retry.
 * 2. read/readtms will block until min_size bytes are read or timeout expires (see readtms for more detail).
 * read/readtms can be called with min_size=0, in which case it will return as soon as no more data is available.
 * 3. Historically, SSL calls + non-SSL write/writetms, used to flip blocking state during the call, this was prone to error,
 * because socket could [legitimately] be written to whilst being read from, e.g. a server could be writing to a client, and
 * at the same time, be notified of traffic and start reading it. Flipping the blocking state of a socket is inherently unsafe
 * in many usage patterns.
 *
 * The non-blocking semantics are designed so that the select handlers can read as much data as possible without blocking.
 * e.g. dafilesrv and MP avoid blocking in their select handlers, thus avoiding up other client traffic.
 */



#include <string>
#include <unordered_set>
#include <functional>

#include "platform.h"
#ifdef _VER_C5
#include <clwclib.h>
#else
#include "platform.h"
#include <stdio.h>
#endif
#include <algorithm>
#include <thread>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <signal.h>  
#else
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stddef.h>
#include <errno.h>
#include <net/if.h>
#include <poll.h>
#endif
#include <limits.h>

#include "jmutex.hpp"
#include "jsocket.hpp"
#include "jexcept.hpp"
#include "jio.hpp"
#include "jmisc.hpp"
#include "jthread.hpp"
#include "jqueue.tpp"
#include "jtime.hpp"
#include "jprop.hpp"
#include "jregexp.hpp"
#include "jdebug.hpp"

// epoll only with linux

#ifndef __linux__
# undef _HAS_EPOLL_SUPPORT
#else
# define _HAS_EPOLL_SUPPORT
# ifdef _HAS_EPOLL_SUPPORT
#  include <unistd.h>
#  include <sys/epoll.h>
#  ifndef EPOLLRDHUP
//  Centos 5.x bug - epoll.h does not define but its in the kernel
#   define EPOLLRDHUP 0x2000
#  endif
#  define MAX_RET_EVENTS  5000 // max events returned from epoll_wait() call
   static unsigned epoll_hdlPerThrd = UINT_MAX;
# endif
#endif

// OSX seems to use TCP_KEEPALIVE instead of TCP_KEEPIDLE
#if defined(__APPLE__) && !defined(TCP_KEEPIDLE)
# define TCP_KEEPIDLE TCP_KEEPALIVE
#endif

// various options 

#define CONNECT_TIMEOUT_REFUSED_WAIT    1000        // maximum to sleep on connect_timeout
#define TRACE_SLOW_BLOCK_TRANSFER   

#define DEFAULT_CONNECT_TIME    (100*1000)      // for connect_wait

#ifdef _DEBUG
//  #define SIMULATE_LOST_UDP_PACKETS
#endif


#ifdef SIMULATE_LOST_UDP_PACKETS
  static const int dropThreshold = 10000;
  static int dropCounter = 0;
#endif

#ifdef _DEBUG
//#define SOCKTRACE
//#define EPOLLTRACE
#endif

// comment out this definition to suppress all low-level jsocket error logging
#define _TRACE

#ifdef _TRACE
#define LOGERR(err,ref,info) LogErr(err,ref,info,__LINE__,NULL)
#define LOGERR2(err,ref,info) LogErr(err,ref,info,__LINE__,tracename)
#else
#define LOGERR(err,ref,info)
#define LOGERR2(err,ref,info)
#endif

JSocketStatistics STATS;
static const bool IP4only=false;        // slighly faster if we know no IPv6
static bool IP6preferred=false;         // e.g. for DNS and socket create

IpSubNet PreferredSubnet(NULL,NULL);    // set this if you prefer a particular subnet for debugging etc
                                        // e.g. PreferredSubnet("192.168.16.0", "255.255.255.0")

static RelaxedAtomic<unsigned> pre_conn_unreach_cnt{0};    // global count of pre_connect() JSE_NETUNREACH error

#define IPV6_SERIALIZE_PREFIX (0x00ff00ff)


class jlib_thrown_decl SocketException: public IJSOCK_Exception, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;

    SocketException(int code,const char *_msg=NULL) : errcode(code) 
    {
        if (_msg) 
            msg = strdup(_msg);
        else
            msg = NULL;
        queryExceptionIntercept().handle(ExceptionInterceptClass::eSocket, this);
    };
    ~SocketException() { free(msg); }
    
    int             errorCode() const { return errcode; }
    static StringBuffer & geterrormessage(int err,StringBuffer &str)
    {
        switch (err) {
        case JSOCKERR_ok:                        return str.append("ok");
        case JSOCKERR_not_opened:                return str.append("socket not opened");
        case JSOCKERR_bad_address:               return str.append("bad address");
        case JSOCKERR_connection_failed:         return str.append("connection failed");
        case JSOCKERR_broken_pipe:               return str.append("connection is broken");
        case JSOCKERR_graceful_close:            return str.append("connection closed other end");
        case JSOCKERR_invalid_access_mode:       return str.append("invalid access mode");
        case JSOCKERR_timeout_expired:           return str.append("timeout expired");
        case JSOCKERR_port_in_use:               return str.append("port in use");
        case JSOCKERR_cancel_accept:             return str.append("cancel accept");
        case JSOCKERR_connectionless_socket:     return str.append("connectionless socket");
        case JSOCKERR_handle_too_large:          return str.append("handle too large");
        case JSOCKERR_bad_netaddr:               return str.append("bad net addr");
        case JSOCKERR_ipv6_not_implemented:      return str.append("IPv6 not implemented");
        case JSOCKERR_small_udp_packet:          return str.append("small UDP packet");
        // OS errors
#ifdef _WIN32
        case WSAEINTR:              return str.append("WSAEINTR(10004) - Interrupted system call.");
        case WSAEBADF:              return str.append("WSAEBADF(10009) - Bad file number.");
        case WSAEACCES:             return str.append("WSAEACCES(10013) - Permission denied.");
        case WSAEFAULT:             return str.append("WSAEFAULT(10014) - Bad address.");
        case WSAEINVAL:             return str.append("WSAEINVAL(10022) - Invalid argument.");
        case WSAEMFILE:             return str.append("WSAEMFILE(10024) - Too many open files.");
        case WSAEWOULDBLOCK:        return str.append("WSAEWOULDBLOCK(10035) - Operation would block.");
        case WSAEINPROGRESS:        return str.append("WSAEINPROGRESS(10036) - Operation now in progress.");
        case WSAEALREADY:           return str.append("WSAEALREADY(10037) - Operation already in progress.");
        case WSAENOTSOCK:           return str.append("WSAENOTSOCK(10038) - Socket operation on nonsocket.");
        case WSAEDESTADDRREQ:       return str.append("WSAEDESTADDRREQ(10039) - Destination address required.");
        case WSAEMSGSIZE:           return str.append("WSAEMSGSIZE(10040) - Message too long.");
        case WSAEPROTOTYPE:         return str.append("WSAEPROTOTYPE(10041) - Protocol wrong type for socket.");
        case WSAENOPROTOOPT:        return str.append("WSAENOPROTOOPT(10042) - Protocol not available.");
        case WSAEPROTONOSUPPORT:    return str.append("WSAEPROTONOSUPPORT(10043) - Protocol not supported.");
        case WSAESOCKTNOSUPPORT:    return str.append("WSAESOCKTNOSUPPORT(10044) - Socket type not supported.");
        case WSAEOPNOTSUPP:         return str.append("WSAEOPNOTSUPP(10045) - Operation not supported on socket.");
        case WSAEPFNOSUPPORT:       return str.append("WSAEPFNOSUPPORT(10046) - Protocol family not supported.");
        case WSAEAFNOSUPPORT:       return str.append("WSAEAFNOSUPPORT(10047) - Address family not supported by protocol family.");
        case WSAEADDRINUSE:         return str.append("WSAEADDRINUSE(10048) - Address already in use.");
        case WSAEADDRNOTAVAIL:      return str.append("WSAEADDRNOTAVAIL(10049) - Cannot assign requested address.");
        case WSAENETDOWN:           return str.append("WSAENETDOWN(10050) - Network is down.");
        case WSAENETUNREACH:        return str.append("WSAENETUNREACH(10051) - Network is unreachable.");
        case WSAENETRESET:          return str.append("WSAENETRESET(10052) - Network dropped connection on reset.");
        case WSAECONNABORTED:       return str.append("WSAECONNABORTED(10053) - Software caused connection abort.");
        case WSAECONNRESET:         return str.append("WSAECONNRESET(10054) - Connection reset by peer.");
        case WSAENOBUFS:            return str.append("WSAENOBUFS(10055) - No buffer space available.");
        case WSAEISCONN:            return str.append("WSAEISCONN(10056) - Socket is already connected.");
        case WSAENOTCONN:           return str.append("WSAENOTCONN(10057) - Socket is not connected.");
        case WSAESHUTDOWN:          return str.append("WSAESHUTDOWN(10058) - Cannot send after socket shutdown.");
        case WSAETOOMANYREFS:       return str.append("WSAETOOMANYREFS(10059) - Too many references: cannot splice.");
        case WSAETIMEDOUT:          return str.append("WSAETIMEDOUT(10060) - Connection timed out.");
        case WSAECONNREFUSED:       return str.append("WSAECONNREFUSED(10061) - Connection refused.");
        case WSAELOOP:              return str.append("WSAELOOP(10062) - Too many levels of symbolic links.");
        case WSAENAMETOOLONG:       return str.append("WSAENAMETOOLONG(10063) - File name too long.");
        case WSAEHOSTDOWN:          return str.append("WSAEHOSTDOWN(10064) - Host is down.");
        case WSAEHOSTUNREACH:       return str.append("WSAEHOSTUNREACH(10065) - No route to host.");
        case WSASYSNOTREADY:        return str.append("WSASYSNOTREADY(10091) - The network subsystem is unusable.");
        case WSAVERNOTSUPPORTED:    return str.append("WSAVERNOTSUPPORTED(10092) - The Windows Sockets DLL cannot support this application.");
        case WSANOTINITIALISED:     return str.append("WSANOTINITIALISED(10093) - Winsock not initialized.");
        case WSAEDISCON:            return str.append("WSAEDISCON(10101) - Disconnect.");
        case WSAHOST_NOT_FOUND:     return str.append("WSAHOST_NOT_FOUND(11001) - Host not found.");
        case WSATRY_AGAIN:          return str.append("WSATRY_AGAIN(11002) - Nonauthoritative host not found.");
        case WSANO_RECOVERY:        return str.append("WSANO_RECOVERY(11003) - Nonrecoverable error.");
        case WSANO_DATA:            return str.append("WSANO_DATA(11004) - Valid name, no data record of requested type.");
#else
        case ENOTSOCK:              return str.append("ENOTSOCK - Socket operation on non-socket ");
        case EDESTADDRREQ:          return str.append("EDESTADDRREQ - Destination address required ");
        case EMSGSIZE:              return str.append("EMSGSIZE - Message too long ");
        case EPROTOTYPE:            return str.append("EPROTOTYPE - Protocol wrong type for socket ");
        case ENOPROTOOPT:           return str.append("ENOPROTOOPT - Protocol not available ");
        case EPROTONOSUPPORT:       return str.append("EPROTONOSUPPORT - Protocol not supported ");
        case ESOCKTNOSUPPORT:       return str.append("ESOCKTNOSUPPORT - Socket type not supported ");
        case EOPNOTSUPP:            return str.append("EOPNOTSUPP - Operation not supported on socket ");
        case EPFNOSUPPORT:          return str.append("EPFNOSUPPORT - Protocol family not supported ");
        case EAFNOSUPPORT:          return str.append("EAFNOSUPPORT - Address family not supported by protocol family ");
        case EADDRINUSE:            return str.append("EADDRINUSE - Address already in use ");
        case EADDRNOTAVAIL:         return str.append("EADDRNOTAVAIL - Can't assign requested address ");
        case ENETDOWN:              return str.append("ENETDOWN - Network is down ");
        case ENETUNREACH:           return str.append("ENETUNREACH - Network is unreachable ");
        case ENETRESET:             return str.append("ENETRESET - Network dropped connection because of reset ");
        case ECONNABORTED:          return str.append("ECONNABORTED - Software caused connection abort ");
        case ECONNRESET:            return str.append("ECONNRESET - Connection reset by peer ");
        case ENOBUFS:               return str.append("ENOBUFS - No buffer space available ");
        case EISCONN:               return str.append("EISCONN - Socket is already connected ");
        case ENOTCONN:              return str.append("ENOTCONN - Socket is not connected ");
        case ESHUTDOWN:             return str.append("ESHUTDOWN - Can't send after socket shutdown ");
        case ETOOMANYREFS:          return str.append("ETOOMANYREFS - Too many references: can't splice ");
        case ETIMEDOUT:             return str.append("ETIMEDOUT - Connection timed out ");
        case ECONNREFUSED:          return str.append("ECONNREFUSED - Connection refused ");
        case EHOSTDOWN:             return str.append("EHOSTDOWN - Host is down ");
        case EHOSTUNREACH:          return str.append("EHOSTUNREACH - No route to host ");
        case EWOULDBLOCK:           return str.append("EWOULDBLOCK - operation already in progress");
        case EINPROGRESS:           return str.append("EINPROGRESS - operation now in progress ");
#endif
        }
        IException *ose = makeOsException(err);
        ose->errorMessage(str);
        ose->Release();
        return str;
    }   
    StringBuffer &  errorMessage(StringBuffer &str) const
    { 
        if (msg)
            return geterrormessage(errcode,str).append('\n').append(msg); 
        return geterrormessage(errcode,str); 
    }
    MessageAudience errorAudience() const 
    { 
        switch (errcode) {
        case JSOCKERR_port_in_use:               
            return MSGAUD_operator; 
        }
        return MSGAUD_user; 
    }
private:
    int     errcode;
    char *msg;
};

IJSOCK_Exception* createJSocketException(int jsockErr, const char *_msg, const char *file, unsigned line)
{
    StringBuffer msg;
    if (_msg)
        msg.append(_msg).append(", ");
    msg.appendf("Raised in: %s, line %d", sanitizeSourceFile(file), line);
    return new SocketException(jsockErr, _msg);

}

void throwJSockException(int jsockErr, const char *_msg, const char *file, unsigned line)
{
    throw createJSocketException(jsockErr, _msg, file, line);
}

inline void throwJSockTargetException(int jsockErr, const char *target, const char *file, unsigned line)
{
    VStringBuffer msg("Target: %s", target);
    throw createJSocketException(jsockErr, msg, file, line);
}

#define THROWJSOCKTARGETEXCEPTION(exc) throwJSockTargetException(exc, tracename, __FILE__, __LINE__)



IJSOCK_Exception *IPv6NotImplementedException(const char *filename,unsigned lineno)
{
    StringBuffer msg;
    msg.appendf("%s(%d)",filename,lineno);
    return new SocketException(JSOCKERR_ipv6_not_implemented,msg.str());
}

struct MCASTREQ
{
   struct in_addr imr_multiaddr;   /* multicast group to join */
   struct in_addr imr_interface;   /* interface to join on    */
   MCASTREQ(const char *mcip)
   {
        imr_multiaddr.s_addr = inet_addr(mcip);
        imr_interface.s_addr = htonl(INADDR_ANY);

   }

};

#ifdef __APPLE__
#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0x4000
#endif
#endif

#if defined( _WIN32)
#define T_SOCKET SOCKET
#define T_FD_SET fd_set
#define XFD_SETSIZE FD_SETSIZE
//Following are defined in more modern headers
#define XFD_ZERO(s) FD_ZERO(s)
#define SEND_FLAGS 0
#define CHECKSOCKRANGE(s)
#define _USE_SELECT // Windows bug 309411 - WSAPoll does not report failed connections - wont fix
// #define poll(a, b, c) WSAPoll((a), (b), (c))
#elif defined(__FreeBSD__) || defined(__APPLE__) || defined(EMSCRIPTEN)
#define XFD_SETSIZE FD_SETSIZE
#define T_FD_SET fd_set
#define XFD_ZERO(s) FD_ZERO(s)
#define T_SOCKET int
#define SEND_FLAGS (MSG_NOSIGNAL)
#define CHECKSOCKRANGE(s)
#else
#define XFD_SETSIZE 32768
struct xfd_set { __fd_mask fds_bits[XFD_SETSIZE / __NFDBITS]; }; // define our own
// linux 64 bit
#ifdef __linux__
#ifdef __64BIT__
#undef __FDMASK
#define __FDMASK(d)     (1UL << ((d) % __NFDBITS))
#undef __FDELT
#define __FDELT(d)      ((d) / __NFDBITS)
#undef __FD_SET
#define __FD_SET(d, s)     (__FDS_BITS (s)[__FDELT(d)] |= __FDMASK(d))
#undef __FD_ISSET
#define __FD_ISSET(d, s)   ((__FDS_BITS (s)[__FDELT(d)] & __FDMASK(d)) != 0)
#endif
#define CHECKSOCKRANGE(s) { if (s>=XFD_SETSIZE) THROWJSOCKEXCEPTION(JSOCKERR_handle_too_large); }
#endif
// end 64 bit
#define T_FD_SET xfd_set
#define XFD_ZERO(s) memset(s,0,sizeof(xfd_set))
#define T_SOCKET int
#define SEND_FLAGS (MSG_NOSIGNAL)
#endif
enum SOCKETMODE { sm_tcp_server, sm_tcp, sm_udp_server, sm_udp, sm_multicast_server, sm_multicast};

#define BADSOCKERR(err) ((err==JSE_BADF)||(err==JSE_NOTSOCK))

#ifdef POLLRDHUP
# define POLLINX (POLLIN | POLLRDHUP)
#else
# define POLLINX POLLIN
#endif

#ifdef _HAS_EPOLL_SUPPORT
# ifdef EPOLLRDHUP
#  define EPOLLINX (EPOLLIN | EPOLLRDHUP)
# else
#  define EPOLLINX EPOLLIN
# endif
#endif

enum UseUDE { UNINIT, INITED };
static std::atomic<UseUDE> expertTCPSettings { UNINIT };
static CriticalSection queryTCPCS;

static bool hasKeepAlive = false;
static int keepAliveTime = -1;
static int keepAliveInterval = -1;
static int keepAliveProbes = -1;
static bool disableDNSTimeout = false;
static int maxDNSThreads = 50;

/*
<Software>
  <Globals disableDNSTimeout="false" maxDNSThreads="100">
    <keepalive time="200" interval="75" probes="9"/>
  </Globals>

global:
  expert:
    disableDNSTimeout: false
    maxDNSThreads: 100
    keepalive:
      time: 200
      interval: 75
      probes: 9
*/

static void queryTCPSettings()
{
    UseUDE state = expertTCPSettings.load();
    if (state == UNINIT)
    {
        CriticalBlock block(queryTCPCS);
        state = expertTCPSettings.load();
        if (state == UNINIT)
        {
#ifdef _CONTAINERIZED
            Owned<IPropertyTree> expert;
#else
            Owned<IPropertyTree> envtree;
            IPropertyTree *expert = nullptr;
#endif
            try
            {
#ifdef _CONTAINERIZED
                expert.setown(getGlobalConfigSP()->getPropTree("expert"));
#else
                // MCK - without this many components will not have a global prop
                envtree.setown(getHPCCEnvironment());
                if (envtree)
                    expert = envtree->queryPropTree("Software/Globals");
#endif
            }
            catch (IException *e)
            {
                e->Release();
            }
            catch (...)
            {
            }
            if (expert)
            {
                IPropertyTree *keepalive = expert->queryPropTree("keepalive");
                if (keepalive)
                {
                    keepAliveTime = keepalive->getPropInt("@time", keepAliveTime);
                    keepAliveInterval = keepalive->getPropInt("@interval", keepAliveInterval);
                    keepAliveProbes = keepalive->getPropInt("@probes", keepAliveProbes);
                    hasKeepAlive = true;
                }
                disableDNSTimeout = expert->getPropBool("@disableDNSTimeout", false);
                maxDNSThreads = expert->getPropInt("@maxDNSThreads", maxDNSThreads);
                // could also consider maxDNSThreads==0 as a way to disable DNS timeout ...
                if (maxDNSThreads < 1)
                    maxDNSThreads = 1;
            }
            expertTCPSettings = INITED;
        }
    }
}

bool queryKeepAlive(int &time, int &intvl, int &probes)
{
    queryTCPSettings();
    if (hasKeepAlive)
    {
        time = keepAliveTime;
        intvl = keepAliveInterval;
        probes = keepAliveProbes;
    }
    return hasKeepAlive;
}

void setKeepAlive(bool enabled, int time, int intvl, int probes)
{
    CriticalBlock block(queryTCPCS);
    keepAliveTime = time ? time : 200;
    keepAliveInterval = intvl ? intvl : 75;
    keepAliveProbes = probes ? probes : 9;
    hasKeepAlive = enabled;
    expertTCPSettings = INITED;
}

static int getAddressInfo(const char *name, unsigned *netaddr, bool okToLogErr);

static CriticalSection queryDNSCS;

#ifndef EAI_SYSTEM
#   define EAI_SYSTEM -11
#endif
class CAddrInfoThreadArgs : public CInterface
{
public:
    StringAttr name;
    unsigned netaddr[4] = { 0, 0, 0, 0 };
    std::atomic<int> retCode { EAI_SYSTEM };

    CAddrInfoThreadArgs(const char *_name) : name(_name) { }
};

class CAddrPoolThread : public CInterface, implements IPooledThread
{
public:
    IMPLEMENT_IINTERFACE;

    Linked<CAddrInfoThreadArgs> localAddrThreadInfo;

    virtual void init(void *param) override
    {
        localAddrThreadInfo.setown((CAddrInfoThreadArgs *)param);
    }

    virtual void threadmain() override
    {
        localAddrThreadInfo->retCode = getAddressInfo(localAddrThreadInfo->name.get(), localAddrThreadInfo->netaddr, false);
    }

    virtual bool stop() override
    {
        return true;
    }

    virtual bool canReuse() const override
    {
        return true;
    }
};

class CAddrInfoFactory : public CInterface, public IThreadFactory
{
public:
    IMPLEMENT_IINTERFACE;
    IPooledThread *createNew()
    {
        return new CAddrPoolThread();
    }
};

static Owned<CAddrInfoFactory> addrInfoFactory;
static Owned<IThreadPool> addrInfoPool;
static std::atomic<bool> addrInfoPoolCreated { false };

#if !defined(EMSCRIPTEN)
static bool useDNSTimeout()
{
    queryTCPSettings();
    if (!disableDNSTimeout)
    {
        if (!addrInfoPoolCreated.load())
        {
            CriticalBlock block(queryDNSCS);
            if (!addrInfoPoolCreated.load())
            {
                addrInfoFactory.setown(new CAddrInfoFactory);
                addrInfoPool.setown(createThreadPool("AddrInfoPool", addrInfoFactory, true, nullptr, maxDNSThreads, 100000000));
                addrInfoPoolCreated = true;
            }
        }
        return true;
    }
    return false;
}
#endif

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}

MODULE_EXIT()
{
    // NB: this (and other MODULE_EXITs) are not called for Thor and Roxie because they are
    //     stopped via SIGTERM signal and thus exit() and the atexit handlers are not called
    if (addrInfoPoolCreated.load())
    {
        addrInfoPool->joinAll(true);
        addrInfoPool.clear();
        addrInfoPoolCreated = false;
    }
}

class CSocket: public ISocket, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;

    static          CriticalSection crit;
protected:
    friend class CSocketConnectWait;
    enum { ss_open, ss_shutdown, ss_close, ss_pre_open } state;
    T_SOCKET        sock;
//    char*           hostname;   // host address
    unsigned short  hostport;   // host port (NB: this is the peer port if an attached socket)
    unsigned short  localPort;
    SOCKETMODE      sockmode;
    IpAddress       targetip;   // NB: this is peer if an attached socket
    SocketEndpoint  returnep;   // set by set_return_addr

    MCASTREQ    *   mcastreq;
    size32_t        nextblocksize;
    unsigned        blockflags = BF_ASYNC_TRANSFER;
    unsigned        blocktimeoutms = WAIT_FOREVER;
    bool            owned;
    enum            {accept_not_cancelled, accept_cancel_pending, accept_cancelled} accept_cancel_state;
    bool            in_accept;
    bool            nonblocking;
    bool            nagling;
    SocketStats     stats;

    static unsigned connectingcount;
#ifdef USERECVSEM
    static Semaphore receiveblocksem;
    bool            receiveblocksemowned; // owned by this socket
#endif
#ifdef _TRACE
    char        *   tracename;
#endif
    
public:
    void        open(int listen_queue_size,bool reuseports=false);
    bool        connect_timeout( unsigned timeout, bool noexception);
    void        connect_wait( unsigned timems);
    void        udpconnect();

    void        read(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read, unsigned timeoutsecs, bool suppresGCIfMinSize=true);
    void        readtms(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read, unsigned timedelaysecs, bool suppresGCIfMinSize=true);
    void        read(void* buf, size32_t size);
    size32_t    write(void const* buf, size32_t size);
    size32_t    writetms(void const* buf, size32_t minSize, size32_t size, unsigned timeoutms=WAIT_FOREVER);
    size32_t    write_multiple(unsigned num,void const**buf, size32_t *size);
    size32_t    udp_write_to(const SocketEndpoint &ep,void const* buf, size32_t size);
    void        close();
    void        errclose();
    bool        connectionless() { return (sockmode!=sm_tcp)&&(sockmode!=sm_tcp_server); }
    void        shutdown(unsigned mode=SHUTDOWN_READWRITE);
    void        shutdownNoThrow(unsigned mode);

    ISocket*    accept(bool allowcancel);
    int         wait_read(unsigned timeout);
    int         logPollError(unsigned revents, const char *rwstr);
    int         wait_write(unsigned timeout);
    int         name(char *name,size32_t namemax);
    int         peer_name(char *name,size32_t namemax);
    SocketEndpoint &getPeerEndpoint(SocketEndpoint &ep);
    IpAddress & getPeerAddress(IpAddress &addr);
    SocketEndpoint &getEndpoint(SocketEndpoint &ep) const override;
    void        set_return_addr(int port,const char *name);  // sets returnep
    void        cancel_accept();
    size32_t    get_max_send_size();
    bool        set_nonblock(bool on=true);
    bool        set_nagle(bool on);
    void        set_linger(int lingersecs); 
    virtual void set_quick_ack(bool on) override;
    void        set_keep_alive(bool set);
    void        logConnectionInfo(unsigned timeoutms, unsigned conn_mstime);
    virtual void set_inherit(bool inherit=false);
    virtual bool check_connection();
    virtual bool isSecure() const override;
    virtual unsigned __int64 getStatistic(StatisticKind kind) const override;

    
    // Block functions
    void        set_block_mode(unsigned flags,size32_t recsize=0,unsigned timeoutms=0);
    bool        send_block(const void *blk,size32_t sz);
    size32_t    receive_block_size();
    size32_t    receive_block(void *blk,size32_t sz);

    size32_t    get_send_buffer_size();
    void        set_send_buffer_size(size32_t sz);

    bool        join_multicast_group(SocketEndpoint &ep);   // for udp multicast
    bool        leave_multicast_group(SocketEndpoint &ep);  // for udp multicast

    void        set_ttl(unsigned _ttl);

    size32_t    get_receive_buffer_size();
    void        set_receive_buffer_size(size32_t sz);

    size32_t    avail_read();

    int         pre_connect(bool block);
    int         post_connect();
    void        prepareForAsyncConnect(struct sockaddr *& addr, size32_t &addrlen);
    void        finishAsyncConnect(int connectResult);

    void        setTraceName(const char * prefix, const char * name);
    void        setTraceName();

    CSocket(const SocketEndpoint &_ep,SOCKETMODE smode,const char *name);
    CSocket(T_SOCKET new_sock,SOCKETMODE smode,bool _owned,SocketEndpoint *_peerEp);

    virtual ~CSocket();

    virtual unsigned OShandle() const
    {
        return (unsigned)sock;
    }

    virtual bool isValid() const
    {
        return sock != INVALID_SOCKET;
    }

private:

    int closesock()
    {
        if (sock!=INVALID_SOCKET) {
            T_SOCKET s = sock;
            sock = INVALID_SOCKET;
            nonblocking = false;
            STATS.activesockets--;
    #ifdef SOCKTRACE
            PROGLOG("SOCKTRACE: Closing socket %x %d (%p)", s, s, this);
    #endif
    #ifdef _WIN32
            return ::closesocket(s);
    #else
            ::shutdown(s, SHUT_WR);
            return ::close(s);
    #endif
        }
        else
            return 0;
    }

    void checkCfgKeepAlive();
    void setKeepAlive(bool set, int time, int intval, int probes);

};

CriticalSection CSocket::crit;
unsigned CSocket::connectingcount=0;
#ifdef USERECVSEM
Semaphore CSocket::receiveblocksem(2);
#endif


#ifdef _WIN32
class win_socket_library 
{
    static bool initdone; // to prevent dependancy probs very early on (e.g. jlog)
public:
    win_socket_library() { init(); }
    bool init()
    {
        if (initdone)
            return true;
        WSADATA wsa;
        if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
            if (WSAStartup(MAKEWORD(1, 1), &wsa) != 0) {
                MessageBox(NULL,"Failed to initialize windows sockets","JLib Socket Error",MB_OK);
                return false;
            }
        }
        initdone = true;
        return true;
    }
    ~win_socket_library() 
    {
        WSACleanup();
    }
};

bool win_socket_library::initdone = false;
static win_socket_library ws32_lib;

#define JSE_ADDRINUSE WSAEADDRINUSE
#define JSE_CONNRESET WSAECONNRESET
#define JSE_CONNABORTED WSAECONNABORTED
#define JSE_NOTCONN WSAENOTCONN
#define JSE_WOULDBLOCK WSAEWOULDBLOCK
#define JSE_INPROGRESS WSAEINPROGRESS
#define JSE_NETUNREACH WSAENETUNREACH
#define JSE_NOTSOCK WSAENOTSOCK
#define JSE_TIMEDOUT WSAETIMEDOUT
#define JSE_CONNREFUSED WSAECONNREFUSED
#define JSE_EAGAIN WSAEWOULDBLOCK
#define JSE_BADF WSAEBADF

#define JSE_INTR WSAEINTR

struct j_sockaddr_in6 {
    short   sin6_family;        /* AF_INET6 */
    u_short sin6_port;          /* Transport level port number */
    u_long  sin6_flowinfo;      /* IPv6 flow information */
    struct in_addr6 sin6_addr;  /* IPv6 address */
    u_long sin6_scope_id;       /* set of interfaces for a scope */
};

typedef union {
    struct sockaddr sa;
    struct j_sockaddr_in6 sin6;
    struct sockaddr_in sin;
} J_SOCKADDR;

#define DEFINE_SOCKADDR(name) J_SOCKADDR name; memset(&name,0,sizeof(J_SOCKADDR))


static int _inet_pton(int af, const char* src, void* dst)
{
    
    DEFINE_SOCKADDR(u);
    
    int address_length;
    
    switch (af) {
    case AF_INET:
        u.sin.sin_family    = AF_INET;
        address_length = sizeof (u.sin);
        break;
        
    case AF_INET6:
        u.sin6.sin6_family  = AF_INET6;
        address_length = sizeof (u.sin6);
        break;
        
    default:
#ifdef EAFNOSUPPORT
        errno = EAFNOSUPPORT;
#else
        errno = 52;
#endif
        return -1;
    }
    ws32_lib.init();
    int ret = WSAStringToAddress ((LPTSTR) src, af, NULL, &u.sa, &address_length);
    if (ret == 0) {
        switch (af) {
        case AF_INET:
            memcpy (dst, &u.sin.sin_addr, sizeof (struct in_addr));
            break;
            
        case AF_INET6:
            memcpy (dst, &u.sin6.sin6_addr, sizeof (u.sin6.sin6_addr));
            break;
        }
        return 1;
    }
    errno = WSAGetLastError();  
    // PROGLOG("errno = %d",errno);
    return 0;
}

static const char * _inet_ntop (int af, const void *src, char *dst, socklen_t cnt)
{
    /* struct sockaddr can't accomodate struct sockaddr_in6. */
    DEFINE_SOCKADDR(u);

    DWORD dstlen = cnt;
    size_t srcsize;
    
    memset(&u,0,sizeof(u));
    switch (af) {
    case AF_INET:
        u.sin.sin_family = AF_INET;
        u.sin.sin_addr = *(struct in_addr *) src;
        srcsize = sizeof (u.sin);
        break;
    case AF_INET6:
        u.sin6.sin6_family = AF_INET6;
        memcpy(&u.sin6.sin6_addr,src,sizeof(in_addr6));
        srcsize = sizeof (u.sin6);
        break;
    default:
        return NULL;
    }
    
    ws32_lib.init();
    if (WSAAddressToString (&u.sa, srcsize, NULL, dst, &dstlen) != 0) {
        errno = WSAGetLastError();
        return NULL;
    }
    return (const char *) dst;
}


int inet_aton (const char *name, struct in_addr *addr)
{
    addr->s_addr = inet_addr (name);
    return (addr->s_addr == (u_long)-1)?1:0;        // 255.255.255.255 has had it here
}



#else

#define JSE_ADDRINUSE EADDRINUSE
#define JSE_CONNRESET ECONNRESET
#define JSE_CONNABORTED ECONNABORTED
#define JSE_NOTCONN ENOTCONN
#define JSE_WOULDBLOCK EWOULDBLOCK
#define JSE_INPROGRESS EINPROGRESS
#define JSE_NETUNREACH ENETUNREACH
#define JSE_NOTSOCK ENOTSOCK
#define JSE_TIMEDOUT ETIMEDOUT
#define JSE_CONNREFUSED ECONNREFUSED
#define JSE_EAGAIN EAGAIN
#define JSE_BADF EBADF


#define _inet_ntop inet_ntop
#define _inet_pton inet_pton

#define in_addr6 in6_addr 

typedef union {
    struct sockaddr sa;
    struct sockaddr_in6 sin6;
    struct sockaddr_in sin;
} J_SOCKADDR;

#define DEFINE_SOCKADDR(name) J_SOCKADDR name; memset(&name,0,sizeof(J_SOCKADDR))


#define JSE_INTR EINTR
#ifndef INADDR_NONE
#define INADDR_NONE (-1)
#endif
#endif

#ifndef INET6_ADDRSTRLEN
#define INET6_ADDRSTRLEN 65
#endif

inline void LogErr(unsigned err,unsigned ref,const char *info,unsigned lineno,const char *tracename)
{
    if (err)
    {
        PROGLOG("jsocket(%d,%d)%s%s err = %d%s%s",ref,lineno,
           (info&&*info)?" ":"",(info&&*info)?info:"",err,
           (tracename&&*tracename)?" : ":"",(tracename&&*tracename)?tracename:"");
#ifdef _TRACELINKCLOSED
        if ((JSE_NOTCONN == err) || (JSE_CONNRESET == err) || (JSE_CONNABORTED == err))
        {
            PROGLOG("Socket not connected, stack:");
            PrintStackReport();
        }
#endif
    }
}



inline socklen_t setSockAddr(J_SOCKADDR &u, const IpAddress &ip,unsigned short port)
{
    if (!IP6preferred) {
        if (ip.getNetAddress(sizeof(in_addr),&u.sin.sin_addr)==sizeof(in_addr)) {
            u.sin.sin_family = AF_INET; 
            u.sin.sin_port = htons(port);
            return sizeof(u.sin);
        }
    }
    if (IP4only)
        IPV6_NOT_IMPLEMENTED();
    ip.getNetAddress(sizeof(in_addr6),&u.sin6.sin6_addr);
    u.sin6.sin6_family = AF_INET6; 
    u.sin6.sin6_port = htons(port);
    return sizeof(u.sin6);
}

inline socklen_t setSockAddrAny(J_SOCKADDR &u, unsigned short port)
{
    if (IP6preferred) {
#ifdef _WIN32
        IN6ADDR_SETANY((PSOCKADDR_IN6)&u.sin6.sin6_addr);
#else
        memcpy(&u.sin6.sin6_addr,&in6addr_any,sizeof(in_addr6));
#endif
        u.sin6.sin6_family= AF_INET6;                           
        u.sin6.sin6_port = htons(port);
        return sizeof(u.sin6);
    }
    u.sin.sin_addr.s_addr = htonl(INADDR_ANY);
    u.sin.sin_family= AF_INET;                          
    u.sin.sin_port = htons(port);
    return sizeof(u.sin);
}

inline void getSockAddrEndpoint(const J_SOCKADDR &u, socklen_t ul, SocketEndpoint &ep)
{
    if (ul==sizeof(u.sin)) {
        ep.setNetAddress(sizeof(in_addr),&u.sin.sin_addr);
        ep.port = htons(u.sin.sin_port); 
    }
    else {
        ep.setNetAddress(sizeof(in_addr6),&u.sin6.sin6_addr);
        ep.port = htons(u.sin6.sin6_port); 
    }
}



/* might need fcntl(F_SETFL), or ioctl(FIONBIO) */
/* Posix.1g says fcntl */

#if defined(O_NONBLOCK) 

bool CSocket::set_nonblock(bool on)
{
    if (nonblocking==on)
        return nonblocking;

    int flags = fcntl(sock, F_GETFL, 0);
    if (flags == -1)
        return nonblocking;
    if (on)
        flags |= O_NONBLOCK;
    else
        flags &= ~O_NONBLOCK;
    if (fcntl(sock, F_SETFL, flags)==0) {
        bool wasNonBlocking = nonblocking;
        nonblocking = on;
        return wasNonBlocking;
    }
    return nonblocking;
}



#else

bool CSocket::set_nonblock(bool on)
{
#ifdef _WIN32
    u_long yes = on?1:0;
    if (ioctlsocket(sock, FIONBIO, &yes)==0) {
#else
    int yes = on?1:0;
    if (ioctl(sock, FIONBIO, &yes)==0) {
#endif
        bool wasNonBlocking = nonblocking;
        nonblocking = on;
        return wasNonBlocking;
    }
    return nonblocking;
}

#endif

bool CSocket::set_nagle(bool on)
{
    bool ret = nagling;
    nagling = on;
    int enabled = !on;
    if (setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (char*)&enabled, sizeof(enabled)) != 0) {
        nagling = !on;
    }
    return ret;
}

void CSocket::set_quick_ack(bool on)
{
#ifdef TCP_QUICKACK
    int flag = (int)on;
    setsockopt(sock, IPPROTO_TCP, TCP_QUICKACK, &flag, sizeof(flag));
#endif
}

void CSocket::set_inherit(bool inherit)
{
#ifndef _WIN32
    long flag = fcntl(sock, F_GETFD);
    if(inherit)
        flag &= ~FD_CLOEXEC;
    else
        flag |= FD_CLOEXEC;
    fcntl(sock, F_SETFD, flag);
#endif
}

size32_t CSocket::avail_read()
{
#ifdef _WIN32
    u_long avail;
    if (ioctlsocket(sock, FIONREAD, &avail)==0) 
#else
    int avail;
    if (ioctl(sock, FIONREAD, &avail)==0) 
#endif
        return (size32_t)avail;
    int err = SOCKETERRNO();
    LOGERR2(err,1,"avail_read");
    return 0;
}

#define PRE_CONN_UNREACH_ELIM  100

// Static helper: Set socket blocking/non-blocking mode
static bool set_socket_nonblock(T_SOCKET sock, bool nonblocking)
{
#if defined(O_NONBLOCK)
    int flags = fcntl(sock, F_GETFL, 0);
    if (flags == -1)
        return false;
    if (nonblocking)
        flags |= O_NONBLOCK;
    else
        flags &= ~O_NONBLOCK;
    return (fcntl(sock, F_SETFL, flags) == 0);
#else
#ifdef _WIN32
    u_long yes = nonblocking ? 1 : 0;
    return (ioctlsocket(sock, FIONBIO, &yes) == 0);
#else
    int yes = nonblocking ? 1 : 0;
    return (ioctl(sock, FIONBIO, &yes) == 0);
#endif
#endif
}

// Static helper: Prepare a socket for connection (shared by sync and async paths)
// Returns the socket descriptor and sockaddr structure
// Throws exception on error
static T_SOCKET prepare_socket_for_connect(const IpAddress & targetip, unsigned short hostport, J_SOCKADDR & sockaddr, socklen_t & sockaddrlen, const char * tracename)
{
    if (targetip.isNull())
    {
        StringBuffer err;
        err.appendf("prepare_socket_for_connect - Invalid/missing host IP address raised in : %s, line %d",sanitizeSourceFile(__FILE__), __LINE__);
        IJSOCK_Exception *e = new SocketException(JSOCKERR_bad_netaddr,err.str());
        throw e;
    }

    memset(&sockaddr, 0, sizeof(J_SOCKADDR));
    sockaddrlen = setSockAddr(sockaddr, targetip, hostport);
    T_SOCKET sock = ::socket(sockaddr.sa.sa_family, SOCK_STREAM, targetip.isIp4() ? 0 : PF_INET6);
    if (sock == INVALID_SOCKET)
    {
        int err = SOCKETERRNO();
        LOGERR(err, 1, "prepare_socket_for_connect");
        throwJSockTargetException(err, tracename, __FILE__, __LINE__);
    }

    STATS.activesockets++;
    return sock;
}

// Static helper: Perform synchronous connect on prepared socket
// Returns error code (0 = success, or error that needs handling by caller)
static int perform_socket_connect(T_SOCKET sock, const J_SOCKADDR & sockaddr, socklen_t sockaddrlen, bool nonblocking, const char * tracename)
{
    // Set socket to blocking or non-blocking mode
    set_socket_nonblock(sock, nonblocking);

    int rc = ::connect(sock, &sockaddr.sa, sockaddrlen);
    int err = 0;
    if (rc == SOCKET_ERROR)
    {
        err = SOCKETERRNO();
        if ((err != JSE_INPROGRESS) && (err != JSE_WOULDBLOCK) && (err != JSE_TIMEDOUT) && (err != JSE_CONNREFUSED))    // handled by caller
        {
            if (err != JSE_NETUNREACH)
            {
                pre_conn_unreach_cnt.store(0);
                LOGERR(err, 1, "perform_socket_connect");
            }
            else
            {
                int ecnt = pre_conn_unreach_cnt.load();
                if (ecnt <= PRE_CONN_UNREACH_ELIM)
                {
                    pre_conn_unreach_cnt.fetch_add(1);
                    LOGERR(err, 1, "perform_socket_connect network unreachable");
                }
            }
        }
        else
            pre_conn_unreach_cnt.store(0);
    }
    else
        pre_conn_unreach_cnt.store(0);

#ifdef SOCKTRACE
    PROGLOG("SOCKTRACE: perform_socket_connect socket%s %x %d err=%d", nonblocking?"":"(block)", sock, sock, err);
#endif
    return err;
}

// Static helper: Complete post-connect operations (shared by sync and async paths)
// Returns error code (0 = success)
static int finalize_socket_connect(T_SOCKET sock, const char * tracename)
{
    // Ensure socket is in non-blocking mode
    set_socket_nonblock(sock, true);

    int err = 0;
    socklen_t errlen = sizeof(err);
    int rc = getsockopt(sock, SOL_SOCKET, SO_ERROR, (char *)&err, &errlen); // check for error
    if ((rc != 0) && !err)
        err = SOCKETERRNO();  // some implementations of getsockopt duff
    if ((err != 0) && (err != JSE_TIMEDOUT) && (err != JSE_CONNREFUSED)) // handled by caller
        LOGERR(err, 1, "finalize_socket_connect");
    return err;
}

int CSocket::pre_connect(bool block)
{
    DEFINE_SOCKADDR(u);
    socklen_t ul;
    sock = prepare_socket_for_connect(targetip, hostport, u, ul, tracename);
    owned = true;
    state = ss_pre_open;            // will be set to open by post_connect

    checkCfgKeepAlive();

    nonblocking = !block;  // Track the blocking state
    int err = perform_socket_connect(sock, u, ul, !block, tracename);
#ifdef SOCKTRACE
    PROGLOG("SOCKTRACE: pre-connected socket%s %x %d (%p) err=%d", block?"(block)":"", sock, sock, this, err);
#endif
    return err;
}

int CSocket::post_connect()
{
    int err = finalize_socket_connect(sock, tracename);
    if (err==0)
    {
        nagling = true;
        set_nagle(false);
        state = ss_open;
        SocketEndpoint ep;
        localPort = getEndpoint(ep).port;
    }
    return err;
}

// Memory for addr is allocated by this function and must be freed -- via free() -- by the caller
void CSocket::prepareForAsyncConnect(struct sockaddr *& addr, size32_t & addrlen)
{
    // Prepare the socket for async connection
    DEFINE_SOCKADDR(u);
    socklen_t ul;
    sock = prepare_socket_for_connect(targetip, hostport, u, ul, tracename);
    owned = true;
    state = ss_pre_open;  // will be set to open by finishAsyncConnect

    checkCfgKeepAlive();
    set_nonblock(true);  // Async connections must be non-blocking

    // Allocate and copy the sockaddr for the caller
    addr = (struct sockaddr *)malloc(ul);
    memcpy(addr, &u, ul);
    addrlen = ul;

#ifdef _TRACE
    setTraceName();
#endif
}

void CSocket::finishAsyncConnect(int connectResult)
{
    if (connectResult < 0)
    {
        // Connection failed
        errclose();
        THROWJSOCKTARGETEXCEPTION(JSOCKERR_connection_failed);
    }

    // Finalize the connection
    int err = finalize_socket_connect(sock, tracename);
    if (err != 0)
    {
        errclose();
        THROWJSOCKTARGETEXCEPTION(err);
    }

    // Complete socket setup
    nagling = true;
    set_nagle(false);
    state = ss_open;
    SocketEndpoint ep;
    localPort = getEndpoint(ep).port;

    STATS.connects++;
#ifdef SOCKTRACE
    PROGLOG("SOCKTRACE: async connect completed for socket %x %d (%p)", sock, sock, this);
#endif
}

void CSocket::open(int listen_queue_size,bool reuseports)
{
    // If listen_queue_size==0 then bind port to address but
    // do not actually listen() for accepting connections.
    // This is used when a unique IP:port is needed for MP client
    // INode/IGroup internals, but client never actually accepts connections.

    if (IP6preferred)
        sock = ::socket(AF_INET6, connectionless()?SOCK_DGRAM:SOCK_STREAM, PF_INET6);
    else
        sock = ::socket(AF_INET, connectionless()?SOCK_DGRAM:SOCK_STREAM, 0);
    if (sock == INVALID_SOCKET) {
        THROWJSOCKTARGETEXCEPTION(SOCKETERRNO());
    }

    checkCfgKeepAlive();
    set_nonblock(true);

    STATS.activesockets++;

#ifdef SOCKTRACE
    PROGLOG("SOCKTRACE: opened socket %x %d (%p)", sock,sock,this);
#endif

    if ((hostport==0)&&(sockmode==sm_udp)) {
        state = ss_open;
#ifdef SOCKTRACE
        PROGLOG("SOCKTRACE: opened socket return udp");
#endif
        set_inherit(false);
        return;
    }

#ifndef _WIN32
    reuseports = true;  // for some reason linux requires reuse ports
#endif
    if (reuseports && listen_queue_size) {
        int on = 1;
        setsockopt( sock, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on));
    }

    DEFINE_SOCKADDR(u);
    socklen_t  ul;
    if (!targetip.isNull()) {
        ul = setSockAddr(u,targetip,hostport);
    }
    else 
        ul = setSockAddrAny(u,hostport);
    int saverr;
    if (::bind(sock, &u.sa, ul) != 0) {
        saverr = SOCKETERRNO();
        if (saverr==JSE_ADDRINUSE) {   // don't log as error (some usages probe ports)
ErrPortInUse:
            closesock();
            char msg[1024]; 
            sprintf(msg,"Target: %s, port = %d, Raised in: %s, line %d",tracename,(int)hostport,sanitizeSourceFile(__FILE__), __LINE__);
            IJSOCK_Exception *e = new SocketException(JSOCKERR_port_in_use,msg);
            throw e; 
        }
        else {
            closesock();
            THROWJSOCKTARGETEXCEPTION(saverr);
        }
    }
    if (!connectionless() && listen_queue_size) {
        if (::listen(sock, listen_queue_size) != 0) {
            saverr = SOCKETERRNO();
            if (saverr==JSE_ADDRINUSE)
                goto ErrPortInUse;
            closesock();
            THROWJSOCKTARGETEXCEPTION(saverr);
        }
    }
    if (mcastreq) {
        if (setsockopt(sock, IPPROTO_IP, IP_ADD_MEMBERSHIP,(char*)mcastreq, sizeof(*mcastreq))!=0) {
            saverr = SOCKETERRNO();
            closesock();
            THROWJSOCKTARGETEXCEPTION(saverr);
        }
    }

    state = ss_open;
#ifdef SOCKTRACE
    PROGLOG("SOCKTRACE: opened socket return");
#endif
    set_inherit(false);
}



ISocket* CSocket::accept(bool allowcancel)
{
    if ((accept_cancel_state!=accept_not_cancelled) && allowcancel) {
        accept_cancel_state=accept_cancelled;
        return NULL;
    }
    if (state != ss_open) {
        IERRLOG("invalid accept, state = %d",(int)state);
        THROWJSOCKTARGETEXCEPTION(JSOCKERR_not_opened);
    }
    if (connectionless()) {
        THROWJSOCKTARGETEXCEPTION(JSOCKERR_connectionless_socket);
    }

    DEFINE_SOCKADDR(peerSockAddr);      // used if peerIp
    socklen_t peerSockAddrLen = sizeof(peerSockAddr);

    T_SOCKET newsock;
    for (;;)
    {
        in_accept = true;
        newsock = (sock!=INVALID_SOCKET)?::accept(sock, &peerSockAddr.sa, &peerSockAddrLen):INVALID_SOCKET;
        in_accept = false;
#ifdef SOCKTRACE
        PROGLOG("SOCKTRACE: accept created socket %x %d (%p)", newsock,newsock,this);
#endif

        if (newsock!=INVALID_SOCKET)
        {
            if ((sock==INVALID_SOCKET)||(accept_cancel_state==accept_cancel_pending)) {
                ::close(newsock);
                newsock=INVALID_SOCKET;
            }
            else
            {
                accept_cancel_state = accept_not_cancelled;
                break;
            }
        }
        int saverr;
        saverr = SOCKETERRNO();

        if ((sock==INVALID_SOCKET)||(accept_cancel_state==accept_cancel_pending))
        {
            accept_cancel_state = accept_cancelled;
            if (allowcancel)
                return NULL;
            THROWJSOCKTARGETEXCEPTION(JSOCKERR_cancel_accept);
        }
        else if (saverr == EAGAIN || saverr == EWOULDBLOCK)
        {
            int rc = wait_read(WAIT_FOREVER); // JCSMORE accept should support a timeout really
            if ((sock==INVALID_SOCKET)||(accept_cancel_state==accept_cancel_pending))
            {
                accept_cancel_state = accept_cancelled;
                if (allowcancel)
                    return NULL;
                THROWJSOCKTARGETEXCEPTION(JSOCKERR_cancel_accept);
            }
            if (rc < 0)
                THROWJSOCKTARGETEXCEPTION(SOCKETERRNO());
            else if (rc == 0)
                THROWJSOCKTARGETEXCEPTION(JSOCKERR_timeout_expired);
            continue;
        }

        if (saverr != JSE_INTR)
        {
            accept_cancel_state = accept_not_cancelled;
            THROWJSOCKTARGETEXCEPTION(saverr);
        }
    }
    if (state != ss_open)
    {
        accept_cancel_state = accept_cancelled;
        if (allowcancel)
            return NULL;
        THROWJSOCKTARGETEXCEPTION(JSOCKERR_cancel_accept);
    }

    SocketEndpoint peerEp;
    getSockAddrEndpoint(peerSockAddr, peerSockAddrLen, peerEp);
    CSocket *ret = new CSocket(newsock,sm_tcp,true,&peerEp);
    ret->checkCfgKeepAlive();
    ret->set_inherit(false);
    ret->set_nonblock(true);
    return ret;
}


void CSocket::set_linger(int lingertime)
{
    struct linger l;
    l.l_onoff = (lingertime>=0)?1:0;
    l.l_linger = (lingertime>=0)?lingertime:0;
    if (setsockopt(sock, SOL_SOCKET, SO_LINGER, (char*)&l, sizeof(l)) != 0) {
        IWARNLOG("Linger not set");
    }
}

void CSocket::setKeepAlive(bool set, int time, int intvl, int probes)
{
    if (connectionless())
        return;

    int on=set?1:0;
    if (setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, (char*)&on, sizeof(on)) != 0)
    {
        OWARNLOG("KeepAlive not set");
        return;
    }

    if (!on)
        return;

    int srtn, optval;
    socklen_t optlen = sizeof(optval);

    if (time >= 0)
    {
        optval = time;
        srtn = setsockopt(sock, IPPROTO_TCP, TCP_KEEPIDLE, (char *)&optval, optlen);
        if (srtn != 0)
            OWARNLOG("KeepAlive time not set");
    }

    if (intvl >= 0)
    {
        optval = intvl;
        srtn = setsockopt(sock, IPPROTO_TCP, TCP_KEEPINTVL, (char *)&optval, optlen);
        if (srtn != 0)
            OWARNLOG("KeepAlive interval not set");
    }

    if (probes >= 0)
    {
        optval = probes;
        srtn = setsockopt(sock, IPPROTO_TCP, TCP_KEEPCNT, (char *)&optval, optlen);
        if (srtn != 0)
            OWARNLOG("KeepAlive probes not set");
    }
}

void CSocket::checkCfgKeepAlive()
{
    if (connectionless())
        return;

    int time, intvl, probes;
    if (queryKeepAlive(time, intvl, probes))
        setKeepAlive(true, time, intvl, probes);
}

void CSocket::set_keep_alive(bool set)
{
    setKeepAlive(set, -1, -1, -1);
}


int CSocket::name(char *retname,size32_t namemax)
{
    SocketEndpoint ep;
    getEndpoint(ep);
    if (retname && namemax)
    {
        StringBuffer s;
        ep.getHostText(s);
        if (namemax-1<s.length())
            s.setLength(namemax-1);
        memcpy(retname,s.str(),s.length()+1);
    }
    return ep.port;
}

int CSocket::peer_name(char *retname,size32_t namemax)
{
    // should not raise exceptions
    int ret = 0;
    if (!retname)
        namemax = 0;
    if (namemax)
        retname[0] = 0;
    if (state != ss_open) {
        return -1; // don't log as used to test socket
    }
    StringBuffer s;
    if (sockmode==sm_udp_server) { // udp server
        returnep.getHostText(s);
        ret =  returnep.port;
    }   
    else {
        DEFINE_SOCKADDR(u);
        socklen_t ul = sizeof(u);       
        if (::getpeername(sock,&u.sa, &ul)<0)           
            return -1;      // don't log as used to test socket
        SocketEndpoint ep;
        getSockAddrEndpoint(u,ul,ep);
        ep.getHostText(s);
        ret = ep.port;
    }
    if (namemax>1) {
        if (namemax-1<s.length())
            s.setLength(namemax-1);
        memcpy(retname,s.str(),s.length()+1);
    }
    return ret;
}

SocketEndpoint &CSocket::getPeerEndpoint(SocketEndpoint &ep)
{
    if (state != ss_open) {
        THROWJSOCKTARGETEXCEPTION(JSOCKERR_not_opened);
    }
    StringBuffer s;
    if (sockmode==sm_udp_server) { // udp server
        ep.set(returnep);
    }   
    else
        ep.set(hostport, targetip); // NB: if this is an attached socket, targetip/hostpost are the peer
    return ep;
}

IpAddress & CSocket::getPeerAddress(IpAddress &addr)
{
    SocketEndpoint ep;
    getPeerEndpoint(ep);
    addr = ep;
    return addr;
}

void CSocket::set_return_addr(int port,const char *retname)
{
    if (!returnep.ipset(retname)) {
        IJSOCK_Exception *e = new SocketException(JSOCKERR_bad_address); // don't use THROWJSOCKTARGETEXCEPTION here
        throw e;
    }
    returnep.port = port;
}


SocketEndpoint &CSocket::getEndpoint(SocketEndpoint &ep) const
{
    if (state != ss_open) {
        THROWJSOCKTARGETEXCEPTION(JSOCKERR_not_opened);
    }
    DEFINE_SOCKADDR(u);
    socklen_t ul = sizeof(u);
    if (::getsockname(sock,&u.sa, &ul)<0) {
        THROWJSOCKTARGETEXCEPTION(SOCKETERRNO());
    }
    getSockAddrEndpoint(u,ul,ep);
    return ep;
}

void CSocket::cancel_accept()
{
    if (connectionless()) {
        THROWJSOCKTARGETEXCEPTION(JSOCKERR_connectionless_socket);
    }
#ifdef SOCKTRACE
    PROGLOG("SOCKTRACE: Cancel accept socket %x %d (%p)", sock, sock, this);
#endif
    if (!in_accept) {
        accept_cancel_state = accept_cancelled;
        shutdown();
        errclose();
        return;
    }
    accept_cancel_state = accept_cancel_pending;
    errclose();     // this should cause accept to terminate (not supported on all linux though)
#ifdef _WIN32        
    for (unsigned i=0;i<5;i++) { // windows closes on different lower priority thread
        Sleep(i);
        if (accept_cancel_state==accept_cancelled) 
            return;
    }
#else
    Sleep(0);
    if (accept_cancel_state==accept_cancelled) 
        return;
#endif
    // Wakeup listener using a connection
    SocketEndpoint ep(hostport,queryHostIP());
    Owned<CSocket> sock = new CSocket(ep,sm_tcp,NULL);
    try {
        sock->connect_timeout(100,true);
    }
    catch (IJSOCK_Exception *e) { 
        EXCLOG(e,"CSocket::cancel_eccept");
        e->Release();                           
    }
}


// ================================================================================
// connect versions

ISocket*  ISocket::connect( const SocketEndpoint &ep )
{
    // general connect 
    return ISocket::connect_wait(ep,DEFAULT_CONNECT_TIME);
}

inline void refused_sleep(CTimeMon &tm, unsigned &refuseddelay)
{
    unsigned remaining;
    if (!tm.timedout(&remaining)) {
        if (refuseddelay<remaining/4) {
            Sleep(refuseddelay);
            if (refuseddelay<CONNECT_TIMEOUT_REFUSED_WAIT/2)
                refuseddelay *=2;
            else
                refuseddelay = CONNECT_TIMEOUT_REFUSED_WAIT;
        }
        else 
        {
            unsigned delay = remaining/4;
            if (delay == 0)
                delay = 1;
            Sleep(delay); // towards end of timeout approach gradually
        }
    }
}

// JCSMORE - there's a lot of duplicated nuanced code here and in connect_wait
// that could do with clearing up. Some of it is very similar to wait_write

// As they current stand:
// connect_timeout if successful, will always leave the socket in a non-blocking state
//
// connect_wait has weird semantic of trying one final attempt if it has timedout, where it will switch to blocking mode
// NB: that will happen on 1st attempt if connect_wait is passed 0 as a timeout.
//
// pre_connect() is what actually customizes the socket [conditionally] sets it to non-blocking mode
// post_connect() errors checks and ensures it is in non-blocking mode (only relevant if connect_wait left it in blocking mode)
bool CSocket::connect_timeout(unsigned timeout, bool noexception)
{
    // simple connect with timeout (no fancy stuff!)
    unsigned startt = usTick();
    CTimeMon tm(timeout);
    unsigned remaining;
    unsigned refuseddelay = 1;
    int err;
    while (!tm.timedout(&remaining))
    {
        err = pre_connect(false); // NB: sets and leaves socket into non-blocking mode
        if ((err == JSE_INPROGRESS)||(err == JSE_WOULDBLOCK))
        {
#ifdef _USE_SELECT
            T_FD_SET fds;
            struct timeval tv;
            CHECKSOCKRANGE(sock);
            XFD_ZERO(&fds);
            FD_SET((unsigned)sock, &fds);
            T_FD_SET except;
            XFD_ZERO(&except);
            FD_SET((unsigned)sock, &except);
            tv.tv_sec = remaining / 1000;
            tv.tv_usec = (remaining % 1000)*1000;
            int rc = ::select( sock + 1, NULL, (fd_set *)&fds, (fd_set *)&except, &tv );
#else
            struct pollfd fds[1];
            fds[0].fd = sock;
            fds[0].events = POLLOUT;
            fds[0].revents = 0;
            int rc = ::poll(fds, 1, remaining);
#endif
            if (rc>0)
            {
                // select/poll succeeded - return error from socket (0 if connected)
                socklen_t errlen = sizeof(err);
                rc = getsockopt(sock, SOL_SOCKET, SO_ERROR, (char *)&err, &errlen); // check for error
                if ((rc!=0)&&!err) 
                    err = SOCKETERRNO();  // some implementations of getsockopt duff
                if (err) //  probably ECONNREFUSED but treat all errors same
                    refused_sleep(tm,refuseddelay);
            }
            else if (rc<0)
            {
                err = SOCKETERRNO();
                if (err != JSE_INTR)
                {
                    LOGERR2(err,2,"::select/poll");
                    errclose();
                    break;
                }
            }
        }
        else if (err)
            refused_sleep(tm, refuseddelay); // this stops becoming cpu bound
        if (err==0)
        {
            err = post_connect();
            if (err==0)
            {
                STATS.connects++;
                STATS.connecttime+=usTick()-startt;
#ifdef _TRACE
                setTraceName();
#endif              
#ifdef SOCKTRACE
                unsigned conn_mstime = (usTick() - startt) / 1000;
                logConnectionInfo(timeout, conn_mstime);
#endif
                return true;
            }
        }
        errclose();
    }
#ifdef SOCKTRACE
    PROGLOG("connect_timeout: failed %d",err);
#endif
    STATS.failedconnects++;
    STATS.failedconnecttime+=usTick()-startt;
    if (!noexception)
        THROWJSOCKTARGETEXCEPTION(JSOCKERR_connection_failed);
    return false;
}


ISocket*  ISocket::connect_timeout(const SocketEndpoint &ep,unsigned timeout)
{
    if (ep.isNull()||(ep.port==0))
        THROWJSOCKEXCEPTION(JSOCKERR_bad_address);
    Owned<CSocket> sock = new CSocket(ep,sm_tcp,NULL);
    sock->connect_timeout(timeout,false);
    return sock.getClear();
}

#define SOCKET_POLLTIME 50

void CSocket::connect_wait(unsigned timems)
{
    // simple connect with timeout (no fancy stuff!)
    unsigned startt = usTick();
    CTimeMon tm(timems);
    bool exit = false;
    int err;
    unsigned refuseddelay = 1;
    while (!exit)
    {
        unsigned remaining;
        exit = tm.timedout(&remaining);
        bool blockselect = exit;                            // if last time round block
        {
            CriticalBlock block(crit);
            if (++connectingcount>4)
                blockselect = true;
        }
        err = pre_connect(blockselect);
        if (blockselect)
        {
            if (err&&!exit)
                refused_sleep(tm,refuseddelay); //  probably ECONNREFUSED but treat all errors same
        }
        else
        {
            unsigned timeoutms = (exit||(remaining<10000))?10000:remaining;
    #ifndef BLOCK_POLLED_SINGLE_CONNECTS
            unsigned polltime = 1;
    #endif
            while (!blockselect && ((err == JSE_INPROGRESS)||(err == JSE_WOULDBLOCK)))
            {

#ifdef _USE_SELECT
                T_FD_SET fds;
                struct timeval tv;
                CHECKSOCKRANGE(sock);
                XFD_ZERO(&fds);
                FD_SET((unsigned)sock, &fds);
                T_FD_SET except;
                XFD_ZERO(&except);
                FD_SET((unsigned)sock, &except);
    #ifdef BLOCK_POLLED_SINGLE_CONNECTS         
                tv.tv_sec = timeoutms / 1000;
                tv.tv_usec = (timeoutms % 1000)*1000;
    #else
                tv.tv_sec = 0;
                tv.tv_usec = 0;
    #endif
                int rc = ::select( sock + 1, NULL, (fd_set *)&fds, (fd_set *)&except, &tv );
#else
                struct pollfd fds[1];
                fds[0].fd = sock;
                fds[0].events = POLLOUT;
                fds[0].revents = 0;
    #ifdef BLOCK_POLLED_SINGLE_CONNECTS
                int pollTimeOut = timeoutms;
    #else
                int pollTimeOut = 0;
    #endif
                int rc = ::poll(fds, 1, pollTimeOut);
#endif
                if (rc>0)
                {
                    // select/poll succeeded - return error from socket (0 if connected)
                    socklen_t errlen = sizeof(err);
                    rc = getsockopt(sock, SOL_SOCKET, SO_ERROR, (char *)&err, &errlen); // check for error
                    if ((rc!=0)&&!err)
                        err = SOCKETERRNO();  // some implementations of getsockopt duff
                    if (err)
                        refused_sleep(tm,refuseddelay); //  probably ECONNREFUSED but treat all errors same
                    break;
                }
                if (rc<0)
                {
                    err = SOCKETERRNO();
                    if (err != JSE_INTR)
                    {
                        LOGERR2(err,2,"::select/poll");
                        break;
                    }
                }
                if (!timeoutms)
                {
    #ifdef SOCKTRACE
                    PROGLOG("connecttimeout: timed out");
    #endif
                    err = -1;
                    break;
                }
    #ifdef BLOCK_POLLED_SINGLE_CONNECTS 
                break;
    #else
                if (timeoutms<polltime)
                    polltime = timeoutms;
                Sleep(polltime);                    // sleeps 1-50ms (to let other threads run)
                timeoutms -= polltime;
                if (polltime>SOCKET_POLLTIME/2)
                    polltime = SOCKET_POLLTIME;
                else
                    polltime *= 2;
    #endif
            }
        }
        {
            CriticalBlock block(crit);
            --connectingcount;
        }
        if (err==0)
        {
            err = post_connect();
            if (err==0)
            {
                STATS.connects++;
                STATS.connecttime+=usTick()-startt;
#ifdef _TRACE
                setTraceName();
#endif              
#ifdef SOCKTRACE
                unsigned conn_mstime = (usTick() - startt) / 1000;
                logConnectionInfo(timems, conn_mstime);
#endif
                return;
            }
        }
        errclose();
    }
#ifdef SOCKTRACE
    PROGLOG("connect_wait: failed %d",err);
#endif
    STATS.failedconnects++;
    STATS.failedconnecttime+=usTick()-startt;
    THROWJSOCKTARGETEXCEPTION(JSOCKERR_connection_failed);
}

void CSocket::setTraceName(const char * prefix, const char * name)
{
#ifdef _TRACE
    StringBuffer peer;
    peer.append(prefix);
    if (state == ss_open)
        peer.append(":").append(localPort).append(" -> ");
    peer.append(name?name:"(NULL)");
    peer.append(":").append(hostport);
    if (sock != INVALID_SOCKET)
        peer.append(" (").append(sock).append(")");

    free(tracename);
    tracename = peer.detach();
#endif
}

void CSocket::setTraceName()
{
#ifdef _TRACE
    StringBuffer hostname;
    targetip.getHostText(hostname);
    setTraceName("C!", hostname);
#endif
}


ISocket *  ISocket::connect_wait( const SocketEndpoint & ep, unsigned timems)
{
    if (ep.isNull() || (ep.port==0))
        THROWJSOCKEXCEPTION(JSOCKERR_bad_address);
    Owned<CSocket> sock = new CSocket(ep,sm_tcp,NULL);
    sock->connect_wait(timems);
    return sock.getClear();
}

// Create a socket prepared for async connection - returns socket descriptor and sockaddr for use with io_uring
// memory for addr is allocated by this function and must be freed -- via free() -- by the caller
ISocket * ISocket::createForAsyncConnect(const SocketEndpoint & ep, struct sockaddr *& addr, size32_t & addrlen)
{
    if (ep.isNull() || (ep.port==0))
        THROWJSOCKEXCEPTION(JSOCKERR_bad_address);

    Owned<CSocket> csock = new CSocket(ep, sm_tcp, NULL);
    csock->prepareForAsyncConnect(addr, addrlen);
    return csock.getClear();
}

// Complete an async connection after io_uring connect completes
void ISocket::completeAsyncConnect(ISocket * socket, int connectResult)
{
    CSocket * csock = dynamic_cast<CSocket *>(socket);
    if (!csock)
        THROWJSOCKEXCEPTION(JSOCKERR_not_opened);

    csock->finishAsyncConnect(connectResult);
}


void CSocket::udpconnect()
{
    DEFINE_SOCKADDR(u);
    socklen_t  ul = setSockAddr(u,targetip,hostport);
    sock = ::socket(u.sa.sa_family, SOCK_DGRAM, targetip.isIp4()?0:PF_INET6);
#ifdef SOCKTRACE
    PROGLOG("SOCKTRACE: udp connected socket %x %d (%p)", sock, sock, this);
#endif
    STATS.activesockets++;
    if (sock == INVALID_SOCKET) {
        THROWJSOCKTARGETEXCEPTION(SOCKETERRNO());
    }
    int res = ::connect(sock, &u.sa, ul);
    if (res != 0) { // works for UDP
        closesock();
        THROWJSOCKTARGETEXCEPTION(JSOCKERR_connection_failed);
    }
    set_nonblock(true);
    nagling = false; // means nothing for UDP
    state = ss_open;
#ifdef _TRACE
    setTraceName();
#endif

}



int CSocket::logPollError(unsigned revents, const char *rwstr)
{
    // return appropriate errno as caller will probably throw/log it
    int retcode = EINVAL;
    if (revents & POLLERR)
    {
        StringBuffer errStr;
        StringBuffer hostname;
        targetip.getHostText(hostname);
        errStr.appendf("%s POLLERR %u l:%d r:%s:%d", rwstr, sock, localPort, hostname.str(), hostport);
        int serror = 0;
        socklen_t serrlen = sizeof(serror);
        int srtn = getsockopt(sock, SOL_SOCKET, SO_ERROR, (char *)&serror, &serrlen);
        if (srtn != 0)
            serror = SOCKETERRNO();
        LOGERR2(serror,2,errStr.str());
        retcode = serror;
    }
    else if (revents & POLLNVAL)
    {
        // These are typically expected - when closing a socket in order to interrupt a thread that waits on it, for example
    }
    else
    {
        StringBuffer errStr;
        errStr.appendf("%s unknown poll() revents: 0x%x", rwstr, revents);
        LOGERR2(999,4,errStr.str());
    }
    return retcode;
}

int CSocket::wait_read(unsigned timeout)
{
    int ret = 0;
    int retrycount = 100;
    while (sock!=INVALID_SOCKET)
    {
#ifdef _USE_SELECT
        T_FD_SET fds;
        CHECKSOCKRANGE(sock);
        XFD_ZERO(&fds);
        FD_SET((unsigned)sock, &fds);
        if (timeout==WAIT_FOREVER)
        {
            ret = ::select( sock + 1, (fd_set *)&fds, NULL, NULL, NULL );
        }
        else
        {
            struct timeval tv;
            tv.tv_sec = timeout / 1000;
            tv.tv_usec = (timeout % 1000)*1000;
            ret = ::select( sock + 1, (fd_set *)&fds, NULL, NULL, &tv );
        }
#else
        struct pollfd fds[1];
        fds[0].fd = sock;
        fds[0].events = POLLINX;
        fds[0].revents = 0;
        ret = ::poll(fds, 1, timeout);
#endif
        if (ret == SOCKET_ERROR)
        {   // error
            int err = SOCKETERRNO();
            if (err!=JSE_INTR)
            {   // else retry (should adjust time but for our usage don't think it matters that much)
                LOGERR2(err,1,"wait_read");
                break;
            }
            else
            {
                if (retrycount-- <= 0)
                    break;
            }
        }
        else if (ret == 0)
        {   // timeout, no error (timeout can be 0 for a fast check)
            break;
        }
        else
        {   // ret > 0 - ready or error
            // if returning < 0 also set errno as caller will probably throw/log it
#ifdef _USE_SELECT
            if (!FD_ISSET(sock, &fds))
            {
                LOGERR2(998,7,"wait_read");
                errno = EBADF;
                ret = -1;
            }
#else
            if ( (fds[0].revents & (POLLERR | POLLNVAL)) || (!(fds[0].revents & (POLLINX | POLLHUP))) )
            {
                errno = logPollError(fds[0].revents, "wait_read");
                ret = -1;
            }
            else
            {
                // cannot error out on POLLRDHUP or POLLHUP as there can still be data to read
                // POLLIN | POLLRDHUP | POLLHUP ok
                break;
            }
#endif
            break;
        }
    }
    return ret;
}

int CSocket::wait_write(unsigned timeout)
{
    int ret = 0;
    while (sock!=INVALID_SOCKET) {
#ifdef _USE_SELECT
        T_FD_SET fds;
        CHECKSOCKRANGE(sock);
        XFD_ZERO(&fds);
        FD_SET((unsigned)sock, &fds);
        if (timeout==WAIT_FOREVER) {
            ret = ::select( sock + 1, NULL, (fd_set *)&fds, NULL, NULL );
        }
        else {
            struct timeval tv;
            tv.tv_sec = timeout / 1000;
            tv.tv_usec = (timeout % 1000)*1000;
            ret = ::select( sock + 1, NULL, (fd_set *)&fds, NULL, &tv );
        }
#else
        struct pollfd fds[1];
        fds[0].fd = sock;
        fds[0].events = POLLOUT;
        fds[0].revents = 0;
        ret = ::poll(fds, 1, timeout);
#endif
        if (ret==SOCKET_ERROR)
        {
            int err = SOCKETERRNO();
            if (err!=JSE_INTR)
            {   // else retry (should adjust time but for our usage don't think it matters that much)
                LOGERR2(err,1,"wait_write");
                break;
            }
        }
        else if (ret == 0)
        {   // timeout
            break;
        }
        else
        {   // ret > 0 - ready or error
            // if returning < 0 also set errno as caller will probably throw/log it
#ifdef _USE_SELECT
            if (!FD_ISSET(sock, &fds))
            {
                LOGERR2(998,7,"wait_write");
                errno = EBADF;
                ret = -1;
            }
#else
            if ( (fds[0].revents & (POLLERR | POLLNVAL)) || (!(fds[0].revents & (POLLOUT | POLLHUP))) )
            {
                errno = logPollError(fds[0].revents, "wait_write");
                ret = -1;
            }
            else if (fds[0].revents & POLLHUP)
            {
                LOGERR2(998,5,"wait_write POLLHUP");
                errno = EPIPE;
                ret = -1;
            }
            else
            {
                // POLLOUT ok
                break;
            }
#endif
            break;
        }
    }
    return ret;
}

void CSocket::readtms(void* buf, size32_t min_size, size32_t max_size, size32_t &sizeRead, unsigned timeoutMs, bool suppresGCIfMinSize)
{
    /*
     * Read at least min_size bytes, up to max_size bytes.
     * Reads as much as possible off socket until block detected.
     * NB: If min_size==0 then will return asap if no data is avail.
     * NB: If min_size==0, but notified of graceful close, throw graceful close exception.
     * NB: timeout is meaningless if min_size is 0
     *
     * NB: for UDP, it will never try to read more. It will call recvfrom once, if it gets less than min_size, it will throw an exception.
     * if blocks and received nothing (and min_size>0), poll and retry.
     *
     * NB: the underlying socket should be in non-blocking mode, if it is not it will not honour the timeout correctly.
     */

    sizeRead = 0;
    if (0 == max_size)
        return;
    if (state != ss_open)
        THROWJSOCKEXCEPTION(JSOCKERR_not_opened);

    CCycleTimer timer;
    while (true)
    {
        unsigned retrycount=100;
        int rc;
EintrRetry:
        if (sockmode==sm_udp_server) // udp server
        {
            DEFINE_SOCKADDR(u);
            socklen_t ul=sizeof(u);
            rc = recvfrom(sock, (char*)buf + sizeRead, max_size - sizeRead, 0, &u.sa,&ul);
            getSockAddrEndpoint(u,ul,returnep);
        }
        else
            rc = recv(sock, (char*)buf + sizeRead, max_size - sizeRead, 0);
        if (rc > 0)
        {
            sizeRead += rc;
            if (sockmode==sm_udp_server)
            {
                if (sizeRead >= min_size)
                    break;
                throwJSockException(JSOCKERR_small_udp_packet, "readtms: UDP packet smaller than min_size", __FILE__, __LINE__); // else, it is an error in UDP if receive anything less than min_size
            }
            else if (sizeRead == max_size)
                break;
            // NB: will exit when blocked if sizeRead >= min_size
        }
        else
        {
            if (rc < 0)
            {
                int err = SOCKETERRNO();
                if (BADSOCKERR(err))
                {
                    // don't think this should happen but convert to same as shutdown while investigation
                    LOGERR2(err,1,"Socket closed during read");
                    rc = 0;
                }
                else if ((err==JSE_INTR)&&(retrycount--!=0))
                {
                    if (sock==INVALID_SOCKET)
                        rc = 0;         // convert an EINTR after closed to a graceful close
                    else
                    {
                        LOGERR2(err,1,"EINTR retrying");
                        goto EintrRetry;
                    }
                }
                else
                {
                    if (err == JSE_WOULDBLOCK || err == JSE_EAGAIN) // if EAGAIN or EWOULDBLOCK - no more data to read
                    {
                        //NB: in UDP can only reach here if have not read anything so far.

                        if (sizeRead >= min_size)
                            break;
                        // otherwise, continue waiting for min_size
                    }
                    else
                    {
                        VStringBuffer errMsg("readtms(timeoutMs=%d)", timeoutMs);
                        LOGERR2(err,1,errMsg.str());
                        if ((err==JSE_CONNRESET)||(err==JSE_INTR)||(err==JSE_CONNABORTED))
                        {
                            errclose();
                            err = JSOCKERR_broken_pipe;
                        }
                        THROWJSOCKTARGETEXCEPTION(err);
                    }
                    // fall through to timeout/wait_read handling below.
                }
            }
            if (rc == 0)
            {
                state = ss_shutdown;
                if (suppresGCIfMinSize && (sizeRead >= min_size))
                    break;
                THROWJSOCKTARGETEXCEPTION(JSOCKERR_graceful_close);
            }
        }

        unsigned remainingMs = timer.remainingMs(timeoutMs);
        if (rc > 0)
        {
            if (0 == remainingMs)
            {
                if (sizeRead >= min_size)
                    break;
                THROWJSOCKTARGETEXCEPTION(JSOCKERR_timeout_expired);
            }

            // loop around to read more, or detect blocked.
        }
        else // NB rc < 0, (if rc == 0 handled already above)
        {
            // here because blocked (and sizeRead < min_size)
            rc = wait_read(remainingMs);
            if (rc < 0)
                THROWJSOCKTARGETEXCEPTION(SOCKETERRNO());
            else if (rc == 0)
                THROWJSOCKTARGETEXCEPTION(JSOCKERR_timeout_expired);
        }
        //else // read something, loop around to see if can read more, or detect blocked.
    }

    cycle_t elapsedCycles = timer.elapsedCycles();
    STATS.reads++;
    STATS.readsize += sizeRead;
    STATS.readtimecycles += elapsedCycles;
    stats.ioReads++;
    stats.ioReadBytes += sizeRead;
    stats.ioReadCycles += elapsedCycles;
}

void CSocket::read(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read, unsigned timeoutsecs, bool suppresGCIfMinSize)
{
    unsigned timeoutMs = (timeoutsecs==WAIT_FOREVER) ? WAIT_FOREVER : (timeoutsecs * 1000);
    readtms(buf, min_size, max_size, size_read, timeoutMs, suppresGCIfMinSize);
}

bool readtmsAllowClose(ISocket *sock, void* buf, size32_t min_size, size32_t max_size, size32_t &sizeRead, unsigned timeoutMs)
{
    try
    {
        sock->readtms(buf, min_size, max_size, sizeRead, timeoutMs, false);
    }
    catch(IJSOCK_Exception *e)
    {
        if (JSOCKERR_graceful_close != e->errorCode())
            throw;
        e->Release();
        return true;
    }
    return false;
}

void CSocket::read(void* buf, size32_t size)
{
    size32_t size_read;
    readtms(buf, size, size, size_read, WAIT_FOREVER);
}

size32_t CSocket::writetms(void const* buf, size32_t minSize, size32_t size, unsigned timeoutMs)
{
    if (size==0)
        return 0;
    size32_t sizeWritten = 0;
    CCycleTimer timer;
    if (state != ss_open)
        THROWJSOCKTARGETEXCEPTION(JSOCKERR_not_opened);

    while (true)
    {
        unsigned retrycount=100;
EintrRetry:
        int rc;
        if (sockmode==sm_udp_server) // udp server
        {
            DEFINE_SOCKADDR(u);
            socklen_t ul = setSockAddr(u,returnep,returnep.port);
            rc = sendto(sock, (char*)buf + sizeWritten, size - sizeWritten, 0, &u.sa, ul);
        }
#ifdef SIMULATE_LOST_UDP_PACKETS
        else if (sockmode==sm_udp && size <= 24 && dropCounter++ >= dropThreshold)
        {
            DBGLOG("Drop size %d cmd %d", size, *(unsigned short *)buf);
            dropCounter = 0;
            rc = size;
        }
#endif
        else
            rc = send(sock, (char*)buf + sizeWritten, size - sizeWritten, SEND_FLAGS);

        if (rc > 0)
        {
            sizeWritten += rc;
            if (sizeWritten == size)
                break;
            // NB: will exit when blocked if sizeWritten >= minSize
        }
        else if (rc < 0)
        {
            int err=SOCKETERRNO();
            if (BADSOCKERR(err))
            {
                LOGERR2(err,7,"Socket closed during write");
                rc = 0;
            }
            else if ((err==JSE_INTR)&&(retrycount--!=0)&&(timer.elapsedMs() < timeoutMs))
            {
                LOGERR2(err,7,"EINTR retrying");
                goto EintrRetry;
            }
            else
            {
                if (((sockmode==sm_multicast)||(sockmode==sm_udp))&&(err==JSE_CONNREFUSED))
                    break; // ignore
                if ((err==JSE_CONNRESET)||(err==JSE_INTR)||(err==JSE_CONNABORTED)
#ifndef _WIN32
                    ||(err==EPIPE)||(err==JSE_TIMEDOUT)  // linux can raise these on broken pipe
#endif
                    )
                {
                    errclose();
                    err = JSOCKERR_broken_pipe;
                }
                if (err == JSE_WOULDBLOCK || err == JSE_EAGAIN)
                {
                    if (sizeWritten >= minSize)
                        break;
                    unsigned remainingMs = timer.remainingMs(timeoutMs);
                    rc = wait_write(remainingMs);
                    if (rc < 0)
                        THROWJSOCKTARGETEXCEPTION(SOCKETERRNO());
                    else if (rc == 0)
                        THROWJSOCKTARGETEXCEPTION(JSOCKERR_timeout_expired);

                    goto EintrRetry;
                }
                THROWJSOCKTARGETEXCEPTION(err);
            }
        }
        if (rc == 0)
        {
            state = ss_shutdown;
            THROWJSOCKTARGETEXCEPTION(JSOCKERR_graceful_close);
        }
        if (timer.elapsedMs() >= timeoutMs)
        {
            IERRLOG("writetms timed out; timeout: %u, nwritten: %u, size: %u", timeoutMs, sizeWritten, size);
            THROWJSOCKTARGETEXCEPTION(JSOCKERR_timeout_expired);
        }
    }

    cycle_t elapsedCycles = timer.elapsedCycles();
    STATS.writes++;
    STATS.writesize += size;
    STATS.writetimecycles += elapsedCycles;
    stats.ioWrites++;
    stats.ioWriteBytes += size;
    stats.ioWriteCycles += elapsedCycles;
    return sizeWritten;
}

size32_t CSocket::write(void const* buf, size32_t size)
{
    return writetms(buf, size, size, WAIT_FOREVER);
}

bool CSocket::check_connection()
{
    if (state != ss_open) 
        return false;

    // This routine is for TCP stream sockets.
    // if another thread called sock->read() between the
    // wait_read() and recv() calls below then the
    // wait_read() might return EWOULDBLOCK and we would
    // assume then the socket is still connected and ok.

    // wait_read() returns = 0 - socket ok atm
    //             returns < 0 - error, assume closed
    //             returns > 0 - recv() to check for eof
    int rc = wait_read(0);
    if (rc == 0)
        return true;
    else if (rc < 0)
        return false;

    int retrycount = 100;
    char buffer;

    int recvFlags = MSG_PEEK;
#ifndef _WIN32
    recvFlags |= MSG_DONTWAIT;
#endif

EintrRecv:
    rc = recv(sock, &buffer, sizeof(buffer), recvFlags);

    // recv() returns = 0 - socket eof (closed)
    //        returns < 0 - and errno not EWOULDBLOCK/EAGAIN, assume closed
    //        returns > 0 - socket ok atm
    if (rc == 0)
        return false;
    else if (rc < 0)
    {
        int err=SOCKETERRNO();
        if ((err==JSE_INTR)&&(retrycount--!=0))
        {
            LOGERR2(err,7,"recv EINTR retrying");
            goto EintrRecv;
        }
        else if ((err==EWOULDBLOCK) || (err==EAGAIN))
            return true;
        else
            return false;
    }

    return true;
}

size32_t CSocket::udp_write_to(const SocketEndpoint &ep, void const* buf, size32_t size)
{
    if (size==0)
        return 0;
    cycle_t startcycles=get_cycles_now();
    if (state != ss_open) {
        THROWJSOCKTARGETEXCEPTION(JSOCKERR_not_opened);
    }
    size32_t res=0;
    DEFINE_SOCKADDR(u); 
    for (;;)
    {
        socklen_t  ul = setSockAddr(u,ep,ep.port);      
        int rc = sendto(sock, (char*)buf, size, 0, &u.sa, ul);
        if (rc < 0)
        {
            int err=SOCKETERRNO();
            if (((sockmode==sm_multicast)||(sockmode==sm_udp))&&(err==JSE_CONNREFUSED))
                break; // ignore
            if (err == JSE_WOULDBLOCK || err == JSE_EAGAIN)
            {
                int retCode = wait_write(WAIT_FOREVER);
                if (retCode < 0)
                    THROWJSOCKTARGETEXCEPTION(SOCKETERRNO());
                else if (retCode == 0)
                    THROWJSOCKTARGETEXCEPTION(JSOCKERR_timeout_expired);
            }
            else if (err!=JSE_INTR)
            {
                THROWJSOCKTARGETEXCEPTION(err);
            }
        }
        else
        {
            res = (size32_t)rc;
            break;
        }
    }

    cycle_t elapsedCycles = get_cycles_now()-startcycles;
    STATS.writes++;
    STATS.writesize += res;
    STATS.writetimecycles += elapsedCycles;
    stats.ioWrites++;
    stats.ioWriteBytes += res;
    stats.ioWriteCycles += elapsedCycles;
    return res;
}

size32_t CSocket::write_multiple(unsigned num,const void **buf, size32_t *size)
{
    assertex(sockmode!=sm_udp_server);
    if (num==1)
        return write(buf[0],size[0]);
    size32_t total = 0;
    unsigned i;
    for (i=0;i<num;i++)
        total += size[i];
    if (total==0)
        return 0;
    
    cycle_t startcycles=get_cycles_now();
    if (state != ss_open) {
        THROWJSOCKTARGETEXCEPTION(JSOCKERR_not_opened);
    }
    size32_t res=0;
#ifdef _WIN32
    WSABUF *bufs = (WSABUF *)alloca(sizeof(WSABUF)*num);
    for (i=0;i<num;i++) {
        bufs[i].buf = (char *)buf[i];
        bufs[i].len = size[i];
    }
    unsigned retrycount=100;
EintrRetry:
    DWORD sent;
    if (WSASendTo(sock,bufs,num,&sent,0,NULL,0,NULL,NULL)==SOCKET_ERROR) {
        int err=SOCKETERRNO();
        if (BADSOCKERR(err)) {
            LOGERR2(err,8,"Socket closed during write");
            sent = 0;
        }
        else if ((err==JSE_INTR)&&(retrycount--!=0)) {
            LOGERR2(err,8,"EINTR retrying");
            goto EintrRetry;
        }
        else {
            LOGERR2(err,8,"write_multiple");
            if ((err==JSE_CONNRESET)||(err==JSE_INTR)||(err==JSE_CONNABORTED)||(err==JSE_TIMEDOUT)) {
                errclose();
                err = JSOCKERR_broken_pipe;
            }
            THROWJSOCKTARGETEXCEPTION(err);
        }
    }   
    if (sent == 0) {
        state = ss_shutdown;
        THROWJSOCKTARGETEXCEPTION(JSOCKERR_graceful_close);
    }
    res = sent;
#else
#ifdef USE_CORK
    if (total>1024) {
        class Copt
        {
            T_SOCKET sock;
            bool nagling;
        public:
            Copt(T_SOCKET _sock,bool _nagling) 
            { 
                nagling = _nagling;
                int enabled = 1;
                int disabled = 0;
                if (!nagling) 
                    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (char*)&disabled, sizeof(disabled));
                setsockopt(sock, IPPROTO_TCP, TCP_CORK, (char*)&enabled, sizeof(enabled));
            }
            ~Copt() 
            { 
                int enabled = 1;
                int disabled = 0;
                setsockopt(sock, IPPROTO_TCP, TCP_CORK, (char*)&disabled, sizeof(disabled));
                if (!nagling) 
                    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (char*)&enabled, sizeof(enabled));
            }
        } copt(sock,nagling);
        for (i=0;i<num;i++) 
            res += write(buf[i],size[i]);
    }
    else {
        byte b[1024];
        byte *p=b;
        for (i=0;i<num;i++) {
            memcpy(p,buf[i],size[i]);
            p += size[i];
        }
        res = write(b,total);
    }
#else
    // send in equal chunks about 64K
    unsigned n = (total+0xffff)/0x10000;
    size32_t outbufsize = (total+n-1)/n;
    MemoryAttr ma;
    byte *outbuf = (byte *)ma.allocate(outbufsize);
    i = 0;
    size32_t os = 0;
    size32_t left = total;
    byte *b = NULL;
    size32_t s=0;
    for (;;) {
        while (!s&&(i<num)) {
            b = (byte *)buf[i];
            s = size[i];
            i++;
        }
        if ((os==0)&&(s==left)) {
            write(b,s);     // go for it
            break;
        }
        else {
            size32_t cpy = outbufsize-os;
            if (cpy>s)
                cpy = s;
            memcpy(outbuf+os,b,cpy);
            os += cpy;
            left -= cpy;
            s -= cpy;
            b += cpy;
            if (left==0)  {
                write(outbuf,os);       
                break;
            }
            else if (os==outbufsize) {
                write(outbuf,os);   
                os = 0;
            }
        }
    }
#endif
#endif

    cycle_t elapsedCycles = get_cycles_now()-startcycles;
    STATS.writes++;
    STATS.writesize += res;
    STATS.writetimecycles += elapsedCycles;
    stats.ioWrites++;
    stats.ioWriteBytes += res;
    stats.ioWriteCycles += elapsedCycles;
    return res;
}



bool CSocket::send_block(const void *blk,size32_t sz)
{
    unsigned startt=usTick();
#ifdef TRACE_SLOW_BLOCK_TRANSFER
    unsigned startt2 = startt;
    unsigned startt3 = startt;
#endif
    if (blockflags&BF_SYNC_TRANSFER_PULL) {
        size32_t rd;
        bool eof = true;
        readtms(&eof,sizeof(eof),sizeof(eof),rd,blocktimeoutms);
        if (eof)
            return false;
#ifdef TRACE_SLOW_BLOCK_TRANSFER
        startt2=usTick();
#endif
    }
    if (!blk||!sz) {
        sz = 0;
        write(&sz,sizeof(sz));
        try {
            bool reply;
            size32_t rd;
            readtms(&reply,sizeof(reply),sizeof(reply),rd,blocktimeoutms);
        }
        catch (IJSOCK_Exception *e) { 
            if ((e->errorCode()!=JSOCKERR_broken_pipe)&&(e->errorCode()!=JSOCKERR_graceful_close)) 
                EXCLOG(e,"CSocket::send_block");
            e->Release();                           
        }
        return false;
    }
    size32_t rsz=sz;
    _WINREV(rsz);
    write(&rsz,sizeof(rsz));
    if (blockflags&BF_SYNC_TRANSFER_PUSH) {
#ifdef TRACE_SLOW_BLOCK_TRANSFER
        startt2=usTick();
#endif
        size32_t rd;
        bool eof = true;
        readtms(&eof,sizeof(eof),sizeof(eof),rd,blocktimeoutms);
        if (eof)
            return false;
#ifdef TRACE_SLOW_BLOCK_TRANSFER
        startt3=usTick();
#endif
    }
    write(blk,sz);
    if (blockflags&BF_RELIABLE_TRANSFER) {
        bool isok=false;
        size32_t rd;
        readtms(&isok,sizeof(isok),sizeof(isok),rd,blocktimeoutms);
        if (!isok)
            return false;
    }
    unsigned nowt = usTick();
    unsigned elapsed = nowt-startt;
    STATS.blocksendtime+=elapsed;
    STATS.numblocksends++;
    STATS.blocksendsize+=sz;
    if (elapsed>STATS.longestblocksend) {
        STATS.longestblocksend = elapsed;
        STATS.longestblocksize = sz;
    }
#ifdef TRACE_SLOW_BLOCK_TRANSFER
    if (elapsed>1000000*60)  // over 1min
        IWARNLOG("send_block took %ds to %s  (%d,%d,%d)",elapsed/1000000,tracename,startt2-startt,startt3-startt2,nowt-startt3);
#endif
    return true;
}

size32_t CSocket::receive_block_size()
{
    // assumed always paired with receive_block
    if (nextblocksize) {
        if (blockflags&BF_SYNC_TRANSFER_PULL) {
            bool eof=false;
            write(&eof,sizeof(eof));
        }
        size32_t rd;
        readtms(&nextblocksize,sizeof(nextblocksize),sizeof(nextblocksize),rd,blocktimeoutms);
        _WINREV(nextblocksize);
        if (nextblocksize==0) { // confirm eof
            try {
                bool confirm=true;
                write(&confirm,sizeof(confirm));
            }
            catch (IJSOCK_Exception *e) { 
                if ((e->errorCode()!=JSOCKERR_broken_pipe)&&(e->errorCode()!=JSOCKERR_graceful_close)) 
                    EXCLOG(e,"receive_block_size");
                e->Release();                           
            }
        }
        else if (blockflags&BF_SYNC_TRANSFER_PUSH) {  // leaves receiveblocksem clear
#ifdef USERECVSEM
            CSemProtect semprot; // this will catch exception in write
            while (!semprot.wait(&receiveblocksem,&receiveblocksemowned,60*1000*5))
                IWARNLOG("Receive block stalled");
#endif
            bool eof=false;
            write(&eof,sizeof(eof));
#ifdef USERECVSEM
            semprot.clear();
#endif
        }

    }
    return nextblocksize;
}

    
size32_t CSocket::receive_block(void *blk,size32_t maxsize)
{
#ifdef USERECVSEM
    CSemProtect semprot; // this will catch exceptions
#endif
    size32_t sz = nextblocksize;
    if (sz) {
        if (sz==UINT_MAX) { // need to get size
            if (!blk||!maxsize) {
                if (blockflags&BF_SYNC_TRANSFER_PUSH) { // ignore block size
                    size32_t rd;
                    readtms(&nextblocksize,sizeof(nextblocksize),sizeof(nextblocksize),rd,blocktimeoutms);
                }
                if (blockflags&(BF_SYNC_TRANSFER_PULL|BF_SYNC_TRANSFER_PUSH)) { // signal eof
                    bool eof=true;
                    write(&eof,sizeof(eof));
                    nextblocksize = 0;
                    return 0;
                }
            }
            sz = receive_block_size();
            if (!sz) 
                return 0;
        }
        unsigned startt=usTick();   // include sem block but not initial handshake
#ifdef USERECVSEM
        if (blockflags&BF_SYNC_TRANSFER_PUSH)  // read_block_size sets semaphore
            semprot.set(&receiveblocksem,&receiveblocksemowned);  // this will reset semaphore on exit
#endif
        nextblocksize = UINT_MAX;
        size32_t rd;
        if (sz<=maxsize) {
            readtms(blk,sz,sz,rd,blocktimeoutms);
        }
        else { // truncate
            readtms(blk,maxsize,maxsize,rd,blocktimeoutms);
            sz -= maxsize;
            OwnedMalloc<void> tmp = malloc(sz);
            readtms(tmp,sz,sz,rd,blocktimeoutms);
            sz = maxsize;
        }
        if (blockflags&BF_RELIABLE_TRANSFER) {
            bool isok=true;
            write(&isok,sizeof(isok));
        }
        unsigned elapsed = usTick()-startt;
        STATS.blockrecvtime+=elapsed;
        STATS.numblockrecvs++;
        STATS.blockrecvsize+=sz;
    }
    return sz;
}

void CSocket::set_block_mode(unsigned flags, size32_t recsize, unsigned _timeoutms)
{
    blockflags = flags;
    nextblocksize = UINT_MAX;
    blocktimeoutms = _timeoutms?_timeoutms:WAIT_FOREVER;
}



void CSocket::shutdown(unsigned mode)
{
    if (state == ss_open) {
        state = ss_shutdown;
#ifdef SOCKTRACE
        PROGLOG("SOCKTRACE: shutdown(%d) socket %x %d (%p)", mode, sock, sock, this);
#endif
        int rc = ::shutdown(sock, mode);
        if (rc != 0) {
            int err=SOCKETERRNO();
            if (err==JSE_NOTCONN) {
#ifdef _TRACELINKCLOSED
                DBGLOG("CSocket::shutdown(%d) failed, socket: %d", mode, sock);
#endif
                LOGERR2(err,9,"shutdown");
                err = JSOCKERR_broken_pipe;
            }
            THROWJSOCKTARGETEXCEPTION(err);
        }
    }
}

void CSocket::shutdownNoThrow(unsigned mode)
{
    if (state == ss_open)
    {
        state = ss_shutdown;
#ifdef SOCKTRACE
        PROGLOG("SOCKTRACE: shutdown(%d) socket %x %d (%p)", mode, sock, sock, this);
#endif
        ::shutdown(sock, mode);
    }
}

void CSocket::errclose()
{
#ifdef USERECVSEM
    if (receiveblocksemowned) {
        receiveblocksemowned = false;
        receiveblocksem.signal();
    }
#endif
    if (state != ss_close) {
        state = ss_close;
#ifdef SOCKTRACE
        PROGLOG("SOCKTRACE: errclose socket %x %d (%p)", sock, sock, this);
#endif
        if (mcastreq)
            setsockopt(sock, IPPROTO_IP, IP_DROP_MEMBERSHIP,(char*)mcastreq,sizeof(*mcastreq));
        closesock();
    }
}


void CSocket::close()
{
#ifdef USERECVSEM
    if (receiveblocksemowned) {
        receiveblocksemowned = false;
        receiveblocksem.signal();
    }
#endif
    if (state != ss_close) {
#ifdef SOCKTRACE
        PROGLOG("SOCKTRACE: close socket %x %d (%p)", sock, sock, this);
#endif
        state = ss_close;
        if (mcastreq)
            setsockopt(sock, IPPROTO_IP, IP_DROP_MEMBERSHIP,(char*)mcastreq,sizeof(*mcastreq));
        if (closesock() != 0) {
            THROWJSOCKTARGETEXCEPTION(SOCKETERRNO());
        }
    }
}

size32_t CSocket::get_max_send_size()
{
    size32_t maxsend=0;
    socklen_t size = sizeof(maxsend);
#if _WIN32
    getsockopt(sock, SOL_SOCKET, SO_MAX_MSG_SIZE, (char *) &maxsend, &size);
#else
    getsockopt(sock, SOL_SOCKET, SO_SNDBUF, (char *) &maxsend, &size);  // not the same but closest I can find
#endif
  return maxsend;
}


size32_t CSocket::get_send_buffer_size()
{
    size32_t maxsend=0;
    socklen_t size = sizeof(maxsend);
    getsockopt(sock, SOL_SOCKET, SO_SNDBUF, (char *) &maxsend, &size);  
    return maxsend;
}


void CSocket::set_send_buffer_size(size32_t maxsend)
{
    if (setsockopt(sock, SOL_SOCKET, SO_SNDBUF, (char *)&maxsend, sizeof(maxsend))!=0) {
        LOGERR2(SOCKETERRNO(),1,"setsockopt(SO_SNDBUF)");
    }
#ifdef CHECKBUFSIZE
    size32_t v;
    if (getsockopt(sock, SOL_SOCKET, SO_SNDBUF, (char *)&v, sizeof(v))!=0) {
        LOGERR2(SOCKETERRNO(),1,"getsockopt(SO_SNDBUF)");
    }
    if (v!=maxsend) 
        IWARNLOG("set_send_buffer_size requested %d, got %d",maxsend,v);
#endif
}


size32_t CSocket::get_receive_buffer_size()
{
    size32_t max=0;
    socklen_t size = sizeof(max);
    getsockopt(sock, SOL_SOCKET, SO_RCVBUF, (char *) &max, &size);  
    return max;
}


void CSocket::set_receive_buffer_size(size32_t max)
{
    if (setsockopt(sock, SOL_SOCKET, SO_RCVBUF, (char *)&max, sizeof(max))!=0) {
        LOGERR2(SOCKETERRNO(),1,"setsockopt(SO_RCVBUF)");
    }
#ifdef CHECKBUFSIZE
    size32_t v;
    if (getsockopt(sock, SOL_SOCKET, SO_RCVBUF, (char *)&v, sizeof(v))!=0) {
        LOGERR2(SOCKETERRNO(),1,"getsockopt(SO_RCVBUF)");
    }
    if (v<max) 
        IWARNLOG("set_receive_buffer_size requested %d, got %d",max,v);
#endif
}


bool CSocket::join_multicast_group(SocketEndpoint &ep) 
{
    StringBuffer s;
    ep.getHostText(s);    // will improve later
    MCASTREQ req(s.str());
    if (setsockopt(sock, IPPROTO_IP, IP_ADD_MEMBERSHIP,(char*)&req, sizeof(req))!=0) {
        return false;
    }
    return true;
}


bool CSocket::leave_multicast_group(SocketEndpoint &ep) 
{
    StringBuffer s;
    ep.getHostText(s);    // will improve later
    MCASTREQ req(s.str());
    if (setsockopt(sock, IPPROTO_IP, IP_DROP_MEMBERSHIP,(char*)&req, sizeof(req))!=0) {
        return false;
    }
    return true;
}


void CSocket::set_ttl(unsigned _ttl)
{
    if (_ttl)
    {
        u_char ttl = _ttl;
        setsockopt(sock, IPPROTO_IP, IP_MULTICAST_TTL, (char *)&ttl, sizeof(ttl));
    }
#ifdef SOCKTRACE
    int ttl0 = 0;
    socklen_t ttl1 = sizeof(ttl0);
    getsockopt(sock, IPPROTO_IP, IP_MULTICAST_TTL, &ttl0, &ttl1);
    DBGLOG("set_ttl: socket fd: %d requested ttl: %d actual ttl: %d", sock, _ttl, ttl0);
#endif
    return;
}

void CSocket::logConnectionInfo(unsigned timeoutms, unsigned conn_mstime)
{
    StringBuffer hostname;
    targetip.getHostText(hostname);
    PROGLOG("SOCKTRACE: connect(%u) - time:%u ms fd:%d l:%d r:%s:%d", timeoutms, conn_mstime, sock, localPort, hostname.str(), hostport);
    // PrintStackReport();
}

bool CSocket::isSecure() const
{
    return false;
}

unsigned __int64 CSocket::getStatistic(StatisticKind kind) const
{
    return stats.getStatistic(kind);
}

CSocket::~CSocket()
{
    if (owned)
    {
        try
        {
            close();
        }
        catch (IException *e)
        {
            EXCLOG(e,"~CSocket close()");
            e->Release();
        }
    }
#ifdef _TRACE
    free(tracename);
    tracename = NULL;
#endif
    delete mcastreq;
}

CSocket::CSocket(const SocketEndpoint &ep,SOCKETMODE smode,const char *name)
{
    state = ss_close;
    nonblocking = false;
#ifdef USERECVSEM
    receiveblocksemowned = false;
#endif
    nagling = true; // until turned off
    hostport = ep.port;
    targetip.ipset(ep);
    localPort = 0;
    mcastreq = NULL;
#ifdef _TRACE
    tracename = NULL;
#endif
    StringBuffer tmp;
    if ((smode==sm_multicast_server)&&(name&&*name)) {
        mcastreq = new MCASTREQ(name);
    }
    else {
        //MORE: I don't think the name parameter is needed anymore
        if (name && ep.isNull())
            targetip.ipset(name);
    }
    sock = INVALID_SOCKET;
    sockmode = smode;
    owned = true;
    nextblocksize = 0;
    in_accept = false;
    accept_cancel_state = accept_not_cancelled;
#ifdef _TRACE
    if (name)
        setTraceName("T>", name);
    else
    {
        StringBuffer hostname;
        targetip.getHostText(hostname);
        setTraceName("S>", hostname.str());
    }
#endif
}

CSocket::CSocket(T_SOCKET new_sock,SOCKETMODE smode,bool _owned,SocketEndpoint *_peerEp)
{
    nonblocking = false;
#ifdef USERECVSEM
    receiveblocksemowned = false;
#endif
    nagling = true; // until turned off
    sock = new_sock;
    if (new_sock!=INVALID_SOCKET)
        STATS.activesockets++;
    mcastreq = NULL;
#ifdef _TRACE
    tracename = NULL;
#endif
    state = ss_open;
    sockmode = smode;
    owned = _owned;
    nextblocksize = 0;
    in_accept = false;
    accept_cancel_state = accept_not_cancelled;
    set_nagle(false);
    //set_linger(DEFAULT_LINGER_TIME); -- experiment with removing this as closesocket should still endevour to send outstanding data
    if (_peerEp)
    {
        targetip = *_peerEp;
        hostport = _peerEp->port;
    }
    else
    {
        char peer[256];
        hostport = peer_name(peer,sizeof(peer));
        targetip.ipset(peer);
    }
    SocketEndpoint ep;
    localPort = getEndpoint(ep).port;
#ifdef _TRACE
    StringBuffer tmp;
    targetip.getIpText(tmp);
    tmp.append(":").append(hostport);
    setTraceName("A!", tmp);
#endif
}

ISocket* ISocket::create(unsigned short p,int listen_queue_size)
{
    if (p==0)
        THROWJSOCKEXCEPTION(JSOCKERR_bad_address);
    SocketEndpoint ep;
    ep.port = p;
    Owned<CSocket> sock = new CSocket(ep,sm_tcp_server,NULL);
    sock->open(listen_queue_size);
    return sock.getClear();
}


ISocket* ISocket::create_ip(unsigned short p,const char *host,int listen_queue_size)
{
    if (p==0)
        THROWJSOCKEXCEPTION(JSOCKERR_bad_address);
    SocketEndpoint ep(host,p);
    Owned<CSocket> sock = new CSocket(ep,sm_tcp_server,host);
    sock->open(listen_queue_size);
    return sock.getClear();
}


ISocket* ISocket::udp_create(unsigned short p)
{
    SocketEndpoint ep;
    ep.port=p;
    Owned<CSocket> sock = new CSocket(ep,(p==0)?sm_udp:sm_udp_server,NULL);
    sock->open(0);
    return sock.getClear();
}

ISocket* ISocket::multicast_create(unsigned short p, const char *mcip, unsigned _ttl)
{
    if (p==0)
        THROWJSOCKEXCEPTION(JSOCKERR_bad_address);
    SocketEndpoint ep(mcip,p);
    Owned<CSocket> sock = new CSocket(ep,sm_multicast_server,mcip);
    sock->open(0,true);
    if (_ttl)
        sock->set_ttl(_ttl);
    return sock.getClear();
}

ISocket* ISocket::multicast_create(unsigned short p, const IpAddress &ip, unsigned _ttl)
{
    if (p==0)
        THROWJSOCKEXCEPTION(JSOCKERR_bad_address);
    SocketEndpoint ep(p, ip);
    StringBuffer tmp;
    Owned<CSocket> sock = new CSocket(ep,sm_multicast_server,ip.getHostText(tmp).str());
    sock->open(0,true);
    if (_ttl)
        sock->set_ttl(_ttl);
    return sock.getClear();
}

ISocket* ISocket::udp_connect(unsigned short p, char const* name)
{
    if (!name||!*name||(p==0))
        THROWJSOCKEXCEPTION(JSOCKERR_bad_address);
    SocketEndpoint ep(name, p);
    Owned<CSocket> sock = new CSocket(ep,sm_udp,name);
    sock->udpconnect();
    return sock.getClear();
}

ISocket* ISocket::udp_connect(const SocketEndpoint &ep)
{
    Owned<CSocket> sock = new CSocket(ep,sm_udp,NULL);
    sock->udpconnect();
    return sock.getClear();
}

ISocket* ISocket::multicast_connect(unsigned short p, char const* mcip, unsigned _ttl)
{
    if (p==0)
        THROWJSOCKEXCEPTION(JSOCKERR_bad_address);
    SocketEndpoint ep(mcip,p);
    return multicast_connect(ep, _ttl);
}

ISocket* ISocket::multicast_connect(const SocketEndpoint &ep, unsigned _ttl)
{
    Owned<CSocket> sock = new CSocket(ep,sm_multicast,NULL);
    sock->udpconnect();
    if (_ttl)
        sock->set_ttl(_ttl);
    return sock.getClear();
}

ISocket* ISocket::attach(int s, bool tcpip)
{
    CSocket* sock = new CSocket((SOCKET)s, tcpip?sm_tcp:sm_udp, false, nullptr);
    sock->set_nonblock(true);
    return sock;
}

bool isInterfaceIp(const IpAddress &ip, const char *ifname)
{
#ifdef _WIN32
    return false;
#else
    int fd = socket(AF_INET, SOCK_DGRAM, 0);  // IPV6 TBD
    if (fd<0)
        return false;
    MemoryAttr ma;
    char *buf = (char *)ma.allocate(1024); 
    struct ifconf ifc;
    ifc.ifc_len = 1024;
    ifc.ifc_buf = buf;
    if(ioctl(fd, SIOCGIFCONF, &ifc) < 0) // query interfaces
    {
        close(fd);
        return false;
    }
    bool match = false;
    struct ifreq *ifr = ifc.ifc_req;
    int len = 0;
    for (int i=0; i<ifc.ifc_len;
#ifdef __APPLE__
         len = _SIZEOF_ADDR_IFREQ(*ifr),
#else
         len = sizeof(struct ifreq),
#endif
         ifr = (struct ifreq *) ((char *) ifr + len),
         i += len
        )
    {
        struct ifreq *item = ifr;
        if (ifname&&*ifname)
            if (!WildMatch(item->ifr_name,ifname))
                continue;
        IpAddress iptest((inet_ntoa(((struct sockaddr_in *)&item->ifr_addr)->sin_addr)));
        if (ip.ipequals(iptest))
        {
            match = true;
            break;
        }
    }
    close(fd);
    return match;
#endif
}

NO_SANITIZE("alignment") bool getInterfaceIp(IpAddress &ip,const char *ifname)
{
#if defined(_WIN32)
    return false;
#else
    static bool recursioncheck = false;
    ip.ipset(NULL);
    int fd = socket(AF_INET, SOCK_DGRAM, 0);  // IPV6 TBD
    if (fd<0)
        return false;
    MemoryAttr ma;
    char *buf = (char *)ma.allocate(1024);
    struct ifconf ifc;
    ifc.ifc_len = 1024;
    ifc.ifc_buf = buf;
    if(ioctl(fd, SIOCGIFCONF, &ifc) < 0) // query interfaces
    {
        close(fd);
        return false;
    }
    for (int loopback = 0; loopback <= 1; loopback++)
    {
        struct ifreq *ifr = ifc.ifc_req;
        int len = 0;
        for (int i=0; i<ifc.ifc_len;
#ifdef __APPLE__
             len = _SIZEOF_ADDR_IFREQ(*ifr),
#else
             len = sizeof(struct ifreq),
#endif
             ifr = (struct ifreq *) ((char *) ifr + len),
             i += len
            )
        {
            bool useLoopback = (loopback==1);
            struct ifreq *item = ifr;
            if (ifname && *ifname)
                if (!WildMatch(item->ifr_name,ifname))
                    continue;
#ifdef __APPLE__
            if (item->ifr_addr.sa_family != AF_INET) // ipv6 support TBD
                continue;
            char host[128];
            getnameinfo(&item->ifr_addr, sizeof(item->ifr_addr), host, sizeof(host), 0, 0, NI_NUMERICHOST);
            IpAddress iptest(host);
#else
            IpAddress iptest((inet_ntoa(((struct sockaddr_in *)&item->ifr_addr)->sin_addr)));
#endif
            struct ifreq flags;
            memset(&flags, 0, sizeof(flags));
            strcpy(flags.ifr_name, item->ifr_name);
            if (ioctl(fd, SIOCGIFFLAGS, &flags) < 0)
            {
                if (!recursioncheck) {
                    recursioncheck = true;
                    IERRLOG("Error retrieving interface flags for interface %s", item->ifr_name);
                    recursioncheck = false;
                }
                continue;
            }
            bool isLoopback = iptest.isLoopBack() || ((flags.ifr_flags & IFF_LOOPBACK) != 0);
            bool isUp = (flags.ifr_flags & IFF_UP) != 0;
            if ((isLoopback==useLoopback) && isUp)
            {
                if (ip.isNull())
                    ip.ipset(iptest);
                else if (!PreferredSubnet.isNull()&&!PreferredSubnet.test(ip)&&PreferredSubnet.test(iptest))
                    ip.ipset(iptest);
            }
        }
        if (!ip.isNull())
            break;
    }
    close(fd);
    return !ip.isNull();
#endif
}


static StringAttr cachehostname;
static IpAddress cachehostip;
static IpAddress localhostip;
static CriticalSection hostnamesect;

const char * GetCachedHostName()
{
    CriticalBlock c(hostnamesect);
    if (!cachehostname.get())
    {
#ifndef _WIN32
#ifdef _CONTAINERIZED
        //MORE: Does this need to be implemented a different way?
#else
        IpAddress ip;
        const char *ifs = queryEnvironmentConf().queryProp("interface");
        if (getInterfaceIp(ip, ifs))
        {
            StringBuffer ips;
            ip.getHostText(ips);
            if (ips.length())
            {
                cachehostname.set(ips.str());
                cachehostip.ipset(ip);
                return cachehostname.get();
            }
        }
#endif
#endif
        char temp[1024];
        if (gethostname(temp, sizeof(temp))==0)
            cachehostname.set(temp);                
        else
            cachehostname.set("localhost");         // assume no NIC card
    }
    return cachehostname.get();
}


IpAddress & queryLocalIP()
{
    CriticalBlock c(hostnamesect);
    if (localhostip.isNull())
    {
        if (IP6preferred)
            localhostip.ipset("::1");   //IPv6 
        else
            localhostip.ipset("127.0.0.1"); //IPv4
    }
    return localhostip;
}

IpAddress & queryHostIP()
{
    CriticalBlock c(hostnamesect);
    if (cachehostip.isNull()) {
        if (!cachehostip.ipset(GetCachedHostName())) {
            cachehostip.ipset(queryLocalIP());          
            // printf("hostname %s not resolved, using localhost\n",GetCachedHostName()); // don't use jlog in case recursive
        }
    }
    return cachehostip;
}

IpAddress &GetHostIp(IpAddress &ip)
{
    ip.ipset(queryHostIP());
    return ip;
}

IpAddress &localHostToNIC(IpAddress &ip)
{
    if (ip.isLoopBack())
        GetHostIp(ip);
    return ip;
}

// IpAddress

bool getInterfaceName(StringBuffer &ifname)
{
#if defined(_WIN32)
    return false;
#else
    IpAddress myIp;
    GetHostIp(myIp);

    int fd = socket(AF_INET, SOCK_DGRAM, 0);  // IPV6 TBD
    if (fd<0)
        return false;

    MemoryAttr ma;
    char *buf = (char *)ma.allocate(1024);

    struct ifconf ifc;
    ifc.ifc_len = 1024;
    ifc.ifc_buf = buf;
    if(ioctl(fd, SIOCGIFCONF, &ifc) < 0) // query interfaces
    {
        close(fd);
        return false;
    }

    struct ifreq *ifr = ifc.ifc_req;
    int len = 0;
    for (int i=0; i<ifc.ifc_len;
#ifdef __APPLE__
         len = _SIZEOF_ADDR_IFREQ(*ifr),
#else
         len = sizeof(struct ifreq),
#endif
         ifr = (struct ifreq *) ((char *) ifr + len),
         i += len
        )
    {
        struct ifreq *item = ifr;
        IpAddress iptest((inet_ntoa(((struct sockaddr_in *)&item->ifr_addr)->sin_addr)));
        if (iptest.ipequals(myIp))
        {
            ifname.set(item->ifr_name);
            close(fd);
            return true;
        }
    }
    close(fd);
    return false;
#endif
}

inline bool isIp4(const unsigned *netaddr)
{
    if (IP4only)
        return true;
    if (netaddr[2]==0xffff0000)
        return (netaddr[1]==0)&&(netaddr[0]==0);
    if (netaddr[2]==0)
        if ((netaddr[3]==0)&&(netaddr[0]==0)&&(netaddr[1]==0))
            return true; // null address
    // maybe should get loopback here
    return false;
}

void IpAddress::setIP4(unsigned ip)
{
    hostname.clear();
    netaddr[0] = 0;
    netaddr[1] = 0;
    if (ip)
        netaddr[2] = 0xffff0000;
    else
        netaddr[2] = 0;
    netaddr[3] = ip;
}

unsigned IpAddress::getIP4() const
{
    assertex(isIp4());
    return netaddr[3];
}

bool IpAddress::isIp4() const
{
    return ::isIp4(netaddr);
}

bool IpAddress::isNull() const
{
    return (netaddr[3]==0)&&(IP4only||((netaddr[2]==0)&&(netaddr[1]==0)&&(netaddr[0]==0)));
}

bool IpAddress::isLoopBack() const
{
    if (::isIp4(netaddr)&&((netaddr[3] & 0x000000ff)==0x000007f))
        return true;
    return (netaddr[3]==0x1000000)&&(netaddr[2]==0)&&(netaddr[1]==0)&&(netaddr[0]==0);
}

bool IpAddress::isLocal() const 
{ 
    if (isLoopBack() || isHost())
        return true;
    IpAddress ip(*this);
    return isInterfaceIp(ip, NULL);
}

bool IpAddress::ipequals(const IpAddress & other) const
{
    // reverse compare for speed
    return (other.netaddr[3]==netaddr[3])&&(IP4only||((other.netaddr[2]==netaddr[2])&&(other.netaddr[1]==netaddr[1])&&(other.netaddr[0]==netaddr[0])));
}

int  IpAddress::ipcompare(const IpAddress & other) const
{
    return memcmp(&netaddr, &other.netaddr, sizeof(netaddr));
}

unsigned IpAddress::iphash(unsigned prev) const
{
    return hashc((const byte *)&netaddr,sizeof(netaddr),prev);
}

unsigned IpAddress::fasthash() const
{
    if (IP4only || isIp4())
        return netaddr[3] >> 24;
    else
        return iphash(0) >> 24;
}

bool IpAddress::isHost() const
{
    return ipequals(queryHostIP());
}

static bool decodeNumericIP(const char *text,unsigned *netaddr)
{
    if (!text)
        return false;
    bool isv6 = strchr(text,':')!=NULL;
    StringBuffer tmp;
    if ((*text=='[')&&!IP4only) {
        text++;
        size32_t l = strlen(text);
        if ((l<=2)||(text[l-1]!=']'))
            return false;
        text = tmp.append(l-2,text);
    }
    if (!isv6&&isdigit(text[0])) {
        if (_inet_pton(AF_INET, text, &netaddr[3])>0) {
            netaddr[2] = netaddr[3]?0xffff0000:0;  // check for NULL
            netaddr[1] = 0;
            netaddr[0] = 0;         // special handling for loopback?
            return true;
        }
    }
    else if (isv6&&!IP4only) {
        int ret = _inet_pton(AF_INET6, text, netaddr);
        if (ret>=0)
            return (ret>0);
        int err = SOCKETERRNO();
        StringBuffer tmp("_inet_pton: ");
        tmp.append(text);
        LOGERR(err,1,tmp.str());
    }   
    return false;
}

static void RecursionSafeLogErr(int ret, int ref, const char *msg, unsigned lineno, const char *name)
{
    static __thread bool recursioncheck = false; // needed to stop error message recursing
    if (!recursioncheck)
    {
        recursioncheck = true;
        LogErr(ret, ref, msg, lineno, name);
#ifdef _DEBUG
        PrintStackReport();
#endif
        recursioncheck = false;
    }
}

int getAddressInfo(const char *name, unsigned *netaddr, bool okToLogErr)
{
    struct addrinfo hints;
    struct addrinfo *addrInfo = NULL;
    int retry=10;
    int retCode;

    // each retry could take up to several seconds, depending on how DNS resolver is configured
    // should a few specific non-zero return codes break out early from retry loop (EAI_NONAME) ?
    while (true)
    {
        memset(&hints, 0, sizeof(hints));
        // dont wait for both A and AAAA records ...
        if (IP4only || (!IP6preferred))
            hints.ai_family = AF_INET;
        // hints.ai_flags = AI_CANONNAME | AI_ADDRCONFIG | AI_V4MAPPED
        retCode = getaddrinfo(name, NULL, &hints, &addrInfo);
        if (0 == retCode)
            break;
        if (--retry > 0)
            Sleep((10-retry)*100);
        else
        {
            // use gai_strerror(ret) to get meaningful error text ?
            if (okToLogErr)
                RecursionSafeLogErr(retCode, 1, "getaddrinfo failed", __LINE__, name);
            return retCode;
        }
        if (EAI_NONAME == retCode)
        {
            // try one more time, but why ?
            // MCK TODO: probably should only retry on EAI_AGAIN and possibly EAI_SYSTEM ...
            retry = 1;
        }
    }

    struct addrinfo *best = NULL;
    bool snm = !PreferredSubnet.isNull();
    for (;;) {
        struct addrinfo  *ai;
        for (ai = addrInfo; ai; ai = ai->ai_next) {
//          printf("flags=%d, family=%d, socktype=%d, protocol=%d, addrlen=%d, canonname=%s\n",ai->ai_flags,ai->ai_family,ai->ai_socktype,ai->ai_protocol,ai->ai_addrlen,ai->ai_canonname?ai->ai_canonname:"NULL");
            switch (ai->ai_family) {
            case AF_INET: {
                    if (snm) {
                        IpAddress ip;
                        ip.setNetAddress(sizeof(in_addr),&(((sockaddr_in *)ai->ai_addr)->sin_addr));
                        if (!PreferredSubnet.test(ip))
                            continue;
                    }
                    if ((best==NULL)||((best->ai_family==AF_INET6)&&!IP6preferred))
                        best = ai;
                    break;
                }
            case AF_INET6: {
                    if (snm) {
                        IpAddress ip;
                        ip.setNetAddress(sizeof(in_addr6),&(((sockaddr_in6 *)ai->ai_addr)->sin6_addr));
                        if (!PreferredSubnet.test(ip))
                            continue;
                    }
                    if ((best==NULL)||((best->ai_family==AF_INET)&&IP6preferred))
                        best = ai;
                    break;
                }
            }
        }
        if (best||!snm)
            break;
        snm = false;
    }
    if (best) {
        if (best->ai_family==AF_INET6)
            memcpy(netaddr,&(((sockaddr_in6 *)best->ai_addr)->sin6_addr),sizeof(in6_addr));
        else {
            memcpy(netaddr+3,&(((sockaddr_in *)best->ai_addr)->sin_addr),sizeof(in_addr));
            netaddr[2] = 0xffff0000;
            netaddr[1] = 0;
            netaddr[0] = 0;
        }
    }
    freeaddrinfo(addrInfo);
    if (best!=NULL)
        return 0;
    else
        return EAI_NONAME; // or EAI_NODATA ?
}

static bool lookupHostAddress(const char *name, unsigned *netaddr, unsigned timeoutms=INFINITE)
{
    // if IP4only or using MS V6 can only resolve IPv4 using 
    int retry=10;

#if defined(__linux__) || defined (__APPLE__) || defined(getaddrinfo)
    if (IP4only) {
#else
    {
#endif
        CriticalBlock c(hostnamesect);
        hostent * entry = gethostbyname(name);
        while (entry==NULL) {
            if (retry--==0) {
                RecursionSafeLogErr(h_errno, 1, "gethostbyname failed", __LINE__, name);
                return false;
            }
            {
                CriticalUnblock ub(hostnamesect);
                Sleep((10-retry)*100);
            }
            entry = gethostbyname(name);
        }
        if (entry->h_addr_list[0]) {
            unsigned ptr = 0;
            if (!PreferredSubnet.isNull()) {
                for (;;) {
                    ptr++;
                    if (entry->h_addr_list[ptr]==NULL) {
                        ptr = 0;
                        break;
                    }
                    IpAddress ip;
                    ip.setNetAddress(sizeof(unsigned),entry->h_addr_list[ptr]);
                    if (PreferredSubnet.test(ip))
                        break;
                }
            }
            memcpy(&netaddr[3], entry->h_addr_list[ptr], sizeof(netaddr[3]));
            netaddr[2] = 0xffff0000;
            netaddr[1] = 0;
            netaddr[0] = 0;
            return true;
        }
        return false;
    }

#if defined(__linux__) || defined (__APPLE__) || defined(getaddrinfo)
    int retCode = 0;
    if ( (timeoutms != INFINITE) && (useDNSTimeout()) ) // could addrInfoPool be NULL ?
    {
        // getaddrinfo_a() offers an async getaddrinfo method, but has some limitations and a possible mem leak
        // could implement timeout getaddrinfo functionality without threads by connecting with DNS servers and parsing response ...
        // lookup performance may be significantly improved if system uses a DNS resolver cache

        PooledThreadHandle thrdHandle;
        Owned<CAddrInfoThreadArgs> addrThreadInfo = new CAddrInfoThreadArgs(name);
        CTimeMon dnstimeout(timeoutms);

        try
        {
            // wait up to timeoutms for an available thread from pool ...
            thrdHandle = addrInfoPool->start((void *)addrThreadInfo.getLink(), "getaddrinfo-thread", timeoutms);
            // MCK TODO: if have to create a new thread and at os/system/shell thread limit, create/start can take up to several seconds, can we control this ?
        }
        catch (IException *e)
        {
            addrThreadInfo->Release();
            StringBuffer excMsg;
            StringBuffer emsg;
            emsg.appendf("getaddrinfo failed (thread) (%d) (exc:%d) %s", timeoutms, e->errorCode(), e->errorMessage(excMsg).str());
            RecursionSafeLogErr(100, 1, emsg.str(), __LINE__, name);
            e->Release();
            return false;
        }
        catch (...)
        {
            addrThreadInfo->Release();
            RecursionSafeLogErr(101, 1, "getaddrinfo failed (thread) (other exc)", __LINE__, name);
            return false;
        }

        // take into account time already passed creating/starting thread above ...
        unsigned remaining;
        dnstimeout.timedout(&remaining);
        if (addrInfoPool->join(thrdHandle, remaining))
        {
            if (0 == addrThreadInfo->retCode)
            {
                memcpy(netaddr, addrThreadInfo->netaddr, sizeof(addrThreadInfo->netaddr));
                return true;
            }
            else
            {
                // use gai_strerror(ret) to get meaningful error text ?
                StringBuffer emsg;
                emsg.appendf("getaddrinfo failed (thread)");
                RecursionSafeLogErr(addrThreadInfo->retCode.load(), 1, emsg.str(), __LINE__, name);
                return false;
            }
        }
        // if join() returns false thread still running, but its detached,
        // will terminate, release thread args and return to pool on its own ...

        StringBuffer emsg;
        emsg.appendf("getaddrinfo timed out (thread) (%d)", timeoutms);
        RecursionSafeLogErr(EAI_AGAIN, 1, emsg.str(), __LINE__, name);
        return false;
    }

    retCode = getAddressInfo(name, netaddr, true);
    if (0 == retCode)
        return true;
    else
        return false;
#endif
}

bool IpAddress::ipset(const char *text, unsigned timeoutms)
{
    if (text&&*text)
    {
        if ((text[0]=='.')&&(text[1]==0))
        {
            ipset(queryHostIP());
            return true;
        }
        if (decodeNumericIP(text,netaddr))
        {
            hostname.clear();
            return true;
        }
        const char *s;
        for (s=text;*s;s++)
            if (!isdigit(*s)&&(*s!=':')&&(*s!='.')) 
                break;
        if (!*s)
            return ipset(NULL);
        if (lookupHostAddress(text, netaddr, timeoutms))
        {
            hostname.set(text);
            return true;
        }
    }
    hostname.clear();
    memset(&netaddr,0,sizeof(netaddr));
    return false;
}

inline char * addbyte(char *s,byte b)
{
    if (b>=100) {
        *(s++) = b/100+'0';
        b %= 100;
        *(s++) = b/10+'0';
        b %= 10;
    }
    else if (b>=10) {
        *(s++) = b/10+'0';
        b %= 10;
    }
    *(s++) = b+'0';
    return s;
}

StringBuffer & IpAddress::getIpText(StringBuffer & out) const
{
    if (::isIp4(netaddr)) {
        const byte *ip = (const byte *)&netaddr[3];
        char ips[16];
        char *s = ips;
        for (unsigned i=0;i<4;i++) {
            if (i)
                *(s++) = '.';
            s = addbyte(s,ip[i]);
        }
        return out.append(s-ips,ips);
    }
    char tmp[INET6_ADDRSTRLEN];
    const char *res = _inet_ntop(AF_INET6, &netaddr, tmp, sizeof(tmp));
    if (!res) 
        throw makeOsException(errno);
    return out.append(res);
}

StringBuffer & IpAddress::getHostText(StringBuffer & out) const
{
    if (hostname)
        return out.append(hostname);
    return getIpText(out);
}

void IpAddress::ipserialize(MemoryBuffer & out) const
{
    if (((netaddr[2]==0xffff0000)||(netaddr[2]==0))&&(netaddr[1]==0)&&(netaddr[0]==0)) {
        if (netaddr[3]==IPV6_SERIALIZE_PREFIX)
            throw MakeStringException(-1,"Invalid network address"); // hack prevention
        out.append(sizeof(netaddr[3]), &netaddr[3]); 
    }
    else {
        unsigned pfx = IPV6_SERIALIZE_PREFIX;
        out.append(sizeof(pfx),&pfx).append(sizeof(netaddr),&netaddr);
    }
}

void IpAddress::ipdeserialize(MemoryBuffer & in)
{
    hostname.clear();
    unsigned pfx;
    in.read(sizeof(pfx),&pfx);
    if (pfx!=IPV6_SERIALIZE_PREFIX) {
        netaddr[0] = 0;
        netaddr[1] = 0;
        netaddr[2] = (pfx == 0 || pfx == 0x1000000) ? 0 : 0xffff0000; // catch null and loopback
        netaddr[3] = pfx;
    }
    else 
        in.read(sizeof(netaddr),&netaddr);
}


unsigned IpAddress::ipdistance(const IpAddress &other,unsigned offset) const
{   
    if (offset>3)
        offset = 3;
    unsigned i1;
    _cpyrev4(&i1,&netaddr[3-offset]);
    unsigned i2;
    _cpyrev4(&i2,&other.netaddr[3-offset]);
    return (i1 > i2) ? (i1 - i2) : (i2 - i1);
}

bool IpAddress::ipincrement(unsigned count,byte minoctet,byte maxoctet,unsigned short minipv6piece,unsigned maxipv6piece)
{
    unsigned base;
    if (::isIp4(netaddr)) {
        base = maxoctet-minoctet+1;
        if (!base||(base>256))
            return false;
        byte * ips = (byte *)&netaddr[3];
        byte * ip = ips+4;
        while (count) {
            if (ip==ips)
                return false;   // overflow
            ip--;
            unsigned v = (count+((*ip>minoctet)?(*ip-minoctet):0));
            *ip = minoctet + v%base;
            count = v/base;
        }
    }
    else {
        base = maxipv6piece-minipv6piece+1;
        if (!base||(base>0x10000))
            return false;
        unsigned short * ps = (unsigned short *)&netaddr;
        unsigned short * p = ps+8;
        while (count) {
            if (p==ps)
                return false;   // overflow (actually near impossible!)
            p--;
            unsigned v = (count+((*p>minipv6piece)?(*p-minipv6piece):0));
            *p = minipv6piece + v%base;
            count = v/base;
        }
    }
    hostname.clear(); // Probably should never be set for an IpAddress where ipincrement is used
    return true;
}

unsigned IpAddress::ipsetrange( const char *text)  // e.g. 10.173.72.1-65  ('-' may be omitted)
{
    unsigned e=0;
    unsigned f=0;
    const char *r = strchr(text,'-');   
    bool ok;
    if (r) {
        e = atoi(r+1);
        StringBuffer tmp(r-text,text);
        ok = ipset(tmp.str());
        if (!::isIp4(netaddr))
            IPV6_NOT_IMPLEMENTED();              // TBD IPv6
        if (ok) {
            while ((r!=text)&&(*(r-1)!='.'))
                r--;
            f = (r!=text)?atoi(r):0;    
        }
    }
    else
        ok = ipset(text);
    if ((f>e)||!ok) 
        return 0;
    return e-f+1;
}


NO_SANITIZE("alignment") size32_t IpAddress::getNetAddress(size32_t maxsz,void *dst) const
{
    if (maxsz==sizeof(unsigned)) {
        if (::isIp4(netaddr)) {
            *(unsigned *)dst = netaddr[3];
            return maxsz;
        }
    }
    else if (!IP4only&&(maxsz==sizeof(netaddr))) {
        memcpy(dst,&netaddr,maxsz);
        return maxsz;
    }
    return 0;
}

NO_SANITIZE("alignment") void IpAddress::setNetAddress(size32_t sz,const void *src)
{
    hostname.clear();
    if (sz==sizeof(unsigned)) { // IPv4
        netaddr[0] = 0;
        netaddr[1] = 0;
        netaddr[3] = *(const unsigned *)src;
        netaddr[2] = netaddr[3] ? 0xffff0000 : 0; // leave as null if Ipv4 address is null
    }
    else if (!IP4only&&(sz==sizeof(netaddr))) { // IPv6
        memcpy(&netaddr,src,sz);
        if ((netaddr[2]==0)&&(netaddr[3]!=0)&&(netaddr[3]!=0x1000000)&&(netaddr[0]==0)&&(netaddr[1]==0))
            netaddr[2]=0xffff0000;  // use this form only
    }
    else
        memset(&netaddr,0,sizeof(netaddr));
}


void SocketEndpoint::deserialize(MemoryBuffer & in)
{
    ipdeserialize(in);
    in.read(port);
}

void SocketEndpoint::serialize(MemoryBuffer & out) const
{
    ipserialize(out);
    out.append(port);
}


bool SocketEndpoint::set(const char *name,unsigned short _port, unsigned timeoutms)
{ 
    if (name) {
        if (*name=='[') { 
            const char *s = name+1;
            const char *t = strchr(s,']');
            if (t) {
                StringBuffer tmp(t-s,s);
                if (t[1]==':') 
                    _port = atoi(t+2);
                return set(tmp.str(),_port);
            }
        }
        const char * colon = strchr(name, ':');
        if (colon) {
            if (!IP4only&&strchr(colon+1, ':'))
                colon = NULL; // hello its IpV6
        }
        else
            colon = strchr(name, '|'); // strange hole convention
        char ips[260];
        if (colon) {
            size32_t l = colon-name;
            if (l>=sizeof(ips))
                l = sizeof(ips)-1;
            memcpy(ips,name,l);
            ips[l] = 0;
            name = ips;
            _port = atoi(colon+1);
        }
        if (ipset(name, timeoutms)) {
            port = _port; 
            return true;
        }
    }
    ipset(NULL);
    port = 0;   
    return false;
}

StringBuffer &SocketEndpoint::getEndpointIpText(StringBuffer &str) const
{
    getIpText(str);
    if (port)
        str.append(':').append((unsigned)port);         // TBD IPv6 put [] on
    return str;
}

StringBuffer &SocketEndpoint::getEndpointHostText(StringBuffer &str) const
{
    getHostText(str);
    if (port)
        str.append(':').append((unsigned)port);         // TBD IPv6 put [] on
    return str;
}

void SocketEndpoint::getEndpointHostText(char * str, size32_t len) const
{
    if (len==0)
        return;
    StringBuffer _str;
    getEndpointHostText(_str);
    size32_t l = _str.length()+1;
    if (l>len)
    { 
        l = len-1;
        str[l] = 0;
    }
    memcpy(str,_str.str(),l);
}

unsigned SocketEndpoint::hash(unsigned prev) const
{
    return hashc((const byte *)&port,sizeof(port),iphash(prev));
}


StringBuffer &getRemoteAccessibleHostText(StringBuffer &str, const SocketEndpoint &ep)
{
    if (isContainerized())
        return ep.getEndpointIpText(str);
    else
        return ep.getEndpointHostText(str);
}



//---------------------------------------------------------------------------

SocketListCreator::SocketListCreator()
{
    lastPort = 0;
}

void SocketListCreator::addSocket(const SocketEndpoint &ep)
{
    StringBuffer ipstr;
    ep.getHostText(ipstr);
    addSocket(ipstr.str(), ep.port);
}

void SocketListCreator::addSocket(const char * ip, unsigned port)
{
    if (fullText.length())
        fullText.append("|");

    const char * prev = lastIp;
    const char * startCopy = ip;
    if (prev)
    {
        if (strcmp(ip, prev) == 0)
        {
            fullText.append("=");
            startCopy = NULL;
        }
        else
        {
            const char * cur = ip;
            for (;;)
            {
                char n = *cur;
                if (!n)
                    break;
                if (n != *prev)
                    break;
                cur++; 
                prev++;
                if (n == '.')
                    startCopy = cur;
            }
            if (startCopy != ip)
                fullText.append("*");
        }
    }

    fullText.append(startCopy);
    if (lastPort != port)
        fullText.append(":").append(port);

    lastIp.set(ip);
    lastPort = port;
}

const char * SocketListCreator::getText()
{
    return fullText.str();
}

void SocketListCreator::addSockets(SocketEndpointArray &array)
{
    ForEachItemIn(i,array) {
        const SocketEndpoint &sockep=array.item(i);
        StringBuffer ipstr;
        sockep.getHostText(ipstr);
        addSocket(ipstr.str(),sockep.port);
    }
}


//---------------------------------------------------------------------------

SocketListParser::SocketListParser(const char * text)
{
    fullText.set(text);
    cursor = NULL;
    lastPort = 0;
}

void SocketListParser::first(unsigned port)
{
    cursor = fullText;
    lastIp.set(NULL);
    lastPort = port;
}

bool SocketListParser::get(StringAttr & ip, unsigned & port, unsigned index, unsigned defport)
{
    first(defport);
    do
    {
        if (!next(ip, port))
            return false;
    } while (index--);
    return true;
}

bool SocketListParser::next(StringAttr & ip, unsigned & port)
{
    // IPV6TBD

    StringBuffer ipText;
    if (*cursor == 0)
        return false;
    if (*cursor == '=')
    {
        ipText.append(lastIp);
        cursor++;
    }
    else if (*cursor == '*')
    {
        cursor++;

        //count the number of dots in the tail
        const char * cur = cursor;
        unsigned count = 0;
        for (;;)
        {
            char c = *cur++;
            switch (c)
            {
            case 0:
            case '|':
            case ',':
            case ':':
                goto done;
            case '.':
                ++count;
                break;
            }
        }
done:
        //copy up to the appropriate dot from the previous ip.
        const unsigned dotCount = 3;        //more what about 6 digit ip's
        cur = lastIp;
        for (;;)
        {
            char c = *cur++;
            switch (c)
            {
            case 0:
            case '|':
            case ',':
            case ':':
                assertex(!"Should not get here!");
                goto done2;
            case '.':
                ipText.append(c);
                if (++count == dotCount)
                    goto done2;
                break;
            default:
                ipText.append(c);
                break;
            }
        }
done2:;
    }

    bool inPort = false;
    port = lastPort;
    for (;;)
    {
        char c = *cursor++;
        switch (c)
        {
        case 0:
            cursor--;
            goto doneCopy;
        case '|':
        case ',':
            goto doneCopy;
        case ':':
            port = atoi(cursor);
            inPort = true;
            break;;
        default:
            if (!inPort)
                ipText.append(c);
            break;
        }
    }

doneCopy:
    lastIp.set(ipText.str());
    ip.set(lastIp);
    lastPort = port;
    return true;
}

unsigned SocketListParser::getSockets(SocketEndpointArray &array,unsigned defport)
{
    first(defport);
    StringAttr ip;
    unsigned port;
    while (next(ip,port)) {
        SocketEndpoint ep(ip,port);
        array.append(ep);
    }
    return array.ordinality();
}

jlib_decl JSocketStatistics *getSocketStatPtr()
{
    return &STATS;
}

void getSocketStatistics(JSocketStatistics &stats)
{
    // should put in simple lock
    memcpy(&stats,&STATS,sizeof(stats));
}

void resetSocketStatistics()
{
    unsigned activesockets=STATS.activesockets;
    memset(&STATS,0,sizeof(STATS));
    STATS.activesockets = activesockets;
}

static StringBuffer &appendtime(StringBuffer &s,unsigned us)
{
    // attemp to get into more sensible units
    if (us>10000000)
        return s.append(us/1000000).append('s');
    if (us>10000)
        return s.append(us/1000).append("ms");
    return s.append(us).append("us");
}


StringBuffer &getSocketStatisticsString(JSocketStatistics &stats,StringBuffer &str)
{

    str.append("connects=").append(stats.connects).append('\n');
    appendtime(str.append("connecttime="),stats.connecttime).append('\n');
    str.append("failedconnects=").append(stats.failedconnects).append('\n');
    appendtime(str.append("failedconnecttime="),stats.failedconnecttime).append('\n');
    str.append("reads=").append(stats.reads).append('\n');
    appendtime(str.append("readtime="),cycle_to_microsec(stats.readtimecycles)).append('\n');
    str.append("readsize=").append(stats.readsize).append(" bytes\n");
    str.append("writes=").append(stats.writes).append('\n');
    appendtime(str.append("writetime="),cycle_to_microsec(stats.writetimecycles)).append('\n');
    str.append("writesize=").append(stats.writesize).append(" bytes").append('\n');
    str.append("activesockets=").append(stats.activesockets).append('\n');
    str.append("numblockrecvs=").append(stats.numblockrecvs).append('\n');
    str.append("numblocksends=").append(stats.numblocksends).append('\n');
    str.append("blockrecvsize=").append(stats.blockrecvsize).append('\n');
    str.append("blocksendsize=").append(stats.blocksendsize).append('\n');
    str.append("blockrecvtime=").append(stats.blockrecvtime).append('\n');
    str.append("blocksendtime=").append(stats.blocksendtime).append('\n');
    str.append("longestblocksend=").append(stats.longestblocksend).append('\n');
    str.append("longestblocksize=").append(stats.longestblocksize);
    return str;
}

// ===============================================================================
// select thread for handling multiple selects

struct SelectItem
{
    ISocket *sock;
    T_SOCKET handle;
    ISocketSelectNotify *nfy;
    byte mode;
    bool del;
    bool operator == (const SelectItem & other) const { return sock == other.sock; }
};

class SelectItemArray : public StructArrayOf<SelectItem> { };
class SelectItemArrayP : public StructArrayOf<SelectItem*> { };

#define SELECT_TIMEOUT_SECS 1           // but it does (TBD)


#ifdef _WIN32

// fd_set utility functions
inline T_FD_SET *cpyfds(T_FD_SET &dst,const T_FD_SET &src)
{
    unsigned i = src.fd_count;
    dst.fd_count = i;
    while (i--)
        dst.fd_array[i] = src.fd_array[i]; // possibly better as memcpy
    return &dst;
}

inline bool findfds(T_FD_SET &s,T_SOCKET h,bool &c) 
{
    unsigned n = s.fd_count;
    unsigned i;
    for(i=0;i<n;i++) {
        if (s.fd_array[i] == h) {
            if (--n)
                s.fd_array[i] = s.fd_array[n]; // remove item
            else
                c = false;
            s.fd_count = n;
            return true;
        }
    }
    return false;
}

inline T_SOCKET popfds(T_FD_SET &s)
{
    unsigned n = s.fd_count;
    T_SOCKET ret;
    if (n) {
        ret = s.fd_array[--n];
        s.fd_count = n;
    }
    else 
        ret = NULL;
    return ret;
}

#else
#define _USE_PIPE_FOR_SELECT_TRIGGER


// not as optimized as windows but I am expecting to convert to using poll anyway
inline T_FD_SET *cpyfds(T_FD_SET &dst,const T_FD_SET &src)
{
    memcpy(&dst,&src,sizeof(T_FD_SET));
    return &dst;
}

inline bool findfds(T_FD_SET &s,T_SOCKET h,bool &c) 
{
    if ((unsigned)h>=XFD_SETSIZE)
        return false;
    return FD_ISSET(h,&s); // does not remove entry or set termination flag when done
}
#endif

class CSocketBaseThread: public Thread
{
protected:
    bool terminating;
    CriticalSection sect;
    Semaphore ticksem;
    std::atomic_uint tickwait;
    unsigned offset;
    std::atomic<bool> selectvarschange;
    unsigned waitingchange;
    Semaphore waitingchangesem;
    int validateselecterror;
    unsigned validateerrcount;
    const char *selecttrace;
#ifdef _USE_PIPE_FOR_SELECT_TRIGGER
    T_SOCKET dummysock[2];
#else
    T_SOCKET dummysock; 
#endif
    bool dummysockopen;

    CSocketBaseThread(const char *trc) : Thread("CSocketBaseThread"), tickwait(0)
    {
    }

    ~CSocketBaseThread()
    {
    }

public:
    void triggerselect()
    {
        if (tickwait)
            ticksem.signal();
#ifdef _USE_PIPE_FOR_SELECT_TRIGGER
        CriticalBlock block(sect);
        char c = 0;
        if(write(dummysock[1], &c, 1) != 1) {
            int err = SOCKETERRNO();
            LOGERR(err,1,"Socket closed during trigger select");
        }
#else
        closedummy();
#endif
    }

    void resettrigger()
    {
#ifdef _USE_PIPE_FOR_SELECT_TRIGGER
        CriticalBlock block(sect);
        char c;
        while((::read(dummysock[0], &c, sizeof(c))) == sizeof(c));
#endif
    }

    void stop(bool wait)
    {
        terminating = true;
        triggerselect();
        if (wait)
            join();
    }


    bool sockOk(T_SOCKET sock)
    {
#ifdef _DEBUG
        PROGLOG("CSocketBaseThread: sockOk testing %d",sock);
#endif
        int t=0;
        socklen_t tl = sizeof(t);
        if (getsockopt(sock, SOL_SOCKET, SO_TYPE, (char *)&t, &tl)!=0) {
            StringBuffer sockstr;
            const char *tracename = sockstr.append((unsigned)sock).str();
            LOGERR2(SOCKETERRNO(),1,"CSocketBaseThread select handle");
            return false;
        }
#ifdef _USE_SELECT
        T_FD_SET fds;
        struct timeval tv;
        CHECKSOCKRANGE(sock);
        XFD_ZERO(&fds);
        FD_SET((unsigned)sock, &fds);
        //FD_SET((unsigned)sock, &except);
        tv.tv_sec = 0;
        tv.tv_usec = 0;
        int rc = ::select( sock + 1, NULL, (fd_set *)&fds, NULL, &tv );
        if (rc<0) {
            StringBuffer sockstr;
            const char *tracename = sockstr.append((unsigned)sock).str();
            LOGERR2(SOCKETERRNO(),2,"CSocketBaseThread select handle");
            return false;
        }
# ifdef _DEBUG
        else if (rc>0)
            PROGLOG("CSocketBaseThread: select handle %d selected(2) %d",sock,rc);
# endif
        XFD_ZERO(&fds);
        FD_SET((unsigned)sock, &fds);
        tv.tv_sec = 0;
        tv.tv_usec = 0;
        rc = ::select( sock + 1, (fd_set *)&fds, NULL, NULL, &tv );
        if (rc<0) {
            StringBuffer sockstr;
            const char *tracename = sockstr.append((unsigned)sock).str();
            LOGERR2(SOCKETERRNO(),3,"CSocketBaseThread select handle");
            return false;
        }
# ifdef _DEBUG
        else if (rc>0)
            PROGLOG("CSocketBaseThread: select handle %d selected(2) %d",sock,rc);
# endif
        return true;
#else
        struct pollfd fds[1];
        fds[0].fd = sock;
        fds[0].events = (POLLINX | POLLOUT);
        fds[0].revents = 0;
        int rc = ::poll(fds, 1, 0);
        if (rc==0)
            return true;
        else if (rc>0)
        {
            if ( !(fds[0].revents & POLLNVAL) ) // MCK - skip POLLERR here
            {
                // PROGLOG("CSocketBaseThread: poll handle %d selected(2) %d",sock,rc);
                return true;
            }
        }
        else
        {
            int err = SOCKETERRNO();
            if (err == JSE_INTR)
                return true; // assume ok until next time called
        }
        StringBuffer sockstr;
        const char *tracename = sockstr.append((unsigned)sock).str();
        LOGERR2(SOCKETERRNO(),3,"CSocketBaseThread poll handle");
        return false;
#endif
    }

    virtual void closedummy() = 0;
};

class CSocketSelectThread: public CSocketBaseThread
{
    SelectItemArray items;
    unsigned basesize;

    void opendummy()
    {
        CriticalBlock block(sect);
        if (!dummysockopen) { 
#ifdef _USE_PIPE_FOR_SELECT_TRIGGER
            if(pipe(dummysock)) {
                IWARNLOG("CSocketSelectThread: create pipe failed %d",SOCKETERRNO());
                return;
            }
            for (unsigned i=0;i<2;i++) {
                int flags = fcntl(dummysock[i], F_GETFL, 0);
                if (flags!=-1) {
                    flags |= O_NONBLOCK;
                    fcntl(dummysock[i], F_SETFL, flags);
                }
                flags = fcntl(dummysock[i], F_GETFD, 0);
                if (flags!=-1) {
                    flags |=  FD_CLOEXEC;
                    fcntl(dummysock[i], F_SETFD, flags);
                }
            }
            CHECKSOCKRANGE(dummysock[0]);
#else
            if (IP6preferred)
                dummysock = ::socket(AF_INET6, SOCK_STREAM, PF_INET6);
            else
                dummysock = ::socket(AF_INET, SOCK_STREAM, 0);
            CHECKSOCKRANGE(dummysock);
#endif
            dummysockopen = true;
        }


    }

    void closedummy()
    {
        CriticalBlock block(sect);
        if (dummysockopen) { 
#ifdef _USE_PIPE_FOR_SELECT_TRIGGER
#ifdef SOCKTRACE
            PROGLOG("SOCKTRACE: Closing dummy sockets %x %d %x %d (%p)", dummysock[0], dummysock[0], dummysock[1], dummysock[1], this);
#endif
            ::close(dummysock[0]);
            ::close(dummysock[1]);
#else
#ifdef _WIN32
            ::closesocket(dummysock);
#else
            ::close(dummysock);
#endif
#endif
            dummysockopen = false;
        }
    }


#ifdef _WIN32
#define HASHTABSIZE 256
#define HASHNULL (HASHTABSIZE-1)
#define HASHTABMASK (HASHTABSIZE-1)
    byte hashtab[HASHTABSIZE];
#define HASHSOCKET(s) ((((unsigned)s)>>2)&HASHTABMASK)      // with some knowledge of windows handles

    void inithash()
    {
        memset(&hashtab,HASHNULL,sizeof(hashtab));
        assertex(FD_SETSIZE<255);
    }

    void reinithash()
    { // done this way because index of items changes and hash table not that big
        inithash();
        assertex(items.ordinality()<HASHTABSIZE-1);
        ForEachItemIn(i,items) {
            unsigned h = HASHSOCKET(items.item(i).handle);
            for (;;) {
                if (hashtab[h]==HASHNULL) {
                    hashtab[h] = (byte)i;
                    break;
                }
                if (++h==HASHTABSIZE)
                    h = 0;
            }
        }
    }

    inline SelectItem &findhash(T_SOCKET handle)
    {
        unsigned h = HASHSOCKET(handle);
        unsigned sh = h;
        for (;;) {
            SelectItem &i=items.element(hashtab[h]);
            if (i.handle==handle) 
                return i;
            if (++h==HASHTABSIZE)
                h = 0;
            assertex(h!=sh);
        }
    }

    inline void processfds(T_FD_SET &s,byte mode,SelectItemArray &tonotify)
    {
        for (;;) {
            T_SOCKET sock = popfds(s);
            if (!sock)
                break;
            if (sock!=dummysock)
            {
                SelectItem &si = findhash(sock);
                if (!si.del)
                {
                    tonotify.append(si); // nb copies
                    SelectItem &itm = tonotify.element(tonotify.length()-1);
                    itm.nfy->Link();
                    itm.sock->Link();
                    itm.mode = mode;
                }
            }
        }   
    }

#endif 
public:
    CSocketSelectThread(const char *trc)
        : CSocketBaseThread("CSocketSelectThread")
    {
        dummysockopen = false;
        opendummy();
        terminating = false;
        waitingchange = 0;
        selectvarschange = false;
        validateselecterror = 0;
        validateerrcount = 0;
        offset = 0;
        selecttrace = trc;
        basesize = 0;
#ifdef _WIN32
        inithash();
#endif      
    }

    ~CSocketSelectThread()
    {
        closedummy();
        ForEachItemIn(i,items) {
            // Release/dtors should not throw but leaving try/catch here until all paths checked
            try {
                SelectItem &si = items.element(i);
                si.nfy->Release();
                si.sock->Release();
            }
            catch (IException *e) {
                EXCLOG(e,"~CSocketSelectThread");
                e->Release();
            }
        }
    }

    Owned<IException> termexcept;

    // JCSMORE: this is very similar to the version in CSocketEpollThread. Could be commoned if items used common structure.
    void updateItems()
    {
        // must be in CriticalBlock block(sect);
        unsigned n = items.ordinality();
#ifdef _WIN32
        bool hashupdateneeded = (n!=basesize); // additions all come at end
#endif
        for (unsigned i=0;i<n;) {
            SelectItem &si = items.element(i);
            if (si.del) {
                // Release/dtors should not throw but leaving try/catch here until all paths checked
                try {
#ifdef SOCKTRACE
                    PROGLOG("CSocketSelectThread::updateItems release %d",si.handle);
#endif
                    si.nfy->Release();
                    si.sock->Release();
                }
                catch (IException *e) {
                    EXCLOG(e,"CSocketSelectThread::updateItems");
                    e->Release();
                }
                // NOTE: si is a reference, put last item into remove slot
                n--;
                if (i<n) 
                    si = items.item(n);
                items.remove(n);
#ifdef _WIN32
                hashupdateneeded = true;
#endif
            }
            else
                i++;
        }
        assertex(n<=XFD_SETSIZE-1);
#ifdef _WIN32
        if (hashupdateneeded)
            reinithash();
#endif
        basesize = n;
    }

    // JCSMORE: this is very similar to the version in CSocketEpollThread. Could be commoned if items used common structure.
    bool checkSocks()
    {
        bool ret = false;
        ForEachItemIn(i,items) {
            SelectItem &si = items.element(i);
            if (si.del)
                ret = true; // maybe that bad one
            else  if (!sockOk(si.handle)) {
                si.del = true;
                ret = true;
            }
        }
        return ret;
    }

    bool remove(ISocket *sock)
    {
        if (terminating)
            return false;
        CriticalBlock block(sect);
        if (sock==NULL) { // wait until no changes outstanding
            while (selectvarschange) {
                waitingchange++;
                CriticalUnblock unblock(sect);
                waitingchangesem.wait();
            }
            return true;
        }
        ForEachItemIn(i,items) {
            SelectItem &si = items.element(i);
            if (!si.del&&(si.sock==sock)) {
                si.del = true;
                selectvarschange = true;
                triggerselect();
                return true;
            }
        }
        return false;
    }

    bool add(ISocket *sock,unsigned mode,ISocketSelectNotify *nfy)
    {
        // maybe check once to prevent 1st delay? TBD
        CriticalBlock block(sect);
        unsigned n=0;
        ForEachItemIn(i,items) {
            SelectItem &si = items.element(i);
            if (!si.del)
            {
                if (si.sock==sock)
                {
                    si.del = true;
                    break;
                }
                else 
                    n++;
            }
        }
        if (n>=XFD_SETSIZE-1)   // leave 1 spare
            return false;
        SelectItem sn;
        sn.nfy = LINK(nfy);
        sn.sock = LINK(sock);
        sn.mode = (byte)mode;
        sn.handle = (T_SOCKET)sock->OShandle();
        CHECKSOCKRANGE(sn.handle);
        sn.del = false;
        items.append(sn);
        selectvarschange = true;        
        triggerselect();
        return true;
    }

    void updateSelectVars(T_FD_SET &rdfds,T_FD_SET &wrfds,T_FD_SET &exfds,bool &isrd,bool &iswr,bool &isex,unsigned &ni,T_SOCKET &max_sockid)
    {
        CriticalBlock block(sect);
        selectvarschange = false;
        if (waitingchange) {
            waitingchangesem.signal(waitingchange);
            waitingchange = 0;
        }
        if (validateselecterror) { // something went wrong so check sockets
            validateerrcount++;
            if (!checkSocks()) {
                // bad socket not found
                PROGLOG("CSocketSelectThread::updateSelectVars cannot find socket error");
                if (validateerrcount>10)
                    throw MakeStringException(-1,"CSocketSelectThread:Socket select error %d",validateselecterror);
            }
        }
        else
            validateerrcount = 0;
        updateItems();
        XFD_ZERO( &rdfds );
        XFD_ZERO( &wrfds );
        XFD_ZERO( &exfds );
        isrd=false;
        iswr=false;
        isex=false;
#ifdef _USE_PIPE_FOR_SELECT_TRIGGER
        max_sockid=dummysockopen?dummysock[0]:0;
#else
        opendummy();
        max_sockid=dummysockopen?dummysock:0;
#endif
        ni = items.ordinality();
#ifdef _WIN32
        if (offset>=ni)
#endif
            offset = 0;
        unsigned j = offset;
        ForEachItemIn(i, items) {
            SelectItem &si = items.element(j);
            j++;
            if (j==ni)
                j = 0;
            if (si.mode & SELECTMODE_READ) {
                FD_SET( si.handle, &rdfds );
                isrd = true;
            }
            if (si.mode & SELECTMODE_WRITE) {
                FD_SET( si.handle, &wrfds );
                iswr = true;
            }
            if (si.mode & SELECTMODE_EXCEPT) {
                FD_SET( si.handle, &exfds );
                isex = true;
            }
            max_sockid=std::max(si.handle, max_sockid);
        }
        if (dummysockopen) {
#ifdef _USE_PIPE_FOR_SELECT_TRIGGER
            FD_SET( dummysock[0], &rdfds );
            isrd = true;
#else
            FD_SET( dummysock, &exfds );
            isex = true;
#endif
        }
        validateselecterror = 0;
        max_sockid++;
#ifdef SOCKTRACE
        PROGLOG("SOCKTRACE: selecting on %d sockets",ni);
#endif
    }

    int run()
    {
        try
        {
            T_FD_SET rdfds;
            T_FD_SET wrfds;
            T_FD_SET exfds;
            timeval selecttimeout;
            bool isrd = false;
            bool iswr = false;
            bool isex = false;
            T_SOCKET maxsockid = 0;
            unsigned ni = 0;
            selectvarschange = true;
            unsigned numto = 0;
            unsigned lastnumto = 0;
            unsigned totnum = 0;
            unsigned total = 0;

            while (!terminating)
            {
                selecttimeout.tv_sec = SELECT_TIMEOUT_SECS;         // linux modifies so initialize inside loop
                selecttimeout.tv_usec = 0;
                if (selectvarschange)
                {
                    updateSelectVars(rdfds,wrfds,exfds,isrd,iswr,isex,ni,maxsockid);
                }
                if (ni==0)
                {
                    validateerrcount = 0;
                    tickwait++;
                    if(!selectvarschange&&!terminating) 
                        ticksem.wait(SELECT_TIMEOUT_SECS*1000);
                    tickwait--;
                        
                    continue;
                }
                T_FD_SET rs;
                T_FD_SET ws;
                T_FD_SET es;
                T_FD_SET *rsp = isrd?cpyfds(rs,rdfds):NULL;
                T_FD_SET *wsp = iswr?cpyfds(ws,wrfds):NULL;
                T_FD_SET *esp = isex?cpyfds(es,exfds):NULL;
                int n = ::select(maxsockid,(fd_set *)rsp,(fd_set *)wsp,(fd_set *)esp,&selecttimeout); // first parameter needed for posix
                if (terminating)
                    break;
                if (n < 0)
                {
                    CriticalBlock block(sect);
                    int err = SOCKETERRNO();
                    if (err != JSE_INTR)
                    {
                        if (dummysockopen)
                        {
                            LOGERR(err,12,"CSocketSelectThread select error"); // should cache error ?
                            validateselecterror = err;
#ifndef _USE_PIPE_FOR_SELECT_TRIGGER
                            closedummy();  // just in case was culprit
#endif
                        }
                        selectvarschange = true;
                        continue;
                    }
                    n = 0;
                }
                else if (n>0)
                {
                    validateerrcount = 0;
                    numto = 0;
                    lastnumto = 0;
                    total += n;
                    totnum++;
                    SelectItemArray tonotify;
                    {
                        CriticalBlock block(sect);
#ifdef _WIN32
                        if (isrd) 
                            processfds(rs,SELECTMODE_READ,tonotify);
                        if (iswr) 
                            processfds(ws,SELECTMODE_WRITE,tonotify);
                        if (isex) 
                            processfds(es,SELECTMODE_EXCEPT,tonotify);
#else
                        unsigned i;
                        SelectItem *si = items.getArray(offset);
                        SelectItem *sie = items.getArray(ni-1)+1;
                        bool r = isrd;
                        bool w = iswr;
                        bool e = isex;
#ifdef _USE_PIPE_FOR_SELECT_TRIGGER
                        if (r&&dummysockopen&&findfds(rs,dummysock[0],r))
                        {
                            resettrigger();
                            --n;
                        }           
#endif
                        for (i=0;(n>0)&&(i<ni);i++)
                        {
                            unsigned addMode = 0;
                            if (r&&findfds(rs,si->handle,r))
                            {
                                if (!si->del)
                                    addMode = SELECTMODE_READ;
                                --n;
                            }
                            if (w&&findfds(ws,si->handle,w))
                            {
                                if (!si->del)
                                    addMode = SELECTMODE_WRITE;
                                --n;
                            }
                            if (e&&findfds(es,si->handle,e))
                            {
                                if (!si->del)
                                    addMode = SELECTMODE_EXCEPT;
                                --n;
                            }
                            if (addMode)
                            {
                                tonotify.append(*si);
                                SelectItem &itm = tonotify.element(tonotify.length()-1);
                                itm.nfy->Link();
                                itm.sock->Link();
                                itm.mode = addMode;
                            }
                            si++;
                            if (si==sie)
                                si = items.getArray();
                        }
#endif
                    }
                    ForEachItemIn(j,tonotify)
                    {
                        const SelectItem &si = tonotify.item(j);
                        try
                        {
                            si.nfy->notifySelected(si.sock,si.mode); // ignore return
                        }
                        catch (IException *e)
                        {   // should be acted upon by notifySelected
                            // could also not throw until after handling all events ...
                            EXCLOG(e,"CSocketSelectThread notifySelected");
                            throw ;
                        }
                        // Release/dtors should not throw but leaving try/catch here until all paths checked
                        try
                        {
                            si.nfy->Release();
                            si.sock->Release();
                        }
                        catch (IException *e)
                        {
                            EXCLOG(e,"CSocketSelectThread nfy/sock Release");
                            e->Release();
                        }
                    }
                }
                else
                {
                    validateerrcount = 0;
                    if ((++numto>=lastnumto*2))
                    {
                        lastnumto = numto;
                        if (selecttrace&&(numto>4))
                            PROGLOG("%s: Select Idle(%d), %d,%d,%0.2f",selecttrace,numto,totnum,total,totnum?((double)total/(double)totnum):0.0);
                    }   
/*
                    if (numto&&(numto%100))
                    {
                        CriticalBlock block(sect);
                        if (!selectvarschange) 
                            selectvarschange = checkSocks();
                    }
*/
                }
                if (++offset>=ni)
                    offset = 0;
            }
        }
        catch (IException *e)
        {
            EXCLOG(e,"CSocketSelectThread");
            termexcept.setown(e);
        }
        CriticalBlock block(sect);
        try
        {
            updateItems();
        }
        catch (IException *e)
        {
            EXCLOG(e,"CSocketSelectThread(2)");
            if (!termexcept)
                termexcept.setown(e);
            else
                e->Release();
        }
        return 0;
    }
};


class CSocketSelectHandler: implements ISocketSelectHandler, public CInterface
{
    IArrayOf<CSocketSelectThread> threads;
    CriticalSection sect;
    bool started;
    std::atomic<bool> stopped;
    StringAttr selecttrace;
public:
    IMPLEMENT_IINTERFACE;
    CSocketSelectHandler(const char *trc)
        : started(false), stopped(false), selecttrace(trc)
    {
    }
    ~CSocketSelectHandler()
    {
        stop(true);
        threads.kill();
    }
    void start()
    {
        CriticalBlock block(sect);
        if (!started) {
            started = true;
            ForEachItemIn(i,threads) {
                threads.item(i).start(false);
            }
        }
            
    }
    void add(ISocket *sock,unsigned mode,ISocketSelectNotify *nfy)
    {
        CriticalBlock block(sect);
        if (stopped)
            return;
        for (;;) {
            bool added=false;
            ForEachItemIn(i,threads) {
                if (added)
                    threads.item(i).remove(sock);
                else
                    added = threads.item(i).add(sock,mode,nfy);
            }
            if (added)
                return;
            CSocketSelectThread *thread = new CSocketSelectThread(selecttrace);
            threads.append(*thread);
            if (started)
                thread->start(false);
        }
    }
    void remove(ISocket *sock)
    {
        CriticalBlock block(sect);
        ForEachItemIn(i,threads) {
            if (threads.item(i).remove(sock)&&sock)
                break;
        }
    }
    void stop(bool wait)
    {
        IException *e=NULL;
        CriticalBlock block(sect);
        stopped = true;
        unsigned i = 0;
        while (i<threads.ordinality()) {
            CSocketSelectThread &t=threads.item(i);
            {
                CriticalUnblock unblock(sect);
                t.stop(wait);           // not quite as quick as could be if wait true
            }
            if (wait && !e && t.termexcept)
                e = t.termexcept.getClear();
            i++;
        }
#if 0 // don't throw error as too late
        if (e)
            throw e;
#else
        ::Release(e);
#endif
    }
};

#ifdef _HAS_EPOLL_SUPPORT
# define SOCK_ADDED   0x1
# define SOCK_REMOVED 0x2
# define SOCK_FAILED  0x4
class CSocketEpollThread: public CSocketBaseThread
{
    int epfd;
    SelectItem *sidummy;
    SelectItemArrayP items;
    struct epoll_event *epevents;
    unsigned hdlPerThrd;

    void epoll_op(int efd, int op, SelectItem *si, unsigned int event_mask)
    {
        int fd = si->handle;
        int srtn;
        struct epoll_event event;

        // write all bytes to eliminate uninitialized warnings
        memset(&event, 0, sizeof(event));

        event.events = event_mask;
        event.data.ptr = si;

# ifdef EPOLLTRACE
        DBGLOG("EPOLL: op(%d) fd %d to epfd %d", op, fd, efd);
# endif
        srtn = ::epoll_ctl(efd, op, fd, &event);
        // if another thread closed fd before here don't fail
        if ( (srtn < 0) && (op != EPOLL_CTL_DEL) ){
            int err = SOCKETERRNO();
            IWARNLOG("epoll_ctl failed op:%d, fd:%d, err=%d", op, fd, err);
        }
    }

    // JCSMORE: this is very similar to the version in CSocketSelectThread. Could be commoned if items used common structure.
    void updateItems()
    {
        // must be in CriticalBlock block(sect);
        unsigned n = items.ordinality();
        for (unsigned i=0;i<n;)
        {
            SelectItem *si = items.element(i);
            if (si->del)
            {
                // Release/dtors should not throw but leaving try/catch here until all paths checked
                try
                {
#ifdef SOCKTRACE
                    PROGLOG("CSocketSelectThread::updateItems release %d",si.handle);
#endif
                    si->nfy->Release();
                    si->sock->Release();
                    delete si;
                }
                catch (IException *e)
                {
                    EXCLOG(e,"CSocketSelectThread::updateItems");
                    e->Release();
                }
                n--;
                if (i < n)
                    items.swap(i, n);
                items.remove(n);
            }
            else
                i++;
        }
    }

    void opendummy()
    {
        CriticalBlock block(sect);
        if (!dummysockopen)
        {
            // NB: not notified when triggered, because explicitly spotted and handled before tonotify loop
            sidummy = new SelectItem;
            sidummy->sock = nullptr;
            sidummy->nfy = nullptr;
            sidummy->del = false;
            sidummy->mode = 0;
#ifdef _USE_PIPE_FOR_SELECT_TRIGGER
            if(pipe(dummysock))
            {
                IWARNLOG("CSocketEpollThread: create pipe failed %d",SOCKETERRNO());
                return;
            }
            for (unsigned i=0;i<2;i++)
            {
                int flags = fcntl(dummysock[i], F_GETFL, 0);
                if (flags!=-1)
                {
                    flags |= O_NONBLOCK;
                    fcntl(dummysock[i], F_SETFL, flags);
                }
                flags = fcntl(dummysock[i], F_GETFD, 0);
                if (flags!=-1)
                {
                    flags |=  FD_CLOEXEC;
                    fcntl(dummysock[i], F_SETFD, flags);
                }
            }
            sidummy->handle = dummysock[0];
            epoll_op(epfd, EPOLL_CTL_ADD, sidummy, EPOLLINX);
#else
            if (IP6preferred)
                dummysock = ::socket(AF_INET6, SOCK_STREAM, PF_INET6);
            else
                dummysock = ::socket(AF_INET, SOCK_STREAM, 0);
            // added EPOLLIN and EPOLLRDHUP also because cannot find anywhere MSG_OOB is sent
            // added here to match existing select() code above which sets
            // the except fd_set mask.
            sidummy->handle = dummysock;
            epoll_op(epfd, EPOLL_CTL_ADD, sidummy, (EPOLLINX | EPOLLPRI));
#endif
            dummysockopen = true;
        }
    }

    void closedummy()
    {
        CriticalBlock block(sect);
        if (dummysockopen)
        {
            epoll_op(epfd, EPOLL_CTL_DEL, sidummy, 0);
#ifdef _USE_PIPE_FOR_SELECT_TRIGGER
#ifdef SOCKTRACE
            PROGLOG("SOCKTRACE: Closing dummy sockets %x %d %x %d (%p)", dummysock[0], dummysock[0], dummysock[1], dummysock[1], this);
#endif
            ::close(dummysock[0]);
            ::close(dummysock[1]);
#else
            ::close(dummysock);
#endif
            delete sidummy;
            dummysockopen = false;
        }
    }

    // void delSelItem(SelectItem *si)
    // {
    //     epoll_op(epfd, EPOLL_CTL_DEL, si, 0);
    //     // Release/dtors should not throw but leaving try/catch here until all paths checked
    //     try
    //     {
    //         si->nfy->Release();
    //         si->sock->Release();
    //         delete si;
    //     }
    //     catch (IException *e)
    //     {
    //         EXCLOG(e,"CSocketEpollThread::delSelItem()");
    //         e->Release();
    //     }
    // }

    // void delSelItemPos(SelectItem *si, unsigned pos)
    // {
    //     unsigned last = items.ordinality();
    //     delSelItem(si);
    //     last--;
    //     if (pos < last)
    //         items.swap(pos, last);
    //     items.remove(last);
    // }

public:
    CSocketEpollThread(const char *trc, unsigned _hdlPerThrd)
        : CSocketBaseThread("CSocketEpollThread")
    {
        dummysockopen = false;
        terminating = false;
        waitingchange = 0;
        selectvarschange = false;
        validateselecterror = 0;
        validateerrcount = 0;
        offset = 0;
        selecttrace = trc;
        hdlPerThrd = _hdlPerThrd;
        epfd = ::epoll_create(1); // NB: arg is not used in newer kernels
        if (epfd < 0)
        {
            int err = SOCKETERRNO();
            LOGERR(err,1,"epoll_create()");
            THROWJSOCKEXCEPTION(err);
        }
# ifdef FD_CLOEXEC
        int epflags = fcntl(epfd, F_GETFD, 0);
        if (epflags != -1)
        {
            epflags |= FD_CLOEXEC;
            fcntl(epfd, F_SETFD, epflags);
        }
# endif
# ifdef EPOLLTRACE
        DBGLOG("CSocketEpollThread: creating epoll fd %d", epfd );
# endif
        try
        {
            epevents = new struct epoll_event[MAX_RET_EVENTS];
        }
        catch (const std::bad_alloc &e)
        {
            int err = SOCKETERRNO();
            LOGERR(err,1,"epevents alloc()");
            THROWJSOCKEXCEPTION(err);
        }
        opendummy();
    }

    ~CSocketEpollThread()
    {
        closedummy();
        ForEachItemIn(i, items)
        {
            // Release/dtors should not throw but leaving try/catch here until all paths checked
            try
            {
                SelectItem *si = items.element(i);
                si->nfy->Release();
                si->sock->Release();
                delete si;
            }
            catch (IException *e)
            {
                EXCLOG(e,"~CSocketSelectThread");
                e->Release();
            }
        }

        if (epfd >= 0)
        {
# ifdef EPOLLTRACE
            DBGLOG("EPOLL: closing epfd %d", epfd);
# endif
            ::close(epfd);
            epfd = -1;
            delete [] epevents;
        }
    }

    Owned<IException> termexcept;

    // JCSMORE: this is very similar to the version in CSocketSelectThread. Could be commoned if items used common structure.
    bool checkSocks()
    {
        bool ret = false;
        ForEachItemIn(i, items)
        {
            SelectItem &si = *items.element(i);
            if (si.del)
                ret = true; // maybe that bad one
            else if (!sockOk(si.handle))
            {
                si.del = true;
                ret = true;
            }
        }
        return ret;
    }

    // bool removeSock(ISocket *sock)
    // {
    //     // must be holding CriticalBlock (sect)
    //     unsigned n = items.ordinality();
    //     for (unsigned i=0;i<n;)
    //     {
    //         SelectItem *si = items.element(i);
    //         if (si->sock==sock)
    //         {
    //             delSelItemPos(si, i);
    //             n--;
    //             return true;
    //         }
    //         else
    //             i++;
    //     }
    //     return false;
    // }

    bool remove(ISocket *sock)
    {
        if (terminating)
            return true; // pretend is, to short-circuit caller
        CriticalBlock block(sect);

        // JCS - I can't see when sock can be null
        if (sock==NULL) // wait until no changes outstanding
        {
            while (selectvarschange)
            {
                waitingchange++;
                CriticalUnblock unblock(sect);
                waitingchangesem.wait();
            }
            return true;
        }

        ForEachItemIn(i, items)
        {
            SelectItem *si = items.element(i);
            if (!si->del && (si->sock==sock))
            {
                // epoll_wait (in the CSocketEpollThread thread) may have pending events on socket this is about to remove
                // so we must not remove the SelectItem here, just mark it as deleted (deletion performed in updateEpollVars)
                epoll_op(epfd, EPOLL_CTL_DEL, si, 0);
                si->del = true;
                selectvarschange = true;
                triggerselect();
                // JCSMORE see comment in CSocketEpollThread::run re. need for CS.
                // I think the array could be manipulated here, as long as the SelectItem isn't deleted.
                // It may also be more efficient if it used a double linked list to remove items.
                return true;
            }
        }
        return false;
    }

    unsigned add(ISocket *sock, unsigned mode, ISocketSelectNotify *nfy)
    {
        if ( !sock || !nfy ||
             !(mode & (SELECTMODE_READ|SELECTMODE_WRITE|SELECTMODE_EXCEPT)) )
        {
            IWARNLOG("EPOLL: adding fd but sock or nfy is NULL or mode is empty");
            dbgassertex(false);
            return SOCK_FAILED;
        }
        CriticalBlock block(sect);
        if (terminating)
            return SOCK_FAILED;

        // JCSMORE may be more efficient to use a double linked list to avoid this scan (and array/expansion)
        unsigned rm = 0;
        unsigned n=0;
        ForEachItemIn(i, items)
        {
            SelectItem &si = *items.element(i);
            if (!si.del)
            {
                if (si.sock==sock)
                {
                    si.del = true;
                    rm = SOCK_REMOVED;
                    break;
                }
                else
                    n++;
            }
        }

        // new handler thread
        if (n >= hdlPerThrd)
            return (0|rm);
        SelectItem *sn = new SelectItem;
        sn->nfy = LINK(nfy);
        sn->sock = LINK(sock);
        sn->mode = (byte)mode;
        sn->handle = (T_SOCKET)sock->OShandle();
        sn->del = false;
        items.append(sn);
        unsigned int ep_mode = 0;
        if (mode & SELECTMODE_READ)
            ep_mode |= EPOLLINX;
        if (mode & SELECTMODE_WRITE)
            ep_mode |= EPOLLOUT;
        if (mode & SELECTMODE_EXCEPT)
            ep_mode |= EPOLLPRI;
        // JCSMORE - we are in a CS, but it is thread-safe to epoll_wait concurrently with epoll_op
        epoll_op(epfd, EPOLL_CTL_ADD, sn, ep_mode);
        selectvarschange = true;
        triggerselect();
        return (SOCK_ADDED|rm);
    }

    void updateEpollVars(unsigned &ni)
    {
        CriticalBlock block(sect);
        selectvarschange = false;
        if (waitingchange)
        {
            waitingchangesem.signal(waitingchange);
            waitingchange = 0;
        }
        if (validateselecterror)
        { // something went wrong so check sockets
            validateerrcount++;
            if (!checkSocks())
            {
                // bad socket not found
                IWARNLOG("CSocketEpollThread::updateEpollVars cannot find socket error");
                if (validateerrcount>10)
                    throw MakeStringException(-1,"CSocketEpollThread:Socket epoll error %d",validateselecterror);
            }
        }
        else
            validateerrcount = 0;
#ifndef _USE_PIPE_FOR_SELECT_TRIGGER
        opendummy();
#endif
        updateItems();
        ni = items.ordinality();
        validateselecterror = 0;
    }

    int run()
    {
        try
        {
            unsigned ni = 0;
            unsigned numto = 0;
            unsigned lastnumto = 0;
            unsigned totnum = 0;
            unsigned total = 0;
            selectvarschange = true;

            while (!terminating)
            {
                if (selectvarschange)
                    updateEpollVars(ni);

                if (ni==0)
                {
                    validateerrcount = 0;
                    tickwait++;
                    if(!selectvarschange&&!terminating)
                        ticksem.wait(SELECT_TIMEOUT_SECS*1000);
                    tickwait--;
                    continue;
                }

                // poll for any events ...
                int err = 0;
                int n = ::epoll_wait(epfd, epevents, 1, 1000);
                if (n < 0)
                    err = SOCKETERRNO();

# ifdef EPOLLTRACE
                if(n > 0)
                    DBGLOG("EPOLL: after epoll_wait(), n = %d, ni = %d", n, ni);
# endif

                if (terminating)
                    break;
                if (n < 0)
                {
                    if (err != JSE_INTR)
                    {
                        CriticalBlock block(sect);
                        if (dummysockopen)
                        {
                            LOGERR(err,12,"CSocketEpollThread epoll error"); // should cache error ?
                            validateselecterror = err;
#ifndef _USE_PIPE_FOR_SELECT_TRIGGER
                            closedummy();  // just in case was culprit
#endif
                        }
                        selectvarschange = true;
                        continue;
                    }
                }
                else if (n>0)
                {
                    validateerrcount = 0;
                    numto = 0;
                    lastnumto = 0;
                    total += n;
                    totnum++;
                    SelectItemArray tonotify;
                    {
                        // JCSMORE - I don't think this CS is needed.
                        // It is in the CSocketSelectThread version because the items array needs protecting)
                        // But in the epoll version, we reference the SelectItem pointers only, the items array isn't referenced,
                        // and epoll_op's are thread-safe. But we must ensure the SelectItem's are only deleted by this thread,
                        // since even if in a mutex, pending events may referene them.
                        CriticalBlock block(sect);

                        // retrieve events, without waiting, while holding CS ...
                        int n2 = ::epoll_wait(epfd, epevents, MAX_RET_EVENTS, 0);

                        for (int j=0;j<n2;j++)
                        {
                            int tfd = -1;
                            SelectItem *epsi = (SelectItem *)epevents[j].data.ptr;
                            if (epsi && !epsi->del) // NB: could receive an event for a pending deleted item
                                tfd = epsi->handle;
# ifdef EPOLLTRACE
                            DBGLOG("EPOLL: j = %d, fd = %d, emask = %u", j, tfd, epevents[j].events);
# endif
                            if (tfd >= 0)
                            {
                                if (dummysockopen)
                                {
# ifdef _USE_PIPE_FOR_SELECT_TRIGGER
                                    if (tfd == dummysock[0])
                                    {
                                        resettrigger();
                                        continue;
                                    }
#else
                                    // would still need to resettrigger and ignore dummysock (but subtly different code)
                                    throwUnimplemented();
# endif
                                }
                                unsigned int ep_mode = 0;
                                if (epevents[j].events & (EPOLLINX | EPOLLHUP | EPOLLERR))
                                    ep_mode |= SELECTMODE_READ;
                                if (epevents[j].events & EPOLLOUT)
                                    ep_mode |= SELECTMODE_WRITE;
                                if (epevents[j].events & EPOLLPRI)
                                    ep_mode |= SELECTMODE_EXCEPT;
                                if (ep_mode != 0)
                                {
                                    tonotify.append(*epsi);
                                    SelectItem &itm = tonotify.element(tonotify.length()-1);
                                    itm.nfy->Link();
                                    itm.sock->Link();
#ifdef _TRACELINKCLOSED
                                    // temporary, to help diagnose spurious socket closes (hpcc-15043)
                                    // currently no implementation of notifySelected() uses the mode
                                    // argument so we can pass in the epoll events mask and log that
                                    // if there is no data and the socket gets closed
                                    itm.mode = epevents[j].events;
#else
                                    itm.mode = ep_mode;
#endif
                                }
                            }
                        }
                    }
                    Owned<IException> nfyexcept;
                    ForEachItemIn(j,tonotify)
                    {
                        const SelectItem &si = tonotify.item(j);
                        try
                        {
                            // is it worth checking si.del again? we can't guarantee that this is a socket that has been marked deleted.
                            si.nfy->notifySelected(si.sock,si.mode); // ignore return
                        }
                        catch (IException *e)
                        {   // should be acted upon by notifySelected
                            // Don't throw until after handling all events ...
                            EXCLOG(e,"CSocketEpollThread notifySelected");
                            if (!nfyexcept)
                                nfyexcept.setown(e);
                            else
                                e->Release();
                        }
                        // Release/dtors should not throw but leaving try/catch here until all paths checked
                        try
                        {
                            si.nfy->Release();
                            si.sock->Release();
                        }
                        catch (IException *e)
                        {
                            EXCLOG(e,"CSocketEpollThread nfy/sock Release");
                            e->Release();
                        }
                    }
                    if (nfyexcept)
                        throw nfyexcept.getClear();
                }
                else
                {
                    validateerrcount = 0;
                    if ((++numto>=lastnumto*2))
                    {
                        lastnumto = numto;
                        if (selecttrace&&(numto>4))
                            PROGLOG("%s: Epoll Idle(%d), %d,%d,%0.2f",selecttrace,numto,totnum,total,totnum?((double)total/(double)totnum):0.0);
                    }
/*
                    if (numto&&(numto%100))
                    {
                        CriticalBlock block(sect);
                        if (!selectvarschange)
                            selectvarschange = checkSocks();
                    }
*/
                }
            }
        }
        catch (IException *e)
        {
            EXCLOG(e,"CSocketEpollThread");
            termexcept.setown(e);
        }
        return 0;
    }
};

class CSocketEpollHandler: implements ISocketSelectHandler, public CInterface
{
    IArrayOf<CSocketEpollThread> threads;
    CriticalSection sect;
    bool started;
    std::atomic<bool> stopped;
    StringAttr epolltrace;
    unsigned hdlPerThrd;
public:
    IMPLEMENT_IINTERFACE;
    CSocketEpollHandler(const char *trc, unsigned _hdlPerThrd)
        : started(false), stopped(false), epolltrace(trc), hdlPerThrd(_hdlPerThrd)
    {
    }

    ~CSocketEpollHandler()
    {
        stop(true);
        threads.kill();
    }

    void start()
    {
        CriticalBlock block(sect);
        if (!started)
        {
            started = true;
            ForEachItemIn(i,threads)
            {
                threads.item(i).start(false);
            }
        }
    }

    void add(ISocket *sock,unsigned mode,ISocketSelectNotify *nfy)
    {
        if ( !sock || !nfy ||
             !(mode & (SELECTMODE_READ|SELECTMODE_WRITE|SELECTMODE_EXCEPT)) )
            throw MakeStringException(-1,"CSocketEpollHandler::add() invalid sock or nfy or mode");

        CriticalBlock block(sect);
        if (stopped)
            return;

        // Create new handler thread if current one has hdlPerThrd fds.
        // epoll() handles many fds faster than select so this would
        // seem not as important, but we are still serializing on
        // nfy events and spreading those over threads may help,
        // especially with SSL as avail_read() could block more.
        unsigned addrm = 0;
        ForEachItemIn(i,threads)
        {
            if (!(addrm & SOCK_ADDED))
            {
                addrm |= threads.item(i).add(sock,mode,nfy);
                // if both added and removed, we're done
                if (((addrm & (SOCK_ADDED | SOCK_REMOVED)) == (SOCK_ADDED | SOCK_REMOVED)) ||
                    (addrm & SOCK_FAILED))
                    return;
            }
            else if (!(addrm & SOCK_REMOVED))
            {
                if (threads.item(i).remove(sock))
                    return;
            }
        }
        if (addrm & SOCK_ADDED)
            return;

        CSocketEpollThread *thread = new CSocketEpollThread(epolltrace, hdlPerThrd);
        threads.append(*thread);
        if (started)
            thread->start(false);
        thread->add(sock,mode,nfy);
    }

    void remove(ISocket *sock)
    {
        CriticalBlock block(sect);
        ForEachItemIn(i,threads)
        {
            if (threads.item(i).remove(sock))
                break;
        }
    }

    void stop(bool wait)
    {
        CriticalBlock block(sect);
        stopped = true;
        ForEachItemIn(i,threads)
        {
            CSocketEpollThread &t=threads.item(i);
            {
                CriticalUnblock unblock(sect);
                t.stop(wait);           // not quite as quick as could be if wait true
            }
        }
    }
};

enum EpollMethod { EPOLL_INIT = 0, EPOLL_DISABLED, EPOLL_ENABLED };
static EpollMethod epoll_method = EPOLL_INIT;
static CriticalSection epollsect;

void check_epoll_cfg()
{
    CriticalBlock block(epollsect);
    // DBGLOG("check_epoll_cfg(): epoll_method = %d",epoll_method);
    if (epoll_method == EPOLL_INIT)
    {
#ifdef _CONTAINERIZED
//Does this need to be implemented a different way?
        epoll_method = EPOLL_ENABLED;
        epoll_hdlPerThrd = UINT_MAX;
#else
        if (queryEnvironmentConf().getPropBool("use_epoll", true))
            epoll_method = EPOLL_ENABLED;
        else
            epoll_method = EPOLL_DISABLED;
        // DBGLOG("check_epoll_cfg(): after reading conf file, epoll_method = %d",epoll_method);
        epoll_hdlPerThrd = (unsigned)queryEnvironmentConf().getPropInt("epoll_hdlperthrd", UINT_MAX);
        if (epoll_hdlPerThrd == 0)
            epoll_hdlPerThrd = UINT_MAX;
        // DBGLOG("check_epoll_cfg(): after reading conf file, epoll_hdlPerThrd = %u",epoll_hdlPerThrd);
#endif
    }
}
#endif // _HAS_EPOLL_SUPPORT

ISocketSelectHandler *createSocketSelectHandler(const char *trc, unsigned hdlPerThrd)
{
#ifdef _HAS_EPOLL_SUPPORT
    check_epoll_cfg();
    if (epoll_method == EPOLL_ENABLED)
    {
        if (hdlPerThrd == 0)
            hdlPerThrd = epoll_hdlPerThrd;
        return new CSocketEpollHandler(trc, hdlPerThrd);
    }
    else
        return new CSocketSelectHandler(trc);
#else
    return new CSocketSelectHandler(trc);
#endif
}

ISocketSelectHandler *createSocketEpollHandler(const char *trc, unsigned hdlPerThrd)
{
#ifdef _HAS_EPOLL_SUPPORT
    check_epoll_cfg();
    if (hdlPerThrd == 0)
        hdlPerThrd = epoll_hdlPerThrd;
    return new CSocketEpollHandler(trc, hdlPerThrd);
#else
    return new CSocketSelectHandler(trc);
#endif
}

void readBuffer(ISocket * socket, MemoryBuffer & buffer)
{
    size32_t len;
    socket->read(&len, sizeof(len));
    _WINREV4(len);
    if (len) {
        void * target = buffer.reserve(len);
        socket->read(target, len);
    }
}

void readBuffer(ISocket * socket, MemoryBuffer & buffer, unsigned timeoutms)
{
    size32_t len;
    size32_t sizeGot;
    socket->readtms(&len, sizeof(len), sizeof(len), sizeGot, timeoutms);
    _WINREV4(len);
    if (len) {
        void * target = buffer.reserve(len);
        socket->readtms(target, len, len, sizeGot, timeoutms);
    }
}

void writeBuffer(ISocket * socket, MemoryBuffer & buffer)
{
    unsigned len = buffer.length();
    _WINREV4(len);
    socket->write(&len, sizeof(len));
    if (len)
        socket->write(buffer.toByteArray(), buffer.length());
}


bool catchReadBuffer(ISocket * socket, MemoryBuffer & buffer)
{
    try
    {
        readBuffer(socket, buffer);
        return true;
    }
    catch (IException * e)
    {
        switch (e->errorCode())
        {
        case JSOCKERR_graceful_close:
            break;
        default:
            EXCLOG(e,"catchReadBuffer");
            break;
        }
        e->Release();
    }
    return false;
}

bool catchReadBuffer(ISocket * socket, MemoryBuffer & buffer, unsigned timeoutms)
{
    try
    {
        readBuffer(socket, buffer, timeoutms);
        return true;
    }
    catch (IException * e)
    {
        switch (e->errorCode())
        {
        case JSOCKERR_graceful_close:
            break;
        default:
            EXCLOG(e,"catchReadBuffer");
            break;
        }
        e->Release();
    }
    return false;
}

bool catchWriteBuffer(ISocket * socket, MemoryBuffer & buffer)
{
    try
    {
        writeBuffer(socket, buffer);
        return true;
    }
    catch (IException * e)
    {
        EXCLOG(e,"catchWriteBuffer");
        e->Release();
    }
    return false;
}

CSingletonSocketConnection::CSingletonSocketConnection(SocketEndpoint &_ep)
{
    ep = _ep;
    state = Snone;
    cancelling = false;
}

CSingletonSocketConnection::~CSingletonSocketConnection()
{
    shutdownAndCloseNoThrow(sock);
}

void CSingletonSocketConnection::set_keep_alive(bool keepalive)
{
    if (sock)
        sock->set_keep_alive(keepalive);
}

bool CSingletonSocketConnection::connect(unsigned timeoutms)
{
    CriticalBlock block(crit);
    if (cancelling)
        state = Scancelled;
    if (state==Scancelled)
        return false;
    assertex(!sock);
    ISocket *newsock=NULL;
    state = Sconnect;
    unsigned start = 0;
    if (timeoutms!=(unsigned)INFINITE)
        start = msTick();
    while (state==Sconnect) {
        try {
            CriticalUnblock unblock(crit);
            newsock = ISocket::connect_wait(ep,1000*60*4);
            break;
        }
        catch (IException * e) {
            if ((e->errorCode()==JSOCKERR_timeout_expired)||(e->errorCode()==JSOCKERR_connection_failed)) {
                e->Release();
                if ((state==Sconnect)&&(timeoutms!=(unsigned)INFINITE)&&(msTick()-start>timeoutms)) {
                    state = Snone;
                    return false;
                }
            }
            else {
                state = Scancelled;
                EXCLOG(e,"CSingletonSocketConnection::connect");
                e->Release();
                return false;
            }
        }
    }
    if (state!=Sconnect) {
        ::Release(newsock);
        newsock = NULL;
    }
    if (!newsock) {
        state = Scancelled;
        return false;
    }
    sock.setown(newsock);
    return true;
}

bool CSingletonSocketConnection::send(MemoryBuffer &mb)
{
    CriticalBlock block(crit);
    if (cancelling)
        state = Scancelled;
    if (state==Scancelled)
        return false;
    assertex(sock);
    state = Srecv;
    try {
        CriticalUnblock unblock(crit);
        writeBuffer(sock,mb);
    }
    catch (IException * e) {
        state = Scancelled;
        EXCLOG(e,"CSingletonSocketConnection::send");
        e->Release();
        return false;
    }
    state = Snone;
    return true;
}

unsigned short CSingletonSocketConnection::setRandomPort(unsigned short base, unsigned num)
{
    for (;;) {
        try {
            ep.port = base+(unsigned short)(getRandom()%num);
            listensock.setown(ISocket::create(ep.port));
            return ep.port;
        }
        catch (IException *e) {
            if (e->errorCode()!=JSOCKERR_port_in_use) {
                state = Scancelled;
                EXCLOG(e,"CSingletonSocketConnection::setRandomPort");
                e->Release();
                break;
            }
            e->Release();
        }
    }
    return 0;
}


bool CSingletonSocketConnection::accept(unsigned timeoutms)
{
    CriticalBlock block(crit);
    if (cancelling)
        state = Scancelled;
    if (state==Scancelled)
        return false;
    if (!sock) {
        ISocket *newsock=NULL;
        state = Saccept;
        for (;;) {
            try {
                {
                    CriticalUnblock unblock(crit);
                    if (!listensock)
                        listensock.setown(ISocket::create(ep.port));
                    if ((timeoutms!=(unsigned)INFINITE)&&(!listensock->wait_read(timeoutms))) {
                        state = Snone;
                        return false;
                    }
                }
                if (cancelling)
                    state = Scancelled;
                if (state==Scancelled)
                    return false;
                {
                    CriticalUnblock unblock(crit);
                    newsock=listensock->accept(true);
                    break;
                }
            }
            catch (IException *e) {
                if (e->errorCode()==JSOCKERR_graceful_close)
                    PROGLOG("CSingletonSocketConnection: Closed socket on accept - retrying...");
                else {
                    state = Scancelled;
                    EXCLOG(e,"CSingletonSocketConnection::accept");
                    e->Release();
                    break;
                }
                e->Release();
            }
        }
        if (state!=Saccept) {
            ::Release(newsock);
            newsock = NULL;
        }
        if (!newsock) {
            state = Scancelled;
            return false;
        }
        sock.setown(newsock);
    }
    return true;
}

bool CSingletonSocketConnection::recv(MemoryBuffer &mb, unsigned timeoutms)
{
    CriticalBlock block(crit);
    if (cancelling)
        state = Scancelled;
    if (state==Scancelled)
        return false;
    assertex(sock);
    state = Srecv;
    try {
        CriticalUnblock unblock(crit);
        readBuffer(sock,mb,timeoutms);
    }
    catch (IException *e) {
        if (e->errorCode()==JSOCKERR_timeout_expired)
            state = Snone;
        else {
            state = Scancelled;
            if (e->errorCode()!=JSOCKERR_graceful_close)
                EXCLOG(e,"CSingletonSocketConnection::recv");
        }
        e->Release();
        return false;
    }
    state = Snone;
    return true;
}

void CSingletonSocketConnection::cancel()
{
    CriticalBlock block(crit);
    while (state!=Scancelled) {
        cancelling = true;
        try {
            switch (state) {
            case Saccept:
                {
                    if (listensock)
                        listensock->cancel_accept();
                }
                break;
            case Sconnect:
                // wait for timeout
                break;
            case Srecv:
                {
                    if (sock)
                        sock->close();
                }
                break;
            case Ssend:
                // wait for finished
                break;
            default:
                state = Scancelled;
                break;
            }
        }
        catch (IException *e) {
            EXCLOG(e,"CSingletonSocketConnection::cancel");
            e->Release();
        }
        {
            CriticalUnblock unblock(crit);
            Sleep(1000);
        }
    }
}

    
IConversation *createSingletonSocketConnection(unsigned short port,SocketEndpoint *_ep)
{
    SocketEndpoint ep;
    if (_ep)
        ep = *_ep;
    if (port)
        ep.port = port;
    return new CSingletonSocketConnection(ep);
}


// interface for reading from multiple sockets using the BF_SYNC_TRANSFER_PUSH protocol 
class CSocketBufferReader: implements ISocketBufferReader, public CInterface
{
    class SocketElem: implements ISocketSelectNotify, public CInterface
    {
        CSocketBufferReader *parent;
        unsigned num;           // top bit used for ready
        MemoryAttr blk;
        CriticalSection sect;
        Linked<ISocket> sock;
        bool active;
        bool pending;
    public:
        IMPLEMENT_IINTERFACE;
        void init(CSocketBufferReader *_parent,ISocket *_sock,unsigned _n)
        {
            parent = _parent;
            num = _n;
            sock.set(_sock);
            active = true;
            pending = false;            
        }

        virtual bool notifySelected(ISocket *socket,unsigned selected)
        {
            assertex(sock==socket);
            {
                CriticalBlock block(sect);
                if (pending) {
                    active = false;
                    parent->remove(sock);  
                    return false;
                }
                pending = true;
                unsigned t1=usTick();
                size32_t sz = sock->receive_block_size();
                unsigned t2=usTick();
                if (sz)
                    sock->receive_block(blk.allocate(sz),sz);
                else
                    parent->remove(sock);  
                unsigned t3=usTick();
                if (t3-t1>60*1000000)
                    PROGLOG("CSocketBufferReader(%d): slow receive_block (%d,%d) sz=%d",num,t2-t1,t3-t2,sz);
            }
            parent->enqueue(this); // nb outside sect critical block
            return false;   // always return false
        }
        unsigned get(MemoryBuffer &mb)
        {
            CriticalBlock block(sect);
            assertex(pending);
            size32_t sz = (size32_t)blk.length();
            if (sz)
                mb.setBuffer(sz,blk.detach(),true);
            pending = false;
            if (!active) {
                active = true;
                parent->add(*this);  
            }
            return num;
        }
        size32_t size()
        {
            return (size32_t)blk.length();
        }
        ISocket *getSocket() { return sock; }
    } *elems;
    SimpleInterThreadQueueOf<SocketElem, false> readyq;
    Owned<ISocketSelectHandler> selecthandler;

    size32_t buffersize;
    size32_t buffermax;
    unsigned bufferwaiting;
    CriticalSection buffersect;
    Semaphore buffersem;
    bool isdone;

public:

    IMPLEMENT_IINTERFACE;
    CSocketBufferReader(const char *trc)
    {
        selecthandler.setown(createSocketSelectHandler(trc));
        elems = NULL;
    }
    ~CSocketBufferReader()
    {
        delete [] elems;
    }
    virtual void init(unsigned num,ISocket **sockets,size32_t _buffermax)
    {
        elems = new SocketElem[num];
        for (unsigned i=0;i<num;i++) {
            ISocket *sock = sockets[i];
            if (sock) { // can have gaps
                elems[i].init(this,sock,i);
                add(elems[i]);
            }
        }
        buffersize = 0;
        buffermax = _buffermax;
        bufferwaiting = 0;
        isdone = false;
        selecthandler->start();
    }
    virtual unsigned get(MemoryBuffer &mb)
    {
        SocketElem &e = *readyq.dequeue();
        CriticalBlock block(buffersect);
        assertex(buffersize>=e.size());
        buffersize-=e.size();
        if (bufferwaiting) {
            buffersem.signal(bufferwaiting);
            bufferwaiting = 0;
        }
        return e.get(mb);
    }
    virtual void done(bool wait)
    {
        buffersem.signal(0x10000);
        isdone = true;
        selecthandler->stop(wait);  
        if (wait) {
            delete [] elems;
            elems = NULL;
        }
    }

    void enqueue(SocketElem *elem)
    {
        if (elem) {
            CriticalBlock block(buffersect);
            size32_t sz = elem->size();
            while ((buffersize>0)&&(sz>0)&&(buffersize+sz>buffermax)) {
                if (isdone)
                    return;
                bufferwaiting++;
                CriticalUnblock unblock(buffersect);
                buffersem.wait();
            }
            buffersize += sz;
        }
        readyq.enqueue(elem);
    }

    void remove(ISocket *sock)
    {
        selecthandler->remove(sock);
    }

    void add(SocketElem &elem)
    {
        selecthandler->add(elem.getSocket(),SELECTMODE_READ,&elem);
    }
};

ISocketBufferReader *createSocketBufferReader(const char *trc)
{
    return new CSocketBufferReader(trc);
}


static CSocket *prepareSocket(unsigned idx,const SocketEndpoint &ep, ISocketConnectNotify &inotify)
{
    Owned<CSocket> sock = new CSocket(ep,sm_tcp,NULL);
    int err = sock->pre_connect(false);
    if ((err == JSE_INPROGRESS)||(err == JSE_WOULDBLOCK)) 
        return sock.getClear();
    if (err==0) {
        int err = sock->post_connect();
        if (err==0) 
            inotify.connected(idx,ep,sock);
        else {
            sock->errclose();
            inotify.failed(idx,ep,err);
        }
    }
    else
        inotify.failed(idx,ep,err);
    return NULL;
}

void multiConnect(const SocketEndpointArray &eps,ISocketConnectNotify &inotify,unsigned timeout)
{
    class SocketElem: implements ISocketSelectNotify, public CInterface
    {
        CriticalSection *sect;
        ISocketSelectHandler *handler;
        unsigned *remaining;
        Semaphore *notifysem;
        ISocketConnectNotify *inotify;
    public:
        Owned<CSocket> sock;
        SocketEndpoint ep;
        unsigned idx;
        IMPLEMENT_IINTERFACE;
        void init(CSocket *_sock,unsigned _idx,const SocketEndpoint &_ep,CriticalSection *_sect,ISocketSelectHandler *_handler,ISocketConnectNotify *_inotify, unsigned *_remaining, Semaphore *_notifysem)
        {
            ep = _ep;
            idx = _idx;
            inotify = _inotify;
            sock.setown(_sock),
            sect = _sect;
            handler = _handler;
            remaining = _remaining;
            notifysem = _notifysem;
        }

        virtual bool notifySelected(ISocket *socket,unsigned selected)
        {
            CriticalBlock block(*sect);
            handler->remove(socket);  
            int err = sock->post_connect();
            CSocket *newsock = NULL;
            {
                CriticalUnblock unblock(*sect); // up to caller to cope with multithread
                if (err==0) 
                    inotify->connected(idx,ep,sock);
                else if ((err==JSE_TIMEDOUT)||(err==JSE_CONNREFUSED))  { 
                         // don't give up so easily (maybe listener not yet started (i.e. racing))
                    newsock = prepareSocket(idx,ep,*inotify);
                    Sleep(100); // not very nice but without this would just loop 
                }
                else 
                    inotify->failed(idx,ep,err);
            }
            if (newsock) {
                sock.setown(newsock);
                handler->add(sock,SELECTMODE_WRITE|SELECTMODE_EXCEPT,this);
            }
            else {
                sock.clear();
                (*remaining)--;
                notifysem->signal();
            }
            return false;
        }
    } *elems;
    unsigned n = eps.ordinality();
    unsigned remaining = n;
    if (!n)
        return;
    elems = new SocketElem[n];
    unsigned i;
    CriticalSection sect;
    Semaphore notifysem;
    Owned<ISocketSelectHandler> selecthandler = createSocketSelectHandler(
#ifdef _DEBUG
        "multiConnect"
#else
        NULL
#endif
    );

    StringBuffer name;
    for (i=0;i<n;i++) {
        CSocket* sock = prepareSocket(i,eps.item(i),inotify);
        if (sock) {
            elems[i].init(sock,i,eps.item(i),&sect,selecthandler,&inotify,&remaining,&notifysem);
            selecthandler->add(sock,SELECTMODE_WRITE|SELECTMODE_EXCEPT,&elems[i]);
        }           
        else 
            remaining--;
    }
    if (remaining) {
        unsigned lastremaining=remaining;
        selecthandler->start();
        for (;;) {
            bool to=!notifysem.wait(timeout);
            {
                CriticalBlock block(sect);
                if (remaining==0)
                    break;
                if (to&&(remaining==lastremaining))
                    break; // nothing happened recently
                lastremaining = remaining;
            }
        }
        selecthandler->stop(true);  
    }
    selecthandler.clear();
    if (remaining) {
        for (unsigned j=0;j<n;j++) {  // mop up timeouts
            SocketElem &elem = elems[j];
            if (elem.sock.get())  {
                elem.sock.clear();
                inotify.failed(j,elem.ep,-1);
                remaining--;
                if (remaining==0)
                    break;
            }
        }
    }
    delete [] elems;
}

void multiConnect(const SocketEndpointArray &eps, IPointerArrayOf<ISocket> &retsockets,unsigned timeout)
{
    unsigned n = eps.ordinality();
    if (n==0)
        return;
    if (n==1) { // no need for multi
        ISocket *sock = NULL;
        try {
            sock = ISocket::connect_timeout(eps.item(0),timeout);
        }
        catch (IException *e) { // ignore error just append NULL
            sock = NULL;
            e->Release();
        }
        retsockets.append(sock);
        return;
    }   
    while (retsockets.ordinality()<n)
        retsockets.append(NULL);
    CriticalSection sect;
    class cNotify: implements ISocketConnectNotify
    {
        CriticalSection &sect;
        IPointerArrayOf<ISocket> &retsockets;
    public:
        cNotify(IPointerArrayOf<ISocket> &_retsockets,CriticalSection &_sect)
            : sect(_sect), retsockets(_retsockets)
        {
        }
        
        void connected(unsigned idx,const SocketEndpoint &ep,ISocket *sock)
        {
            CriticalBlock block(sect);
            assertex(idx<retsockets.ordinality());
            sock->Link();
            retsockets.replace(sock,idx);
        }
        void failed(unsigned idx,const SocketEndpoint &ep,int err)
        {
            StringBuffer s;
            PROGLOG("multiConnect failed to %s with %d",ep.getEndpointHostText(s).str(),err);
        }
    } notify(retsockets,sect);
    multiConnect(eps,notify,timeout);

}

inline void flushText(StringBuffer &text,unsigned short port,unsigned &rep,unsigned &range) 
{
    if (rep) {
        text.append('*').append(rep+1);
        rep = 0;
    }
    else if (range) {
        text.append('-').append(range);
        range = 0;
    }
    if (port)
        text.append(':').append(port);
}



StringBuffer &SocketEndpointArray::getText(StringBuffer &text) const
{
    unsigned count = ordinality();
    if (!count)
        return text;
    if (count==1)
        return item(0).getEndpointHostText(text);
    byte lastip[4];
    const SocketEndpoint &first = item(0);
    first.getHostText(text);
    bool lastis4 = false;
    if (!first.queryHostname())
        lastis4 = first.getNetAddress(sizeof(lastip),&lastip)==sizeof(lastip);
    unsigned short lastport = first.port;
    unsigned rep=0;
    unsigned range=0;
    for (unsigned i=1;i<count;i++) {
        byte ip[4];
        const SocketEndpoint &ep = item(i);
        bool is4 = false;
        if (!ep.queryHostname())
            is4 = ep.getNetAddress(sizeof(ip),&ip)==sizeof(ip);
        if (!lastis4||!is4) {
            flushText(text,lastport,rep,range);
            text.append(',');
            ep.getHostText(text);
        }
        else { // try and shorten
            unsigned j;
            for (j=0;j<4;j++)
                if (ip[j]!=lastip[j])
                    break;
            if (ep.port==lastport) {
                if (j==4) {
                    if (range)  // cant have range and rep
                        j--;  // pretend only 3 matched
                    else {
                        rep++;
                        continue;
                    }
                }
                else if ((j==3)&&(lastip[3]+1==ip[3])&&(rep==0)) {
                    range = ip[3];
                    lastip[3] = (byte)range;
                    continue;
                }
            }
            flushText(text,lastport,rep,range); 
            // output diff
            text.append(',');
            if (j==4)
                j--;
            for (unsigned k=j;k<4;k++) {
                if (k>j)
                    text.append('.');
                text.append((int)ip[k]);
            }
        }
        memcpy(&lastip,&ip,sizeof(lastip));
        lastis4 = is4;
        lastport = ep.port;
    }
    flushText(text,lastport,rep,range);
    return text;
}

inline const char *getnum(const char *s,unsigned &n)
{
    n = 0;
    while (isdigit(*s)) {
        n = n*10+(*s-'0');
        s++;
    }
    return s;
}

inline bool appendv4range(SocketEndpointArray *array,char *str,SocketEndpoint &ep, unsigned defport)
{
    char *s = str;
    unsigned dc = 0;
    unsigned port = defport;
    unsigned rng = 0;
    unsigned rep = 1;
    bool notip = false;
    while (*s) {
        if (*s=='.') {
            dc++;
            s++;
        }
        else if (*s==':') {
            *s = 0;
            s = (char *)getnum(s+1,port);
        }
        else if (*s=='-')
        {
            if (!notip)//don't assume '-' is an ip range delimiter
            {          //Allow '-' in any octet due to ip group support
                *s = 0;
                s = (char *)getnum(s+1,rng);
            }
            else
            {
                notip = true;
                s++;
            }
        }
        else if (*s=='*') {
            *s = 0;
            s = (char *)getnum(s+1,rep);
        }
        else {
            if (!isdigit(*s))
                notip = true;
            s++;
        }
    }
    ep.port = port;
    if (*str) {
        if (!notip&&((dc<3)&&((dc!=1)||(strlen(str)!=1)))) {
            if (!ep.isIp4()) {
                return false;
            }
            StringBuffer tmp;
            ep.getHostText(tmp);
            dc++;
            for (;;) {
                if (tmp.length()==0)
                    return false;
                if (tmp.charAt(tmp.length()-1)=='.')
                    if (--dc==0)
                        break;
                tmp.setLength(tmp.length()-1);
            }
            tmp.append(str);
            if (rng) {
                tmp.appendf("-%d",rng);
                rep = ep.ipsetrange(tmp.str());
            }
            else
                ep.ipset(tmp.str());
        }
        else if (rng) { // not nice as have to add back range (must be better way - maybe ipincrementto) TBD
            StringBuffer tmp;
            tmp.appendf("%s-%d",str,rng);
            rep = ep.ipsetrange(tmp.str());
        }
        else if (*str)
            ep.ipset(str);
        if (ep.isNull())
            ep.port = 0;
        for (unsigned i=0;i<rep;i++) {
            array->append(ep);
            if (rng)
                ep.ipincrement(1);
        }
    }
    else  {// just a port change
        if (ep.isNull())        // avoid null values with ports
            ep.port = 0;
        array->append(ep);
    }
    return true;
}

bool SocketEndpointArray::fromName(const char *name, unsigned defport)
{
    // Lookup a single name that may resolve to multiple IPs in a headless service scenario
    StringArray portSplit;
    portSplit.appendList(name, ":");
    switch (portSplit.ordinality())
    {
    case 2:
        defport = atoi(portSplit.item(1));
        name = portSplit.item(0);
        // fallthrough
    case 1:
        break;
    default:
        throw MakeStringException(-1, "Invalid name %s SocketEndpointArray::fromName", name);
    }
#if defined(__linux__) || defined (__APPLE__) || defined(getaddrinfo)
    if (IP4only)
#endif
    {
        CriticalBlock c(hostnamesect);
        hostent * entry = gethostbyname(name);
        if (entry && entry->h_addr_list[0])
        {
            unsigned ptr = 0;
            for (;;)
            {
                ptr++;
                if (entry->h_addr_list[ptr]==NULL)
                    break;
                SocketEndpoint ep;
                ep.setNetAddress(sizeof(unsigned),entry->h_addr_list[ptr]);
                ep.port = defport;
                append(ep);
            }
        }
        return ordinality()>0;
    }
#if defined(__linux__) || defined (__APPLE__) || defined(getaddrinfo)
    unsigned netaddr[4];
    int retCode = getAddressInfo(name, netaddr, true);
    if (0 == retCode)
    {
        SocketEndpoint ep;
        ep.setNetAddress(sizeof(netaddr),netaddr);
        ep.port = defport;
        append(ep);
    }
#endif
    return ordinality()>0;
}

void SocketEndpointArray::fromText(const char *text,unsigned defport) 
{
    // this is quite complicated with (mixed) IPv4 and IPv6
    // only support 'full' IPv6 and no ranges

    if (!text)
        return;
    char *str = strdup(text);
    char *s = str;
    SocketEndpoint ep;
    bool eol = false;
    for (;;) {
        while (isspace(*s)||(*s==','))
            s++;
        if (!*s)
            break;
        char *e=s;
        if (*e=='[') {  // we have a IPv6
            while (*e&&(*e!=']'))
                e++;
            while ((*e!=',')&&!isspace(*e)) {
                if (!*s) {
                    eol = true;
                    break;
                }
                e++;
            }
            *e = 0;
            ep.set(s,defport);
            if (ep.isNull()) {
                // Error TBD
            }
            append(ep);
        }
        else {
            bool hascolon = false;
            bool isv6 = false;
            do {
                if (*e==':') {
                    if (hascolon)
                        isv6 = true;
                    else
                        hascolon = true;
                }
                e++;
                if (!*e) {
                    eol = true;
                    break;
                }
            } while (!isspace(*e)&&(*e!=','));
            *e = 0;
            if (isv6) {
                ep.set(s,defport);
                if (ep.isNull()) {
                    // Error TBD
                }
                append(ep);
            }
            else {
                if (!appendv4range(this,s,ep,defport)) {
                    // Error TBD
                }
            }

        }
        if (eol)
            break;
        s = e+1;
    }
    free(str);
}


bool IpSubNet::set(const char *_net,const char *_mask)
{
    if (!_net||!decodeNumericIP(_net,net)) {    // _net NULL means match everything
        memset(net,0,sizeof(net));
        memset(mask,0,sizeof(mask));
        return (_net==NULL);
    }
    if (!_mask||!decodeNumericIP(_mask,mask)) { // _mask NULL means match exact
        memset(mask,0xff,sizeof(mask));
        return (_mask==NULL);
    }
    if (isIp4(net)!=isIp4(mask))
        return false;
    for (unsigned j=0;j<4;j++)
        if (net[j]&~mask[j])
            return false;
    return true;
}

bool IpSubNet::test(const IpAddress &ip) const
{
    unsigned i;
    if (ip.getNetAddress(sizeof(i),&i)==sizeof(i)) {
        if (!isIp4(net))
            return false;
        return (i&mask[3])==(net[3]&mask[3]);
    }
    unsigned na[4];
    if (ip.getNetAddress(sizeof(na),&na)==sizeof(na)) {
        for (unsigned j=0;j<4;j++)
            if ((na[j]&mask[j])!=(net[j]&mask[j]))
                return false;
        return true;
    }
    return false;
}

StringBuffer &IpSubNet::getNetText(StringBuffer &text) const
{
    char tmp[INET6_ADDRSTRLEN];
    const char *res  = ::isIp4(net) ? _inet_ntop(AF_INET, &net[3], tmp, sizeof(tmp))
                                    : _inet_ntop(AF_INET6, &net, tmp, sizeof(tmp));
    return text.append(res);
}

StringBuffer &IpSubNet::getMaskText(StringBuffer &text) const
{
    char tmp[INET6_ADDRSTRLEN];
    // isIp4(net) is correct here
    const char *res  = ::isIp4(net) ? _inet_ntop(AF_INET, &mask[3], tmp, sizeof(tmp))
                                    : _inet_ntop(AF_INET6, &mask, tmp, sizeof(tmp));
    return text.append(res);
}

bool IpSubNet::isNull() const
{
    for (unsigned i=0;i<4;i++)
        if (net[i]||mask[i])
            return false;
    return true;
}

IpSubNet &queryPreferredSubnet()
{
    return PreferredSubnet;
}

bool setPreferredSubnet(const char *ip,const char *mask)
{
    // also resets cached host IP
    if (PreferredSubnet.set(ip,mask))
    {
        if (!cachehostip.isNull()) 
        {
            cachehostip.ipset(NULL);
            queryHostIP();
        }
        return true;
    }
    else
        return false;
}

StringBuffer &lookupHostName(const IpAddress &ip,StringBuffer &ret)
{
// not a common routine (no Jlib function!) only support IPv4 initially
    unsigned ipa;
    if (ip.getNetAddress(sizeof(ipa),&ipa)==sizeof(ipa)) {
        struct hostent *phostent = gethostbyaddr( (char *) &ipa, sizeof(ipa), PF_INET);
        if (phostent)
            ret.append(phostent->h_name);
        else
            ip.getHostText(ret);
    }
    else
        ip.getHostText(ret);
    return ret;
}

struct SocketEndpointHTElem
{
    IInterface *ii;
    SocketEndpoint ep;
    SocketEndpointHTElem(const SocketEndpoint _ep,IInterface *_ii) { ep.set(_ep); ii = _ii; }
    ~SocketEndpointHTElem() { ::Release(ii); }
};


class jlib_decl CSocketEndpointHashTable : public SuperHashTableOf<SocketEndpointHTElem, SocketEndpoint>, implements ISocketEndpointHashTable
{

        
    virtual void onAdd(void *) {}
    virtual void onRemove(void *e) { delete (SocketEndpointHTElem *)e; }
    
    unsigned getHashFromElement(const void *e) const
    {
        return ((const SocketEndpointHTElem *)e)->ep.hash(0);
    }
    unsigned getHashFromFindParam(const void *fp) const
    {
        return ((const SocketEndpoint *)fp)->hash(0);
    }
    const void * getFindParam(const void *p) const
    {
        return &((const SocketEndpointHTElem *)p)->ep;
    }

    bool matchesFindParam(const void * et, const void *fp, unsigned) const
    {
        return ((const SocketEndpointHTElem *)et)->ep.equals(*(SocketEndpoint *)fp);
    }

    IMPLEMENT_SUPERHASHTABLEOF_REF_FIND(SocketEndpointHTElem,SocketEndpoint);
    
public: 
    IMPLEMENT_IINTERFACE;

    CSocketEndpointHashTable() {}
    ~CSocketEndpointHashTable() { _releaseAll(); }
    
    void add(const SocketEndpoint &ep, IInterface *i)
    {
        SocketEndpointHTElem *e = SuperHashTableOf<SocketEndpointHTElem,SocketEndpoint>::find(&ep);
        if (e) {
            ::Release(e->ii);
            e->ii = i;
        }
        else {
            e = new SocketEndpointHTElem(ep,i);
            SuperHashTableOf<SocketEndpointHTElem,SocketEndpoint>::add(*e);
        }
        
    }

    void remove(const SocketEndpoint &ep)
    {
        SuperHashTableOf<SocketEndpointHTElem,SocketEndpoint>::remove(&ep);
    }

    IInterface *find(const SocketEndpoint &ep)
    {
        SocketEndpointHTElem *e = SuperHashTableOf<SocketEndpointHTElem,SocketEndpoint>::find(&ep);
        if (e) 
            return e->ii;
        return NULL;
    }
    
};


ISocketEndpointHashTable *createSocketEndpointHashTable()
{
    CSocketEndpointHashTable *ht = new CSocketEndpointHashTable;
    return ht;
}


class CSocketConnectWait: implements ISocketConnectWait, public CInterface
{
    Owned<CSocket> sock;
    bool done;
    CTimeMon connecttm;
    unsigned startt;
    bool oneshot;
    bool isopen;    
    int initerr;

    void successfulConnect()
    {
        STATS.connects++;
        STATS.connecttime+=usTick()-startt;
#ifdef _TRACE
        sock->setTraceName();
#endif              
    }

    void failedConnect()
    {
        STATS.failedconnects++;
        STATS.failedconnecttime+=usTick()-startt;
        const char* tracename = sock->tracename;
        THROWJSOCKTARGETEXCEPTION(JSOCKERR_connection_failed);
    }

public:
    IMPLEMENT_IINTERFACE;

    CSocketConnectWait(SocketEndpoint &ep,unsigned connecttimeoutms)
        : connecttm(connecttimeoutms)
    {
        oneshot = (connecttimeoutms==0); // i.e. as long as one connect takes
        done = false;
        startt = usTick();
        sock.setown(new CSocket(ep,sm_tcp,NULL));
        isopen = true;
        initerr = sock->pre_connect(false);
    }

    ISocket *wait(unsigned timems)
    {
        // this is a bit spagetti due to dual timeouts etc
        CTimeMon waittm(timems);
        unsigned refuseddelay = 1;
        bool waittimedout = false;
        bool connectimedout = false;
        do {
            bool connectdone = false;
            unsigned remaining;
            connectimedout = connecttm.timedout(&remaining);
            unsigned waitremaining;
            waittimedout = waittm.timedout(&waitremaining);
            if (oneshot||(waitremaining<remaining))
                remaining = waitremaining;
            int err = 0;
            if (!isopen||initerr)
            {
                isopen = true;
                err = initerr?initerr:sock->pre_connect(false);
                initerr = 0;
                if ((err == JSE_INPROGRESS)||(err == JSE_WOULDBLOCK))
                    err = 0; // continue
                else
                {
                    if (err==0)
                        connectdone = true; // done immediately
                    else if(!oneshot) //  probably ECONNREFUSED but treat all errors same
                        refused_sleep((waitremaining==remaining)?waittm:connecttm,refuseddelay); // this stops becoming cpu bound
                }
            }
            if (!connectdone&&(err==0))
            {
                SOCKET s = sock->sock;
#ifdef _USE_SELECT
                CHECKSOCKRANGE(s);
                T_FD_SET fds;
                struct timeval tv;
                XFD_ZERO(&fds);
                FD_SET((unsigned)s, &fds);
                T_FD_SET except;
                XFD_ZERO(&except);
                FD_SET((unsigned)s, &except);
                tv.tv_sec = remaining / 1000;
                tv.tv_usec = (remaining % 1000)*1000;
                int rc = ::select( s + 1, NULL, (fd_set *)&fds, (fd_set *)&except, &tv );
#else
                struct pollfd fds[1];
                fds[0].fd = s;
                fds[0].events = POLLOUT;
                fds[0].revents = 0;
                int rc = ::poll(fds, 1, remaining);
#endif
                if (rc==0) 
                    break; // timeout
                done = true;
                err = 0;
                if (rc>0)
                {
                    // select/poll succeeded - return error from socket (0 if connected)
                    socklen_t errlen = sizeof(err);
                    rc = getsockopt(s, SOL_SOCKET, SO_ERROR, (char *)&err, &errlen); // check for error
                    if ((rc!=0)&&!err) 
                        err = SOCKETERRNO();  // some implementations of getsockopt duff
                    if (err&&!oneshot) //  probably ECONNREFUSED but treat all errors same
                        refused_sleep((waitremaining==remaining)?waittm:connecttm,refuseddelay); // this stops becoming cpu bound
                }
                else
                {   // select/poll failed
                    err = SOCKETERRNO();
                    if (err != JSE_INTR)
                    {
                        LOGERR(err,2,"CSocketConnectWait ::select/poll");
                        sock->errclose();
                        isopen = false;
                        if (!oneshot)
                            connectimedout = true;
                        break;
                    }
                }
            }
            if (err==0)
            {
                err = sock->post_connect();
                if (err==0)
                {
                    successfulConnect();
                    return sock.getClear();
                }
            }
            sock->errclose();
            isopen = false;
        } while (!waittimedout&&!connectimedout&&!oneshot);
        if (connectimedout)
        {
            STATS.failedconnects++;
            STATS.failedconnecttime+=usTick()-startt;
            const char* tracename = sock->tracename;
            THROWJSOCKTARGETEXCEPTION(JSOCKERR_connection_failed);
        }
        return NULL;
    }

};

ISocketConnectWait *nonBlockingConnect(SocketEndpoint &ep,unsigned connecttimeoutms)
{
    return new CSocketConnectWait(ep,connecttimeoutms);
}



int wait_multiple(bool isRead,               //IN   true if wait read, false it wait write
                  UnsignedArray &socks,      //IN   sockets to be checked for readiness
                  unsigned timeoutMS,        //IN   timeout
                  UnsignedArray &readySocks) //OUT  sockets ready
{
    aindex_t numSocks = socks.length();
    if (numSocks == 0)
        THROWJSOCKEXCEPTION(JSOCKERR_bad_address);

#ifdef _DEBUG
    StringBuffer dbgSB("wait_multiple() on sockets :");
#endif
#ifdef _USE_SELECT
    SOCKET maxSocket = 0;
    T_FD_SET fds;
    XFD_ZERO(&fds);
    //Add each SOCKET in array to T_FD_SET
    for (aindex_t idx = 0; idx < numSocks; idx++)
    {
#ifdef _DEBUG
        dbgSB.appendf(" %d",socks.item(idx));
#endif
        SOCKET s = socks.item(idx);
        CHECKSOCKRANGE(s);
        maxSocket = s > maxSocket ? s : maxSocket;
        FD_SET((unsigned)s, &fds);
    }
#else
    struct pollfd *fds = nullptr;
    try
    {
        fds = new pollfd[numSocks];
    }
    catch (const std::bad_alloc &e)
    {
        int err = SOCKETERRNO();
        throw MakeStringException(-1,"wait_multiple::fds malloc failure %d", err);
    }
    for (aindex_t idx = 0; idx < numSocks; idx++)
    {
#ifdef _DEBUG
        dbgSB.appendf(" %d",socks.item(idx));
#endif
        SOCKET s = socks.item(idx);
        fds[idx].fd = s;
        fds[idx].events = isRead ? POLLINX : POLLOUT;
        fds[idx].revents = 0;
    }
#endif
#ifdef _DEBUG
    DBGLOG("%s",dbgSB.str());
#endif
    //Check socket states
    int res = 0;
#ifdef _USE_SELECT
    if (timeoutMS == WAIT_FOREVER)
        res = ::select( maxSocket + 1, isRead ? (fd_set *)&fds : NULL, isRead ? NULL : (fd_set *)&fds, NULL, NULL );
    else
    {
        struct timeval tv;
        tv.tv_sec = timeoutMS / 1000;
        tv.tv_usec = (timeoutMS % 1000)*1000;
        res = ::select( maxSocket + 1,  isRead ? (fd_set *)&fds : NULL, isRead ? NULL : (fd_set *)&fds, NULL, &tv );
    }
#else
    res = poll(fds, numSocks, timeoutMS);
#endif
    if (res > 0)
    {
#ifdef _DEBUG
        StringBuffer dbgSB("wait_multiple() ready socket(s) :");
#endif
        //Build up list of socks which are ready for accept read/write without blocking
        for (aindex_t idx = 0; idx < numSocks; idx++)
        {
            SOCKET s = socks.item(idx);
#ifdef _USE_SELECT
            if (FD_ISSET(s, &fds))
#else
            if (fds[idx].revents)
#endif
            {
#ifdef _DEBUG
                dbgSB.appendf(" %d",s);
#endif
                readySocks.append(s);
                if ((int) readySocks.length() == res)
                    break;
            }
        }
#ifdef _DEBUG
        DBGLOG("%s",dbgSB.str());
#endif
        res = readySocks.ordinality();
    }
    else if (res == SOCKET_ERROR)
    {
        res = 0; // dont return negative on failure
        int err = SOCKETERRNO();
        if (err != JSE_INTR)
        {
#ifndef _USE_SELECT
            delete [] fds;
#endif
            throw MakeStringException(-1,"wait_multiple::select/poll error %d", err);
        }
    }
#ifndef _USE_SELECT
    delete [] fds;
#endif
    return res;
}

//Given a list of sockets, wait until any one or more are ready to be read (wont block)
//returns 0 if timeout, number of waiting sockets otherwise
int wait_read_multiple(UnsignedArray &socks,        //IN   sockets to be checked for readiness
                       unsigned timeoutMS,          //IN   timeout
                       UnsignedArray &readySocks)   //OUT  sockets ready to be read
{
    return wait_multiple(true, socks, timeoutMS, readySocks);
}

int wait_write_multiple(UnsignedArray &socks,       //IN   sockets to be checked for readiness
                       unsigned timeoutMS,          //IN   timeout
                       UnsignedArray &readySocks)   //OUT  sockets ready to be written
{
    return wait_multiple(false, socks, timeoutMS, readySocks);
}

inline bool isIPV4Internal(const char *ip)
{
    struct sockaddr_in sa;
    return 0 != inet_pton(AF_INET, ip, &sa.sin_addr);
}

inline bool isIPV6Internal(const char *ip)
{
    struct sockaddr_in6 sa;
    return 0 != inet_pton(AF_INET6, ip, &sa.sin6_addr);
}

bool isIPV4(const char *ip)
{
    if (isEmptyString(ip))
        return false;
    return isIPV4Internal(ip);
}

bool isIPV6(const char *ip)
{
    if (isEmptyString(ip))
        return false;
    return isIPV6Internal(ip);
}

bool isIPAddress(const char *ip)
{
    if (isEmptyString(ip))
        return false;
    return isIPV4(ip) || isIPV6(ip);
}


class CAllowListHandler : public CSimpleInterfaceOf<IAllowListHandler>, implements IAllowListWriter
{
    typedef CSimpleInterfaceOf<IAllowListHandler> PARENT;

    struct PairHasher
    {
        template <class T1, class T2>
        std::size_t operator () (std::pair<T1, T2> const &pair) const
        {
            std::size_t h1 = std::hash<T1>()(pair.first);
            std::size_t h2 = std::hash<T2>()(pair.second);
            return h1 ^ h2;
        }
    };

    using AllowListHT = std::unordered_set<std::pair<std::string, unsigned __int64>, PairHasher>;
    AllowListPopulateFunction populateFunc;
    AllowListFormatFunction roleFormatFunc;
    std::unordered_set<std::pair<std::string, unsigned __int64>, PairHasher> allowList;
    std::unordered_set<std::string> IPOnlyAllowList;
    bool allowAnonRoles = false;
    mutable CriticalSection populatedCrit;
    mutable bool populated = false;
    mutable bool enabled = true;

    void ensurePopulated() const
    {
        // should be called within CS
        if (populated)
            return;
        // NB: want to keep this method const, as used by isXX functions that are const, but if need to refresh it's effectively mutable
        enabled = populateFunc(* const_cast<IAllowListWriter *>((const IAllowListWriter *)this));
        populated = true;
    }
public:
    IMPLEMENT_IINTERFACE_O_USING(PARENT);

    CAllowListHandler(AllowListPopulateFunction _populateFunc, AllowListFormatFunction _roleFormatFunc) : populateFunc(_populateFunc), roleFormatFunc(_roleFormatFunc)
    {
    }
// IAllowListHandler impl.
    virtual bool isAllowListed(const char *ip, unsigned __int64 role, StringBuffer *responseText) const override
    {
        CriticalBlock block(populatedCrit);
        ensurePopulated();
        if (0 == role) // unknown, can only check ip
        {
            if (allowAnonRoles)
            {
                const auto &it = IPOnlyAllowList.find(ip);
                if (it != IPOnlyAllowList.end())
                    return true;
            }
        }
        else
        {
            const auto &it = allowList.find({ip, role});
            if (it != allowList.end())
                return true;
        }

        // if !enabled and no responseText supplied, generate response and warn that disabled
        StringBuffer disabledResponseText;
        if (!enabled && !responseText)
            responseText = &disabledResponseText;

        if (responseText)
        {
            responseText->append("Access denied! [server ip=");
            queryHostIP().getHostText(*responseText);
            responseText->append(", client ip=");
            responseText->append(ip);
            if (role)
            {
                responseText->append(", role=");
                if (roleFormatFunc)
                    roleFormatFunc(*responseText, role);
                else
                    responseText->append(role);
            }
            responseText->append("] not in allowlist");
        }

        if (enabled)
            return false;
        else
        {
            OWARNLOG("Allowlist is disabled, ignoring: %s", responseText->str());
            return true;
        }
    }
    virtual StringBuffer &getAllowList(StringBuffer &out) const override
    {
        CriticalBlock block(populatedCrit);
        ensurePopulated();
        for (const auto &it: allowList)
        {
            out.append(it.first.c_str()).append(", ");
            if (roleFormatFunc)
                roleFormatFunc(out, it.second);
            else
                out.append(it.second);
            out.newline();
        }
        out.newline().appendf("Allowlist is currently: %s", enabled ? "enabled" : "disabled").newline();
        return out;
    }
    virtual void refresh() override
    {
        /* NB: clear only, so that next usage will re-populated
         * Do not want to repopulate now, because refresh() is likely called within a update write transaction
         */
        CriticalBlock block(populatedCrit);
        enabled = true;
        allowList.clear();
        IPOnlyAllowList.clear();
        populated = false;
    }
// IAllowListWriter impl.
    virtual void add(const char *ip, unsigned __int64 role) override
    {
        // NB: called via populateFunc, which is called whilst populatedCrit is locked.
        allowList.insert({ ip, role });
        if (allowAnonRoles)
            IPOnlyAllowList.insert(ip);
    }
    virtual void setAllowAnonRoles(bool tf) override
    {
        allowAnonRoles = tf;
        refresh();
    }
};

IAllowListHandler *createAllowListHandler(AllowListPopulateFunction populateFunc, AllowListFormatFunction roleFormatFunc)
{
    return new CAllowListHandler(populateFunc, roleFormatFunc);
}

extern jlib_decl void shutdownAndCloseNoThrow(ISocket * optSocket)
{
    if (!optSocket)
        return;

    optSocket->shutdownNoThrow();
    try
    {
        optSocket->close();
    }
    catch (IException * e)
    {
        e->Release();
    }
}
