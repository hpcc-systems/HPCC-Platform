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


#include "platform.h"
#ifdef _VER_C5
#include <clwclib.h>
#else
#include "platform.h"
#include <stdio.h>
#endif
#include <algorithm>

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
#include "build-config.h"

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
# endif
#endif

// various options 

#define CONNECT_TIMEOUT_REFUSED_WAIT    1000        // maximum to sleep on connect_timeout
#define TRACE_SLOW_BLOCK_TRANSFER   

#define DEFAULT_CONNECT_TIME    (100*1000)      // for connect_wait



#ifndef _WIN32 
#define BLOCK_POLLED_SINGLE_CONNECTS  // NB this is much slower in windows
#define CENTRAL_NODE_RANDOM_DELAY
#else
#define USERECVSEM      // to singlethread BF_SYNC_TRANSFER_PUSH 
#endif

#ifdef _DEBUG
//#define SOCKTRACE
//#define EPOLLTRACE
#endif

#ifdef _TESTING
#define _TRACE
#endif

#ifdef _TRACE
#define THROWJSOCKEXCEPTION(exc) \
  { StringBuffer msg; \
    msg.appendf("Target: %s, Raised in: %s, line %d",tracename ,__FILE__, __LINE__); \
    IJSOCK_Exception *e = new SocketException(exc,msg.str());\
    throw e; }
#define THROWJSOCKEXCEPTION2(exc) \
  { StringBuffer msg; \
    msg.appendf("Raised in: %s, line %d",__FILE__, __LINE__); \
    IJSOCK_Exception *e = new SocketException(exc,msg.str());\
    throw e; }
#define LOGERR(err,ref,info) LogErr(err,ref,info,__LINE__,NULL)
#define LOGERR2(err,ref,info) LogErr(err,ref,info,__LINE__,tracename)
    
#else
#define THROWJSOCKEXCEPTION(exc) \
  { IJSOCK_Exception *e = new SocketException(exc);\
    throw e; }
#define THROWJSOCKEXCEPTION2(exc) THROWJSOCKEXCEPTION(exc)
#define LOGERR(err,ref,info)
#define LOGERR2(err,ref,info)
#endif


JSocketStatistics STATS;
static bool IP4only=false;              // slighly faster if we know no IPv6
static bool IP6preferred=false;         // e.g. for DNS and socket create

IpSubNet PreferredSubnet(NULL,NULL);    // set this if you prefer a particular subnet for debugging etc
                                        // e.g. PreferredSubnet("192.168.16.0", "255.255.255.0")

static atomic_t pre_conn_unreach_cnt = ATOMIC_INIT(0);    // global count of pre_connect() ENETUNREACH error

#define IPV6_SERIALIZE_PREFIX (0x00ff00ff)

inline void LogErr(unsigned err,unsigned ref,const char *info,unsigned lineno,const char *tracename)
{
    if (err) 
        PROGLOG("jsocket(%d,%d)%s%s err = %d%s%s",ref,lineno,
           (info&&*info)?" ":"",(info&&*info)?info:"",err,
           (tracename&&*tracename)?" : ":"",(tracename&&*tracename)?tracename:"");
}
    

class jlib_thrown_decl SocketException: public CInterface, public IJSOCK_Exception
{
public:
    IMPLEMENT_IINTERFACE;

    SocketException(int code,const char *_msg=NULL) : errcode(code) 
    {
        if (_msg) 
            msg = strdup(_msg);
        else
            msg = NULL;
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
#ifndef ETIMEDOUT
#define ETIMEDOUT WSAETIMEDOUT
#define ECONNREFUSED WSAECONNREFUSED
#endif
#define XFD_ZERO(s) FD_ZERO(s)
#define SEND_FLAGS 0
#define BADSOCKERR(err) ((err==WSAEBADF)||(err==WSAENOTSOCK))
#define CHECKSOCKRANGE(s)
#elif defined(__FreeBSD__) || defined(__APPLE__)
#define XFD_SETSIZE FD_SETSIZE
#define T_FD_SET fd_set
#define XFD_ZERO(s) FD_ZERO(s)
#define T_SOCKET int
#define SEND_FLAGS (MSG_NOSIGNAL)
#define BADSOCKERR(err) ((err==EBADF)||(err==ENOTSOCK))
#define CHECKSOCKRANGE(s)
#else
#define XFD_SETSIZE 32768
struct xfd_set { __fd_mask fds_bits[XFD_SETSIZE / __NFDBITS]; }; // define our own
// linux 64 bit
#ifdef __linux__
#ifdef __x86_64__
#undef __FDMASK
#define __FDMASK(d)     (1UL << ((d) % __NFDBITS))
#undef __FDELT
#define __FDELT(d)      ((d) / __NFDBITS)
#undef __FD_SET
#define __FD_SET(d, s)     (__FDS_BITS (s)[__FDELT(d)] |= __FDMASK(d))
#undef __FD_ISSET
#define __FD_ISSET(d, s)   ((__FDS_BITS (s)[__FDELT(d)] & __FDMASK(d)) != 0)
#endif
#define CHECKSOCKRANGE(s) { if (s>=XFD_SETSIZE) THROWJSOCKEXCEPTION2(JSOCKERR_handle_too_large); }
#endif
// end 64 bit
#define T_FD_SET xfd_set
#define XFD_ZERO(s) memset(s,0,sizeof(xfd_set))
#define T_SOCKET int
#define SEND_FLAGS (MSG_NOSIGNAL)
#define BADSOCKERR(err) ((err==EBADF)||(err==ENOTSOCK))
#endif
#ifdef CENTRAL_NODE_RANDOM_DELAY
static SocketEndpointArray CentralNodeArray;
#endif
enum SOCKETMODE { sm_tcp_server, sm_tcp, sm_udp_server, sm_udp, sm_multicast_server, sm_multicast};

class CSocket: public CInterface, public ISocket
{
public:
    IMPLEMENT_IINTERFACE;

    static          CriticalSection crit;
protected:
    friend class CSocketConnectWait;
    enum { ss_open, ss_shutdown, ss_close, ss_pre_open } state;
    T_SOCKET        sock;
    char*           hostname;   // host address
    unsigned short  hostport;   // host port
    SOCKETMODE      sockmode;
    IpAddress       targetip;
    SocketEndpoint  returnep;   // set by set_return_addr

    MCASTREQ    *   mcastreq;
    size32_t        nextblocksize;
    unsigned        blockflags;
    unsigned        blocktimeoutms;
    bool            owned;
    enum            {accept_not_cancelled, accept_cancel_pending, accept_cancelled} accept_cancel_state;
    bool            in_accept;
    bool            nonblocking;
    bool            nagling;
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

    void        read(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read,unsigned timeoutsecs);
    void        readtms(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read, unsigned timedelaysecs);
    void        read(void* buf, size32_t size);
    size32_t    write(void const* buf, size32_t size);
    size32_t    writetms(void const* buf, size32_t size, unsigned timeoutms=WAIT_FOREVER);
    size32_t    write_multiple(unsigned num,void const**buf, size32_t *size);
    size32_t    udp_write_to(const SocketEndpoint &ep,void const* buf, size32_t size);
    void        close();
    void        errclose();
    bool        connectionless() { return (sockmode!=sm_tcp)&&(sockmode!=sm_tcp_server); }
    void        shutdown(unsigned mode);

    ISocket*    accept(bool allowcancel);
    int         wait_read(unsigned timeout);
    int         wait_write(unsigned timeout);
    int         name(char *name,size32_t namemax);
    int         peer_name(char *name,size32_t namemax);
    SocketEndpoint &getPeerEndpoint(SocketEndpoint &ep);
    IpAddress & getPeerAddress(IpAddress &addr);
    void        set_return_addr(int port,const char *name);  // sets returnep
    void        cancel_accept();
    size32_t    get_max_send_size();
    bool        set_nonblock(bool on=true);
    bool        set_nagle(bool on);
    void        set_linger(int lingersecs); 
    void        set_keep_alive(bool set);
    virtual void set_inherit(bool inherit=false);
    virtual bool check_connection();

    
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

    CSocket(const SocketEndpoint &_ep,SOCKETMODE smode,const char *name);
    CSocket(T_SOCKET new_sock,SOCKETMODE smode,bool _owned);

    virtual ~CSocket();

    unsigned OShandle()
    {
        return (unsigned)sock;
    }

private:

    int closesock()
    {
        if (sock!=INVALID_SOCKET) {
            T_SOCKET s = sock;
            sock = INVALID_SOCKET;
            STATS.activesockets--;
    #ifdef SOCKTRACE
            PROGLOG("SOCKTRACE: Closing socket %x %d (%p)", s, s, this);
    #endif
    #ifdef _WIN32
            return ::closesocket(s);
    #else
            return ::close(s);
    #endif
        }
        else
            return 0;
    }
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

#define ERRNO() WSAGetLastError()
#ifndef EADDRINUSE
#define EADDRINUSE WSAEADDRINUSE
#define ECONNRESET WSAECONNRESET
#define ECONNABORTED WSAECONNABORTED
#define ENOTCONN WSAENOTCONN
#define EWOULDBLOCK WSAEWOULDBLOCK
#define EINPROGRESS WSAEINPROGRESS
#define ENETUNREACH WSAENETUNREACH
#define ENOTSOCK WSAENOTSOCK
#endif
#define EINTRCALL WSAEINTR

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
#define _inet_ntop inet_ntop
#define _inet_pton inet_pton

#define in_addr6 in6_addr 

typedef union {
    struct sockaddr sa;
    struct sockaddr_in6 sin6;
    struct sockaddr_in sin;
} J_SOCKADDR;

#define DEFINE_SOCKADDR(name) J_SOCKADDR name; memset(&name,0,sizeof(J_SOCKADDR))


#define EINTRCALL EINTR
#define ERRNO() (errno)
#ifndef INADDR_NONE
#define INADDR_NONE (-1)
#endif
#endif

#ifndef INET6_ADDRSTRLEN
#define INET6_ADDRSTRLEN 65
#endif


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
    int err = ERRNO();
    LOGERR2(err,1,"avail_read");
    return 0;
}

#define PRE_CONN_UNREACH_ELIM  100

int CSocket::pre_connect (bool block)
{
    if (NULL == hostname || '\0' == (*hostname))
    {
        StringBuffer err;
        err.appendf("CSocket::pre_connect - Invalid/missing host IP address raised in : %s, line %d",__FILE__, __LINE__);
        IJSOCK_Exception *e = new SocketException(JSOCKERR_bad_netaddr,err.str());
        throw e;
    }

    DEFINE_SOCKADDR(u);
    if (targetip.isNull()) {
        set_return_addr(hostport,hostname);
        targetip.ipset(returnep);
    }
    socklen_t ul = setSockAddr(u,targetip,hostport);
    sock = ::socket(u.sa.sa_family, SOCK_STREAM, targetip.isIp4()?0:PF_INET6);
    owned = true;
    state = ss_pre_open;            // will be set to open by post_connect
    if (sock == INVALID_SOCKET) {
        int err = ERRNO();
        THROWJSOCKEXCEPTION(err);
    }
    STATS.activesockets++;
    int err = 0;
    set_nonblock(!block);
    int rc = ::connect(sock, &u.sa, ul);
    if (rc==SOCKET_ERROR) {
        err = ERRNO();
        if ((err != EINPROGRESS)&&(err != EWOULDBLOCK)&&(err != ETIMEDOUT)&&(err!=ECONNREFUSED)) {   // handled by caller
            if (err != ENETUNREACH) {
                atomic_set(&pre_conn_unreach_cnt, 0);
                LOGERR2(err,1,"pre_connect");
            } else {
                int ecnt = atomic_read(&pre_conn_unreach_cnt);
                if (ecnt <= PRE_CONN_UNREACH_ELIM) {
                    atomic_inc(&pre_conn_unreach_cnt);
                    LOGERR2(err,1,"pre_connect network unreachable");
                }
            }
        } else
            atomic_set(&pre_conn_unreach_cnt, 0);
    } else
        atomic_set(&pre_conn_unreach_cnt, 0);
#ifdef SOCKTRACE
    PROGLOG("SOCKTRACE: pre-connected socket%s %x %d (%p) err=%d", block?"(block)":"", sock, sock, this, err);
#endif
    return err;
}

int CSocket::post_connect ()
{
    set_nonblock(false);
    int err = 0;
    socklen_t  errlen = sizeof(err);
    int rc = getsockopt(sock, SOL_SOCKET, SO_ERROR, (char *)&err, &errlen); // check for error
    if ((rc!=0)&&!err)
        err = ERRNO();  // some implementations of getsockopt duff
    if (err==0) {
        nagling = true;
        set_nagle(false);
        state = ss_open;
    }
    else if ((err!=ETIMEDOUT)&&(err!=ECONNREFUSED)) // handled by caller
        LOGERR2(err,1,"post_connect");
    return err;
}


void CSocket::open(int listen_queue_size,bool reuseports)
{
    if (IP6preferred)
        sock = ::socket(AF_INET6, connectionless()?SOCK_DGRAM:SOCK_STREAM, PF_INET6);
    else
        sock = ::socket(AF_INET, connectionless()?SOCK_DGRAM:SOCK_STREAM, 0);
    if (sock == INVALID_SOCKET) {
        THROWJSOCKEXCEPTION(ERRNO());
    }
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
    if (reuseports) {
        int on = 1;
        setsockopt( sock, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on));
    }

    DEFINE_SOCKADDR(u);
    socklen_t  ul;
    if (hostname) {
        if (targetip.isNull()) {
            set_return_addr(hostport,hostname);
            targetip.ipset(returnep);
        }
        ul = setSockAddr(u,targetip,hostport);
    }
    else 
        ul = setSockAddrAny(u,hostport);
    int saverr;
    if (::bind(sock, &u.sa, ul) != 0) {
        saverr = ERRNO();
        if (saverr==EADDRINUSE) {   // don't log as error (some usages probe ports)
ErrPortInUse:
            closesock();
            char msg[1024]; 
            sprintf(msg,"Target: %s, port = %d, Raised in: %s, line %d",tracename,(int)hostport,__FILE__, __LINE__); 
            IJSOCK_Exception *e = new SocketException(JSOCKERR_port_in_use,msg);
            throw e; 
        }
        else {
            closesock();
            THROWJSOCKEXCEPTION(saverr);
        }
    }
    if (!connectionless()) {
        if (::listen(sock, listen_queue_size) != 0) {
            saverr = ERRNO();
            if (saverr==EADDRINUSE)
                goto ErrPortInUse;
            closesock();
            THROWJSOCKEXCEPTION(saverr);
        }
    }
    if (mcastreq) {
        if (setsockopt(sock, IPPROTO_IP, IP_ADD_MEMBERSHIP,(char*)mcastreq, sizeof(*mcastreq))!=0) {
            saverr = ERRNO();
            closesock();
            THROWJSOCKEXCEPTION(saverr);
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
        ERRLOG("invalid accept, state = %d",(int)state);
        THROWJSOCKEXCEPTION(JSOCKERR_not_opened);
    }
    if (connectionless()) {
        THROWJSOCKEXCEPTION(JSOCKERR_connectionless_socket);
    }
    T_SOCKET newsock;
    loop {
        in_accept = true;
        newsock = (sock!=INVALID_SOCKET)?::accept(sock, NULL, NULL):INVALID_SOCKET;
        in_accept = false;
    #ifdef SOCKTRACE
        PROGLOG("SOCKTRACE: accept created socket %x %d (%p)", newsock,newsock,this);
    #endif

        if (newsock!=INVALID_SOCKET) {
            if ((sock==INVALID_SOCKET)||(accept_cancel_state==accept_cancel_pending)) {
                ::close(newsock);
                newsock=INVALID_SOCKET;
            }
            else {
                accept_cancel_state = accept_not_cancelled;
                break;
            }
        }
        int saverr;
        saverr = ERRNO();
        if ((sock==INVALID_SOCKET)||(accept_cancel_state==accept_cancel_pending)) {
            accept_cancel_state = accept_cancelled;
            if (allowcancel)
                return NULL;
            THROWJSOCKEXCEPTION(JSOCKERR_cancel_accept);
        }
        if (saverr != EINTRCALL) {
            accept_cancel_state = accept_not_cancelled;
            THROWJSOCKEXCEPTION(saverr);
        }
    }
    if (state != ss_open) {
        accept_cancel_state = accept_cancelled;
        if (allowcancel)
            return NULL;
        THROWJSOCKEXCEPTION(JSOCKERR_cancel_accept);
    }
    CSocket *ret = new CSocket(newsock,sm_tcp,true);
    ret->set_inherit(false);
    return ret;

}


void CSocket::set_linger(int lingertime)
{
    struct linger l;
    l.l_onoff = (lingertime>=0)?1:0;
    l.l_linger = (lingertime>=0)?lingertime:0;
    if (setsockopt(sock, SOL_SOCKET, SO_LINGER, (char*)&l, sizeof(l)) != 0) {
        WARNLOG("Linger not set");
    }
}

void CSocket::set_keep_alive(bool set)
{
    int on=set?1:0;
    if (setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, (char*)&on, sizeof(on)) != 0) {
        WARNLOG("KeepAlive not set");
    }
}


int CSocket::name(char *retname,size32_t namemax)
{
    if (!retname)
        namemax = 0;
    if (namemax)
        retname[0] = 0;
    if (state != ss_open) {
        THROWJSOCKEXCEPTION(JSOCKERR_not_opened);
    }
    DEFINE_SOCKADDR(u);
    socklen_t ul = sizeof(u);
    if (::getsockname(sock,&u.sa, &ul)<0) {
        THROWJSOCKEXCEPTION(ERRNO());
    }
    SocketEndpoint ep;
    getSockAddrEndpoint(u,ul,ep);
    if (namemax>=1)
    {
        StringBuffer s;
        ep.getIpText(s);
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
        returnep.getIpText(s);
        ret =  returnep.port;
    }   
    else {
        DEFINE_SOCKADDR(u);
        socklen_t ul = sizeof(u);       
        if (::getpeername(sock,&u.sa, &ul)<0)           
            return -1;      // don't log as used to test socket
        SocketEndpoint ep;
        getSockAddrEndpoint(u,ul,ep);
        ep.getIpText(s);
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
        THROWJSOCKEXCEPTION(JSOCKERR_not_opened);
    }
    StringBuffer s;
    if (sockmode==sm_udp_server) { // udp server
        ep.set(returnep);
    }   
    else {
        DEFINE_SOCKADDR(u);
        socklen_t ul = sizeof(u);       
        if (::getpeername(sock,&u.sa, &ul)<0) {         
            DBGLOG("getpeername failed %d",ERRNO());
            ep.set(NULL, 0);
        }
        else
            getSockAddrEndpoint(u,ul,ep);
    }
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
        IJSOCK_Exception *e = new SocketException(JSOCKERR_bad_address); // don't use THROWJSOCKEXCEPTION here
        throw e;
    }
    returnep.port = port;
}



void CSocket::cancel_accept()
{
    if (connectionless()) {
        THROWJSOCKEXCEPTION(JSOCKERR_connectionless_socket);
    }
#ifdef SOCKTRACE
    PROGLOG("SOCKTRACE: Cancel accept socket %x %d (%p)", sock, sock, this);
#endif
    if (!in_accept) {
        accept_cancel_state = accept_cancelled;
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
            Sleep(remaining/4); // towards end of timeout approach gradually
    }
}

bool CSocket::connect_timeout( unsigned timeout, bool noexception)
{
    // simple connect with timeout (no fancy stuff!)
    unsigned startt = usTick();
    CTimeMon tm(timeout);
    unsigned remaining;
    unsigned refuseddelay = 1;
    int err;
    while (!tm.timedout(&remaining)) {
        err = pre_connect(false);
        if ((err == EINPROGRESS)||(err == EWOULDBLOCK)) {
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
            if (rc>0) {
                // select succeeded - return error from socket (0 if connected)
                socklen_t errlen = sizeof(err);
                rc = getsockopt(sock, SOL_SOCKET, SO_ERROR, (char *)&err, &errlen); // check for error
                if ((rc!=0)&&!err) 
                    err = ERRNO();  // some implementations of getsockopt duff
                if (err) //  probably ECONNREFUSED but treat all errors same
                    refused_sleep(tm,refuseddelay);
            }
            else if (rc<0) { 
                err = ERRNO();
                LOGERR2(err,2,"::select");
            }
        }
        if (err==0) {
            err = post_connect();
            if (err==0) {
                STATS.connects++;
                STATS.connecttime+=usTick()-startt;
#ifdef _TRACE
                char peer[256];
                peer[0] = 'C';
                peer[1] = '!';
                strcpy(peer+2,hostname?hostname:"(NULL)");
                free(tracename);
                tracename = strdup(peer);
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
        THROWJSOCKEXCEPTION(JSOCKERR_connection_failed);
    return false;
}


ISocket*  ISocket::connect_timeout(const SocketEndpoint &ep,unsigned timeout)
{
    if (ep.isNull()||(ep.port==0))
        THROWJSOCKEXCEPTION2(JSOCKERR_bad_address);
    Owned<CSocket> sock = new CSocket(ep,sm_tcp,NULL);
    sock->connect_timeout(timeout,false);
    return sock.getClear();
}

#define POLLTIME 50



void CSocket::connect_wait(unsigned timems)
{
    // simple connect with timeout (no fancy stuff!)
    unsigned startt = usTick();
    CTimeMon tm(timems);
    bool exit = false;
    int err;
    unsigned refuseddelay = 1;
    while (!exit) {
#ifdef CENTRAL_NODE_RANDOM_DELAY
        ForEachItemIn(cn,CentralNodeArray) {
            const SocketEndpoint &ep=CentralNodeArray.item(cn);
            if (ep.ipequals(targetip)) {
                unsigned sleeptime = getRandom() % 1000;
                StringBuffer s;
                ep.getIpText(s);
                PrintLog("Connection to central node %s - sleeping %d milliseconds", s.str(), sleeptime);
                Sleep(sleeptime);           
                break;
            }
        }
#endif
        unsigned remaining;
        exit = tm.timedout(&remaining);
        bool blockselect = exit;                            // if last time round block
        {
            CriticalBlock block(crit);
            if (++connectingcount>4)
                blockselect = true;
        }
        err = pre_connect(blockselect);             
        if (blockselect) {
            if (err&&!exit)
                refused_sleep(tm,refuseddelay); //  probably ECONNREFUSED but treat all errors same
        }
        else {
            unsigned timeoutms = (exit||(remaining<10000))?10000:remaining;
            unsigned polltime = 1;
            while (!blockselect && ((err == EINPROGRESS)||(err == EWOULDBLOCK))) {
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
                if (rc>0) {
                    // select succeeded - return error from socket (0 if connected)
                    socklen_t errlen = sizeof(err);
                    rc = getsockopt(sock, SOL_SOCKET, SO_ERROR, (char *)&err, &errlen); // check for error
                    if ((rc!=0)&&!err)
                        err = ERRNO();  // some implementations of getsockopt duff
                    if (err)
                        refused_sleep(tm,refuseddelay); //  probably ECONNREFUSED but treat all errors same
                    break;
                }
                if (rc<0) { 
                    err = ERRNO();
                    LOGERR2(err,2,"::select");
                    break;
                }
                if (!timeoutms) {
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
                if (polltime>POLLTIME/2)
                    polltime = POLLTIME;
                else
                    polltime *= 2;
    #endif
            }
        }
        {
            CriticalBlock block(crit);
            --connectingcount;
        }
        if (err==0) {
            err = post_connect();
            if (err==0) {
                STATS.connects++;
                STATS.connecttime+=usTick()-startt;
#ifdef _TRACE
                char peer[256];
                peer[0] = 'C';
                peer[1] = '!';
                strcpy(peer+2,hostname?hostname:"(NULL)");
                free(tracename);
                tracename = strdup(peer);
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
    THROWJSOCKEXCEPTION(JSOCKERR_connection_failed);
}



ISocket*  ISocket::connect_wait( const SocketEndpoint &ep, unsigned timems)
{
    if (ep.isNull()||(ep.port==0))
        THROWJSOCKEXCEPTION2(JSOCKERR_bad_address);
    Owned<CSocket> sock = new CSocket(ep,sm_tcp,NULL);
    sock->connect_wait(timems);
    return sock.getClear();
}



void CSocket::udpconnect()
{
    DEFINE_SOCKADDR(u);
    if (targetip.isNull()) {
        set_return_addr(hostport,hostname);
        targetip.ipset(returnep);
    }
    socklen_t  ul = setSockAddr(u,targetip,hostport);
    sock = ::socket(u.sa.sa_family, SOCK_DGRAM, targetip.isIp4()?0:PF_INET6);
#ifdef SOCKTRACE
    PROGLOG("SOCKTRACE: udp connected socket %x %d (%p)", sock, sock, this);
#endif
    STATS.activesockets++;
    if (sock == INVALID_SOCKET) {
        THROWJSOCKEXCEPTION(ERRNO());
    }
    int res = ::connect(sock, &u.sa, ul);
    if (res != 0) { // works for UDP
        closesock();
        THROWJSOCKEXCEPTION(JSOCKERR_connection_failed);
    }
    nagling = false; // means nothing for UDP
    state = ss_open;
#ifdef _TRACE
    char peer[256];
    peer[0] = 'C';
    peer[1] = '!';
    strcpy(peer+2,hostname?hostname:"(NULL)");
    free(tracename);
    tracename = strdup(peer);
#endif

}




int CSocket::wait_read(unsigned timeout)
{
    int ret = 0;
    while (sock!=INVALID_SOCKET) {
        T_FD_SET fds;
        CHECKSOCKRANGE(sock);
        XFD_ZERO(&fds);
        FD_SET((unsigned)sock, &fds);
        if (timeout==WAIT_FOREVER) {
            ret = ::select( sock + 1, (fd_set *)&fds, NULL, NULL, NULL );
        }
        else {
            struct timeval tv;
            tv.tv_sec = timeout / 1000;
            tv.tv_usec = (timeout % 1000)*1000;
            ret = ::select( sock + 1, (fd_set *)&fds, NULL, NULL, &tv );
        }
        if (ret==SOCKET_ERROR) {
            int err = ERRNO();
            if (err!=EINTRCALL) {   // else retry (should adjust time but for our usage don't think it matters that much)
                LOGERR2(err,1,"wait_read");
                break;
            }
        }
        else
            break;
    }
    return ret;
}

int CSocket::wait_write(unsigned timeout)
{
    int ret = 0;
    while (sock!=INVALID_SOCKET) {
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
        if (ret==SOCKET_ERROR) {
            int err = ERRNO();
            if (err!=EINTRCALL) {   // else retry (should adjust time but for our usage don't think it matters that much)
                LOGERR2(err,1,"wait_write");
                break;
            }
        }
        else
            break;
    }
    return ret;
}

void CSocket::readtms(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read,
                     unsigned timeoutms)
{
    if (timeoutms == WAIT_FOREVER) {
        read(buf,min_size, max_size, size_read,WAIT_FOREVER);
        return;
    }
        

    unsigned startt=usTick();
    size_read = 0;
    if (state != ss_open) {
        THROWJSOCKEXCEPTION(JSOCKERR_not_opened);
    }
    unsigned start;
    unsigned timeleft;
    start = msTick();
    timeleft = timeoutms;

    do {
        int rc = wait_read(timeleft);
        if (rc < 0) {
            THROWJSOCKEXCEPTION(ERRNO());
        }
        if (rc == 0) {
            THROWJSOCKEXCEPTION(JSOCKERR_timeout_expired);
        }
        unsigned elapsed = (msTick()-start);
        if (elapsed<timeoutms)
            timeleft = timeoutms-elapsed;
        else
            timeleft = 0;
        unsigned retrycount=100;
EintrRetry:
        if (sockmode==sm_udp_server) { // udp server        
            DEFINE_SOCKADDR(u);
            socklen_t ul=sizeof(u);
            rc = recvfrom(sock, (char*)buf + size_read, max_size - size_read, 0, &u.sa,&ul);
            getSockAddrEndpoint(u,ul,returnep);
        }
        else {
            rc = recv(sock, (char*)buf + size_read, max_size - size_read, 0);
        }
        if (rc < 0) {
            int err = ERRNO();
            if (BADSOCKERR(err)) {
                // don't think this should happen but convert to same as shutdown while investigation
                LOGERR2(err,1,"Socket closed during read");
                rc = 0;
            }
            else if ((err==EINTRCALL)&&(retrycount--!=0)) {
                LOGERR2(err,1,"EINTR retrying");
                goto EintrRetry;
            }
            else {
                VStringBuffer errMsg("readtms(timeoutms=%d)", timeoutms);
                LOGERR2(err,1,errMsg.str());
                if ((err==ECONNRESET)||(err==EINTRCALL)||(err==ECONNABORTED)) {
                    errclose();
                    err = JSOCKERR_broken_pipe;
                }
                THROWJSOCKEXCEPTION(err);
            }
        }
        if (rc == 0) {
            state = ss_shutdown;
            if (min_size==0) 
                break;                      // if min_read is 0 return 0 if socket closed
            THROWJSOCKEXCEPTION(JSOCKERR_graceful_close);
        }
        size_read += rc;
    } while (size_read < min_size);
    STATS.reads++;
    STATS.readsize += size_read;
    STATS.readtime+=usTick()-startt;

}

void CSocket::read(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read,
                     unsigned timeoutsecs)
{
    unsigned startt=usTick();
    size_read = 0;
    unsigned start = 0;
    unsigned timeleft = 0;
    if (state != ss_open) {
        THROWJSOCKEXCEPTION(JSOCKERR_not_opened);
    }
    if (timeoutsecs != WAIT_FOREVER) {
        start = (unsigned)time(NULL);
        timeleft = timeoutsecs;
    }

    do {
        int rc;
        if (timeoutsecs != WAIT_FOREVER) {
            rc = wait_read(timeleft*1000);
            if (rc < 0) {
                THROWJSOCKEXCEPTION(ERRNO());
            }
            if (rc == 0) {
                THROWJSOCKEXCEPTION(JSOCKERR_timeout_expired);
            }
            unsigned elapsed = ((unsigned)time(NULL))-start;
            if (elapsed<timeoutsecs)
                timeleft = timeoutsecs-elapsed;
            else
                timeleft = 0;
        }
        unsigned retrycount=100;
EintrRetry:
        if (sockmode==sm_udp_server) { // udp server
            DEFINE_SOCKADDR(u);
            socklen_t ul=sizeof(u.sin);     
            rc = recvfrom(sock, (char*)buf + size_read, max_size - size_read, 0, &u.sa,&ul);
            getSockAddrEndpoint(u,ul,returnep);
        }
        else {
            rc = recv(sock, (char*)buf + size_read, max_size - size_read, 0);
        }
        if (rc < 0) {
            int err = ERRNO();
            if (BADSOCKERR(err)) {
                // don't think this should happen but convert to same as shutdown while investigation
                LOGERR2(err,3,"Socket closed during read");
                rc = 0;
            }
            else if ((err==EINTRCALL)&&(retrycount--!=0)) {
                if (sock==INVALID_SOCKET)
                    rc = 0;         // convert an EINTR after closed to a graceful close
                else {
                    LOGERR2(err,3,"EINTR retrying");
                    goto EintrRetry;
                }
            }
            else {
                LOGERR2(err,3,"read");
                if ((err==ECONNRESET)||(err==EINTRCALL)||(err==ECONNABORTED)) {
                    errclose();
                    err = JSOCKERR_broken_pipe;
                }
                THROWJSOCKEXCEPTION(err);
            }
        }
        if (rc == 0) {
            state = ss_shutdown;
            if (min_size==0) 
                break;                      // if min_read is 0 return 0 if socket closed
            THROWJSOCKEXCEPTION(JSOCKERR_graceful_close);
        }
        size_read += rc;
    } while (size_read < min_size);
    STATS.reads++;
    STATS.readsize += size_read;
    STATS.readtime+=usTick()-startt;
}

void CSocket::read(void* buf, size32_t size)
{
    if (!size)
        return;
    unsigned startt=usTick();
    size32_t size_read=size;
    if (state != ss_open) {
        THROWJSOCKEXCEPTION(JSOCKERR_not_opened);
    }

    do {
        unsigned retrycount=100;
EintrRetry:
        int rc;
        if (sockmode==sm_udp_server) { // udp server
            DEFINE_SOCKADDR(u);
            socklen_t ul=sizeof(u.sin);
            rc = recvfrom(sock, (char*)buf, size, 0, &u.sa,&ul);
            getSockAddrEndpoint(u,ul,returnep);
        }
        else {
            rc = recv(sock, (char*)buf, size, 0);
        }
        if (rc < 0) {
            int err = ERRNO();
            if (BADSOCKERR(err)) {
                // don't think this should happen but convert to same as shutdown while investigation
                LOGERR2(err,5,"Socket closed during read");
                rc = 0;
            }
            else if ((err==EINTRCALL)&&(retrycount--!=0)) {
                LOGERR2(err,5,"EINTR retrying");
                goto EintrRetry;
            }
            else {
                LOGERR2(err,5,"read");
                if ((err==ECONNRESET)||(err==EINTRCALL)||(err==ECONNABORTED)) {
                    errclose();
                    err = JSOCKERR_broken_pipe;
                }
                THROWJSOCKEXCEPTION(err);
            }
        }
        if (rc == 0) {
            state = ss_shutdown;
            THROWJSOCKEXCEPTION(JSOCKERR_graceful_close);
        }
        buf = (char*)buf + rc;
        size -= rc;
    } while (size != 0);
    STATS.reads++;
    STATS.readsize += size_read;
    STATS.readtime+=usTick()-startt;

}



size32_t CSocket::write(void const* buf, size32_t size)
{
    if (size==0)
        return 0;
    unsigned startt=usTick();
    size32_t size_writ = size;
    if (state != ss_open) {
        THROWJSOCKEXCEPTION(JSOCKERR_not_opened);
    }
    size32_t res=0;
    do {
        unsigned retrycount=100;
EintrRetry:
        int rc;
        if (sockmode==sm_udp_server) { // udp server
            DEFINE_SOCKADDR(u);
            socklen_t  ul = setSockAddr(u,returnep,returnep.port);
            rc = sendto(sock, (char*)buf, size, 0, &u.sa, ul);
        }
        else {
            rc = send(sock, (char*)buf, size, SEND_FLAGS);
        }
        if (rc < 0) {
            int err=ERRNO();
            if (BADSOCKERR(err)) {
                LOGERR2(err,7,"Socket closed during write");
                rc = 0;
            }
            else if ((err==EINTRCALL)&&(retrycount--!=0)) {
                LOGERR2(err,7,"EINTR retrying");
                goto EintrRetry;
            }
            else {
                if (((sockmode==sm_multicast)||(sockmode==sm_udp))&&(err==ECONNREFUSED))
                    break; // ignore
                LOGERR2(err,7,"write");
                if ((err==ECONNRESET)||(err==EINTRCALL)||(err==ECONNABORTED)
#ifndef _WIN32
                    ||(err==EPIPE)||(err==ETIMEDOUT)  // linux can raise these on broken pipe
#endif
                    ) {
                    errclose();
                    err = JSOCKERR_broken_pipe;
                }
                if ((err == EWOULDBLOCK) && nonblocking)
                    break;
                THROWJSOCKEXCEPTION(err);
            }
        }   
        res += rc;
        if (rc == 0) {
            state = ss_shutdown;
            THROWJSOCKEXCEPTION(JSOCKERR_graceful_close);
        }
        if (nonblocking)
            break;
        buf = (char*)buf + rc;
        size -= rc;
    } while (size != 0);
    STATS.writes++;
    STATS.writesize += size_writ;
    STATS.writetime+=usTick()-startt;
    return res;
}

size32_t CSocket::writetms(void const* buf, size32_t size, unsigned timeoutms)
{
    if (size==0)
        return 0;

    if (state != ss_open)
    {
        THROWJSOCKEXCEPTION(JSOCKERR_not_opened);
    }

    if (timeoutms == WAIT_FOREVER)
        return write(buf, size);

    const char *p = (const char *)buf;
    unsigned start, elapsed;
    start = msTick();
    elapsed = 0;
    size32_t nwritten = 0;
    size32_t nleft = size;
    unsigned rollover = 0;

    bool prevblock = set_nonblock(true);

    while ( (nwritten < size) && (elapsed <= timeoutms) )
    {
        size32_t amnt = write(p,nleft);

        if ( ((amnt == (size32_t)-1) || (amnt == 0)) && (++rollover >= 20) )
        {
            rollover = 0;
            Sleep(20);
        }
        else
        {
            nwritten += amnt;
            nleft -= amnt;
            p += amnt;
        }
        elapsed = msTick() - start;
    }

    set_nonblock(prevblock);

    if (nwritten < size)
    {
        ERRLOG("writetms timed out; timeout: %u, nwritten: %u, size: %u", timeoutms, nwritten, size);
        THROWJSOCKEXCEPTION(JSOCKERR_timeout_expired);
    }

    return nwritten;
}

bool CSocket::check_connection()
{
    if (state != ss_open) 
        return false;

    unsigned retrycount=100;
EintrRetry:
    int rc;
    if (sockmode==sm_udp_server) { // udp server
        DEFINE_SOCKADDR(u);
        socklen_t  ul = setSockAddr(u,returnep,returnep.port);
        rc = sendto(sock, NULL, 0, 0, &u.sa, ul);
    }
    else {
        rc = send(sock, NULL, 0, SEND_FLAGS);
    }
    if (rc < 0) {
        int err=ERRNO();
        if ((err==EINTRCALL)&&(retrycount--!=0)) {
            LOGERR2(err,7,"EINTR retrying");
            goto EintrRetry;
        }
        else
            return false;
    }   
    return true;
}

size32_t CSocket::udp_write_to(const SocketEndpoint &ep, void const* buf, size32_t size)
{
    if (size==0)
        return 0;
    unsigned startt=usTick();
    size32_t size_writ = size;
    if (state != ss_open) {
        THROWJSOCKEXCEPTION(JSOCKERR_not_opened);
    }
    size32_t res=0;
    DEFINE_SOCKADDR(u); 
    loop {
        socklen_t  ul = setSockAddr(u,ep,ep.port);      
        int rc = sendto(sock, (char*)buf, size, 0, &u.sa, ul);
        if (rc < 0) {
            int err=ERRNO();
            if (((sockmode==sm_multicast)||(sockmode==sm_udp))&&(err==ECONNREFUSED))
                break; // ignore
            if (err!=EINTRCALL) {
                THROWJSOCKEXCEPTION(err);
            }
        }
        else {
            res = (size32_t)rc;
            break;
        }
    }
    STATS.writes++;
    STATS.writesize += res;
    STATS.writetime+=usTick()-startt;
    return res;
}

size32_t CSocket::write_multiple(unsigned num,const void **buf, size32_t *size)
{
    assertex(sockmode!=sm_udp_server);
    assertex(!nonblocking);
    if (num==1)
        return write(buf[0],size[0]);
    size32_t total = 0;
    unsigned i;
    for (i=0;i<num;i++)
        total += size[i];
    if (total==0)
        return 0;
    
    unsigned startt=usTick();
    if (state != ss_open) {
        THROWJSOCKEXCEPTION(JSOCKERR_not_opened);
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
        int err=ERRNO();
        if (BADSOCKERR(err)) {
            LOGERR2(err,8,"Socket closed during write");
            sent = 0;
        }
        else if ((err==EINTRCALL)&&(retrycount--!=0)) {
            LOGERR2(err,8,"EINTR retrying");
            goto EintrRetry;
        }
        else {
            LOGERR2(err,8,"write_multiple");
            if ((err==ECONNRESET)||(err==EINTRCALL)||(err==ECONNABORTED)||(err==ETIMEDOUT)) {
                errclose();
                err = JSOCKERR_broken_pipe;
            }
            THROWJSOCKEXCEPTION(err);
        }
    }   
    if (sent == 0) {
        state = ss_shutdown;
        THROWJSOCKEXCEPTION(JSOCKERR_graceful_close);
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
    size32_t outwr = 0;
    i = 0;
    size32_t os = 0;
    size32_t left = total;
    byte *b = NULL;
    size32_t s=0;
    loop {
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
    STATS.writes++;
    STATS.writesize += res;
    STATS.writetime+=usTick()-startt;
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
    static unsigned lastreporttime=0;
    static unsigned lastexceeded=0;
    if (elapsed>1000000*60) { // over 1min
        unsigned t = msTick();
        if (1) { //((t-lastreporttime>1000*60) || // only report once per min
                 // (elapsed>lastexceeded*2)) {
            lastexceeded = elapsed;
            lastreporttime = t;
            WARNLOG("send_block took %ds to %s  (%d,%d,%d)",elapsed/1000000,tracename,startt2-startt,startt3-startt2,nowt-startt3);
        }
    }
#endif
    return true;
}

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
                WARNLOG("Receive block stalled");
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
            void *tmp=malloc(sz);
            readtms(tmp,sz,sz,rd,blocktimeoutms);
            free(tmp);
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
            int err=ERRNO();
            if (err==ENOTCONN) {
                LOGERR2(err,9,"shutdown");
                err = JSOCKERR_broken_pipe;
            }
            THROWJSOCKEXCEPTION(err);
        }
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
            THROWJSOCKEXCEPTION(ERRNO());
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
        LOGERR2(ERRNO(),1,"setsockopt(SO_SNDBUF)");
    }
#ifdef CHECKBUFSIZE
    size32_t v;
    if (getsockopt(sock, SOL_SOCKET, SO_SNDBUF, (char *)&v, sizeof(v))!=0) {
        LOGERR2(ERRNO(),1,"getsockopt(SO_SNDBUF)");
    }
    if (v!=maxsend) 
        WARNLOG("set_send_buffer_size requested %d, got %d",maxsend,v);
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
        LOGERR2(ERRNO(),1,"setsockopt(SO_RCVBUF)");
    }
#ifdef CHECKBUFSIZE
    size32_t v;
    if (getsockopt(sock, SOL_SOCKET, SO_RCVBUF, (char *)&v, sizeof(v))!=0) {
        LOGERR2(ERRNO(),1,"getsockopt(SO_RCVBUF)");
    }
    if (v<max) 
        WARNLOG("set_receive_buffer_size requested %d, got %d",max,v);
#endif
}


bool CSocket::join_multicast_group(SocketEndpoint &ep) 
{
    StringBuffer s;
    ep.getIpText(s);    // will improve later
    MCASTREQ req(s.str());
    if (setsockopt(sock, IPPROTO_IP, IP_ADD_MEMBERSHIP,(char*)&req, sizeof(req))!=0) {
        return false;
    }
    return true;
}


bool CSocket::leave_multicast_group(SocketEndpoint &ep) 
{
    StringBuffer s;
    ep.getIpText(s);    // will improve later
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

CSocket::~CSocket()
{
  if (owned) 
    close();
  free(hostname);
  hostname = NULL;
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
    hostname = NULL;
    mcastreq = NULL;
    tracename = NULL;
    StringBuffer tmp;
    if ((smode==sm_multicast_server)&&(name&&*name)) {
        mcastreq = new MCASTREQ(name);
    }
    else {
        if (!name&&!ep.isNull())
            name = ep.getIpText(tmp).str();
        hostname = name?strdup(name):NULL;
    }
    sock = INVALID_SOCKET;
    sockmode = smode;
    owned = true;
    nextblocksize = 0;
    in_accept = false;
    accept_cancel_state = accept_not_cancelled;
#ifdef _TRACE
    char peer[256];
    peer[0] = name?'T':'S';
    peer[1] = '>';
    if (name)
        strcpy(peer+2,name);
    else {
        SocketEndpoint self;
        self.setLocalHost(0);
        self.getUrlStr(peer+2,sizeof(peer)-2);
    }
    tracename = strdup(peer);
#endif
}

CSocket::CSocket(T_SOCKET new_sock,SOCKETMODE smode,bool _owned)
{
    nonblocking = false;
#ifdef USERECVSEM
    receiveblocksemowned = false;
#endif
    nagling = true; // until turned off
    sock = new_sock;
    if (new_sock!=INVALID_SOCKET)
        STATS.activesockets++;
    hostname = NULL;
    mcastreq = NULL;
    hostport = 0;
    tracename = NULL;
    state = ss_open;
    sockmode = smode;
    owned = _owned;
    nextblocksize = 0;
    in_accept = false;
    accept_cancel_state = accept_not_cancelled;
    set_nagle(false);
    //set_linger(DEFAULT_LINGER_TIME); -- experiment with removing this as closesocket should still endevour to send outstanding data
#ifdef _TRACE
    char peer[256];
    peer[0] = 'A';
    peer[1] = '!';
    peer_name(peer+2,sizeof(peer)-2);
    tracename = strdup(peer);
#endif
}

ISocket* ISocket::create(unsigned short p,int listen_queue_size)
{
    if (p==0)
        THROWJSOCKEXCEPTION2(JSOCKERR_bad_address);
    SocketEndpoint ep;
    ep.port = p;
    Owned<CSocket> sock = new CSocket(ep,sm_tcp_server,NULL);
    sock->open(listen_queue_size);
    return sock.getClear();
}


ISocket* ISocket::create_ip(unsigned short p,const char *host,int listen_queue_size)
{
    if (p==0)
        THROWJSOCKEXCEPTION2(JSOCKERR_bad_address);
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
        THROWJSOCKEXCEPTION2(JSOCKERR_bad_address);
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
        THROWJSOCKEXCEPTION2(JSOCKERR_bad_address);
    SocketEndpoint ep(p, ip);
    StringBuffer tmp;
    Owned<CSocket> sock = new CSocket(ep,sm_multicast_server,ip.getIpText(tmp).str());
    sock->open(0,true);
    if (_ttl)
        sock->set_ttl(_ttl);
    return sock.getClear();
}

ISocket* ISocket::udp_connect(unsigned short p, char const* name)
{
    if (!name||!*name||(p==0))
        THROWJSOCKEXCEPTION2(JSOCKERR_bad_address);
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
        THROWJSOCKEXCEPTION2(JSOCKERR_bad_address);
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
    CSocket* sock = new CSocket((SOCKET)s, tcpip?sm_tcp:sm_udp, false);
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
    struct ifreq *ifr = ifc.ifc_req;
    unsigned n = ifc.ifc_len/sizeof(struct ifreq);
    bool match = false;
    for(unsigned i=0; i<n; i++)
    {
        struct ifreq *item = &ifr[i];
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

bool getInterfaceIp(IpAddress &ip,const char *ifname)
{
#if defined(_WIN32) || defined(__APPLE__)
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
    struct ifreq *ifr = ifc.ifc_req;
    unsigned n = ifc.ifc_len/sizeof(struct ifreq);
    for (int loopback = 0; loopback <= 1; loopback++)
    {
        for (int i=0; i<n; i++)
        {
            bool useLoopback = (loopback==1);
            struct ifreq *item = &ifr[i];
            if (ifname&&*ifname)
                if (!WildMatch(item->ifr_name,ifname))
                    continue;
            IpAddress iptest((inet_ntoa(((struct sockaddr_in *)&item->ifr_addr)->sin_addr)));
            if (ioctl(fd, SIOCGIFFLAGS, item) < 0)
            {
                if (!recursioncheck) {
                    recursioncheck = true;
                    DBGLOG("Error retrieving interface flags for interface %s", item->ifr_name);
                    recursioncheck = false;
                }
                continue;
            }
            bool isLoopback = iptest.isLoopBack() || ((item->ifr_flags & IFF_LOOPBACK) != 0);
            bool isUp = (item->ifr_flags & IFF_UP) != 0;
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
static StringBuffer EnvConfPath;

const char * GetCachedHostName()
{
    CriticalBlock c(hostnamesect);
    if (!cachehostname.get())
    {
#ifndef _WIN32
        IpAddress ip;
        if (EnvConfPath.length() == 0)
            EnvConfPath.append(CONFIG_DIR).append(PATHSEPSTR).append("environment.conf");
        Owned<IProperties> conf = createProperties(EnvConfPath.str(), true);

        StringBuffer ifs;
        conf->getProp("interface", ifs);
        if (getInterfaceIp(ip, ifs.str()))
        {
            StringBuffer ips;
            ip.getIpText(ips);
            if (ips.length())
            {
                cachehostname.set(ips.str());
                cachehostip.ipset(ip);
                return cachehostname.get();
            }
        }
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
        int err = ERRNO();
        StringBuffer tmp("_inet_pton: ");
        tmp.append(text);
        LOGERR(err,1,tmp.str());
    }   
    return false;
}

static bool lookupHostAddress(const char *name,unsigned *netaddr)
{
    // if IP4only or using MS V6 can only resolve IPv4 using 
    static bool recursioncheck = false; // needed to stop error message recursing
    unsigned retry=10;
#if defined(__linux__) || defined(getaddrinfo)
    if (IP4only) {
#else
    {
#endif
        CriticalBlock c(hostnamesect);
        hostent * entry = gethostbyname(name);
        while (entry==NULL) {
            if (retry--==0) {
                if (!recursioncheck) {
                    recursioncheck = true;
                    LogErr(h_errno,1,"gethostbyname failed",__LINE__,name);
                    recursioncheck = false;
                }
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
                loop {
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
#if defined(__linux__) || defined(getaddrinfo)
    struct addrinfo hints;
    memset(&hints,0,sizeof(hints));
    struct addrinfo  *addrInfo = NULL;
    loop {
        memset(&hints,0,sizeof(hints));
        int ret = getaddrinfo(name, NULL , &hints, &addrInfo);
        if (!ret) 
            break;
        if (retry--==0) {
            if (!recursioncheck) {
                recursioncheck = true;
                LogErr(ret,1,"getaddrinfo failed",__LINE__,name);
#ifdef _DEBUG
                PrintStackReport();
#endif
                recursioncheck = false;
            }
            return false;
        }
        Sleep((10-retry)*100);
    }
    struct addrinfo  *best = NULL;
    bool snm = !PreferredSubnet.isNull();
    loop {
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
    return best!=NULL;
#endif
    return false;
            
}



bool IpAddress::ipset(const char *text)
{
    if (text&&*text) {
        if ((text[0]=='.')&&(text[1]==0)) {
            ipset(queryHostIP());
            return true;
        }
        if (decodeNumericIP(text,netaddr))
            return true;
        const char *s;
        for (s=text;*s;s++)
            if (!isdigit(*s)&&(*s!=':')&&(*s!='.')) 
                break;
        if (!*s)
            return ipset(NULL);
        if (lookupHostAddress(text,netaddr))
            return true;
    }
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
    int i1;
    _cpyrev4(&i1,&netaddr[3-offset]);
    int i2;
    _cpyrev4(&i2,&other.netaddr[3-offset]);
    i1-=i2;
    if (i1>0)
        return i1;
    return -i1;
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


size32_t IpAddress::getNetAddress(size32_t maxsz,void *dst) const
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

void IpAddress::setNetAddress(size32_t sz,const void *src)
{
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


bool SocketEndpoint::set(const char *name,unsigned short _port)
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
        if (ipset(name)) {
            port = _port; 
            return true;
        }
    }
    ipset(NULL);
    port = 0;   
    return false;
}

void SocketEndpoint::getUrlStr(char * str, size32_t len) const
{
    if (len==0)
        return;
    StringBuffer _str;
    getUrlStr(_str);
    size32_t l = _str.length()+1;
    if (l>len)
    { 
        l = len-1;
        str[l] = 0;
    }
    memcpy(str,_str.str(),l);
}

StringBuffer &SocketEndpoint::getUrlStr(StringBuffer &str) const
{
    getIpText(str);
    if (port) 
        str.append(':').append((unsigned)port);         // TBD IPv6 put [] on
    return str;
}


unsigned SocketEndpoint::hash(unsigned prev) const
{
    return hashc((const byte *)&port,sizeof(port),iphash(prev));
}




//---------------------------------------------------------------------------

SocketListCreator::SocketListCreator()
{
    lastPort = 0;
}

void SocketListCreator::addSocket(const SocketEndpoint &ep)
{
    StringBuffer ipstr;
    ep.getIpText(ipstr);
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
            loop
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
        sockep.getIpText(ipstr);
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
        loop
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
        loop
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
    loop
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
    appendtime(str.append("readtime="),stats.readtime).append('\n');
    str.append("readsize=").append(stats.readsize).append(" bytes\n");
    str.append("writes=").append(stats.writes).append('\n');
    appendtime(str.append("writetime="),stats.writetime).append('\n');
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
    bool add_epoll;
    bool operator == (const SelectItem & other) const { return sock == other.sock; }
};

class SelectItemArray : public StructArrayOf<SelectItem> { };

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
    atomic_t tickwait;
    SelectItemArray items;
    unsigned offset;
    bool selectvarschange;
    unsigned waitingchange;
    Semaphore waitingchangesem;
    int validateselecterror;
    unsigned validateerrcount;
    const char *selecttrace;
    unsigned basesize;
#ifdef _USE_PIPE_FOR_SELECT_TRIGGER
    T_SOCKET dummysock[2];
#else
    T_SOCKET dummysock; 
#endif
    bool dummysockopen;

    CSocketBaseThread(const char *trc) : Thread("CSocketBaseThread")
    {
    }

    ~CSocketBaseThread()
    {
    }

public:
    void triggerselect()
    {
        if (atomic_read(&tickwait))
            ticksem.signal();
#ifdef _USE_PIPE_FOR_SELECT_TRIGGER
        CriticalBlock block(sect);
        char c = 0;
        if(write(dummysock[1], &c, 1) != 1) {
            int err = ERRNO();
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

    void stop(bool wait)
    {
        terminating = true;
        triggerselect();
        if (wait)
            join();
    }


    bool sockOk(T_SOCKET sock)
    {
        PROGLOG("CSocketBaseThread: sockOk testing %d",sock);
        int err = 0;
        int t=0;
        socklen_t tl = sizeof(t);
        if (getsockopt(sock, SOL_SOCKET, SO_TYPE, (char *)&t, &tl)!=0) {
            StringBuffer sockstr;
            const char *tracename = sockstr.append((unsigned)sock).str();
            LOGERR2(ERRNO(),1,"CSocketBaseThread select handle");
            return false;
        }
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
            LOGERR2(ERRNO(),2,"CSocketBaseThread select handle");
            return false;
        }
        else if (rc>0)
            PROGLOG("CSocketBaseThread: select handle %d selected(2) %d",sock,rc);
        XFD_ZERO(&fds);
        FD_SET((unsigned)sock, &fds);
        tv.tv_sec = 0;
        tv.tv_usec = 0;
        rc = ::select( sock + 1, (fd_set *)&fds, NULL, NULL, &tv );
        if (rc<0) {
            StringBuffer sockstr;
            const char *tracename = sockstr.append((unsigned)sock).str();
            LOGERR2(ERRNO(),3,"CSocketBaseThread select handle");
            return false;
        }
        else if (rc>0)
            PROGLOG("CSocketBaseThread: select handle %d selected(2) %d",sock,rc);
        return true;
    }

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

    virtual void closedummy() = 0;
};

class CSocketSelectThread: public CSocketBaseThread
{
    void opendummy()
    {
        CriticalBlock block(sect);
        if (!dummysockopen) { 
#ifdef _USE_PIPE_FOR_SELECT_TRIGGER
            if(pipe(dummysock)) {
                WARNLOG("CSocketSelectThread: create pipe failed %d",ERRNO());
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
            loop {
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
        loop {
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
        loop {
            T_SOCKET sock = popfds(s);
            if (!sock)
                break;
            if (sock!=dummysock) {
                SelectItem si = findhash(sock); // nb copies
                if (!si.del) {
                    si.mode = mode;
                    tonotify.append(si);
                }
            }
        }   
    }

#endif 
public:
    IMPLEMENT_IINTERFACE;
    CSocketSelectThread(const char *trc)
        : CSocketBaseThread("CSocketSelectThread")
    {
        dummysockopen = false;
        opendummy();
        terminating = false;
        atomic_set(&tickwait,0);
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
            try {
                SelectItem &si = items.element(i);
                si.sock->Release();
                si.nfy->Release();
            }
            catch (IException *e) {
                EXCLOG(e,"~CSocketSelectThread");
                e->Release();
            }
        }
    }

    Owned<IException> termexcept;

    void updateItems()
    {
        // must be in CriticalBlock block(sect); 
        unsigned n = items.ordinality();
        bool hashupdateneeded = (n!=basesize); // additions all come at end
        for (unsigned i=0;i<n;) {
            SelectItem &si = items.element(i);
            if (si.del) {
                si.nfy->Release();
                try {
#ifdef SOCKTRACE
                    PROGLOG("CSocketSelectThread::updateItems release %d",si.handle);
#endif
                    si.sock->Release();
                }
                catch (IException *e) {
                    EXCLOG(e,"CSocketSelectThread::updateItems");
                    e->Release();
                }
                n--;
                if (i<n) 
                    si = items.item(n);
                items.remove(n);
                hashupdateneeded = true;
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


    bool add(ISocket *sock,unsigned mode,ISocketSelectNotify *nfy)
    {
        // maybe check once to prevent 1st delay? TBD
        CriticalBlock block(sect);
        unsigned n=0;
        ForEachItemIn(i,items) {
            SelectItem &si = items.element(i);
            if (!si.del) {
                if (si.sock==sock) {
                    si.del = true;
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
        sn.add_epoll = false;
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
        unsigned j=offset;
        ForEachItemIn(i,items) {
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
        try {
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


            while (!terminating) {
                selecttimeout.tv_sec = SELECT_TIMEOUT_SECS;         // linux modifies so initialize inside loop
                selecttimeout.tv_usec = 0;
                if (selectvarschange) {
                    updateSelectVars(rdfds,wrfds,exfds,isrd,iswr,isex,ni,maxsockid);
                }
                if (ni==0) {
                    validateerrcount = 0;
                    atomic_inc(&tickwait);                  
                    if(!selectvarschange&&!terminating) 
                    ticksem.wait(SELECT_TIMEOUT_SECS*1000);
                    atomic_dec(&tickwait);
                        
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
                if (n < 0) {
                    CriticalBlock block(sect);
                    int err = ERRNO();
                    if (err != EINTRCALL) {
                        if (dummysockopen) {
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
                else if (n>0) { 
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
                        if (r&&dummysockopen&&findfds(rs,dummysock[0],r)) {
                            resettrigger();
                            --n;
                        }           
#endif
                        for (i=0;(n>0)&&(i<ni);i++) {
                            if (r&&findfds(rs,si->handle,r)) {
                                if (!si->del) {
                                    tonotify.append(*si);
                                    tonotify.element(tonotify.length()-1).mode = SELECTMODE_READ;
                                }
                                --n;
                            }
                            if (w&&findfds(ws,si->handle,w)) {
                                if (!si->del) {
                                    tonotify.append(*si);
                                    tonotify.element(tonotify.length()-1).mode = SELECTMODE_WRITE;
                                }
                                --n;
                            }
                            if (e&&findfds(es,si->handle,e)) {
                                if (!si->del) {
                                    tonotify.append(*si);
                                    tonotify.element(tonotify.length()-1).mode = SELECTMODE_EXCEPT;
                                }
                                --n;
                            }
                            si++;
                            if (si==sie)
                                si = items.getArray();
                        }
#endif
                    }
                    ForEachItemIn(j,tonotify) {
                        const SelectItem &si = tonotify.item(j);
                        try {
                            si.nfy->notifySelected(si.sock,si.mode); // ignore return
                        }
                        catch (IException *e) { // should be acted upon by notifySelected
                            EXCLOG(e,"CSocketSelectThread notifySelected");
                            throw ;
                        }
                    }
                }
                else  {
                    validateerrcount = 0;
                    if ((++numto>=lastnumto*2)) {
                        lastnumto = numto;
                        if (selecttrace&&(numto>4))
                            PROGLOG("%s: Select Idle(%d), %d,%d,%0.2f",selecttrace,numto,totnum,total,totnum?((double)total/(double)totnum):0.0);
                    }   
/*
                    if (numto&&(numto%100)) {
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
        catch (IException *e) {
            EXCLOG(e,"CSocketSelectThread");
            termexcept.setown(e);
        }
        CriticalBlock block(sect);
        try {
            updateItems();
        }
        catch (IException *e) {
            EXCLOG(e,"CSocketSelectThread(2)");
            if (!termexcept)
                termexcept.setown(e);
            else
                e->Release();
        }
        return 0;
    }
};


class CSocketSelectHandler: public CInterface, implements ISocketSelectHandler
{
    CIArrayOf<CSocketSelectThread> threads;
    CriticalSection sect;
    bool started;
    StringAttr selecttrace;
public:
    IMPLEMENT_IINTERFACE;
    CSocketSelectHandler(const char *trc)
        : selecttrace(trc)
    {
        started = false;
    }
    void start()
    {
        CriticalBlock block(sect);
        if (!started) {
            started = true;
            ForEachItemIn(i,threads) {
                threads.item(i).start();
            }
        }
            
    }
    void add(ISocket *sock,unsigned mode,ISocketSelectNotify *nfy)
    {
        CriticalBlock block(sect);
        loop {
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
                thread->start();
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
class CSocketEpollThread: public CSocketBaseThread
{
    int epfd;
    int *epfdtbl; // table of fd<->item index for lookups
    struct epoll_event *epevents;

    void epoll_op(int efd, int op, int fd, unsigned int event_mask)
    {
        int srtn;
        struct epoll_event event;

        // write all bytes to eliminate uninitialized warnings
        memset(&event, 0, sizeof(event));

        event.events = event_mask;
        event.data.fd = fd;

# ifdef EPOLLTRACE
        DBGLOG("EPOLL: op(%d) fd %d to epfd %d", op, fd, efd);
# endif
        srtn = ::epoll_ctl(efd, op, fd, &event);
        // if another thread closed fd before here don't fail
        if ( (srtn < 0) && (op != EPOLL_CTL_DEL) ){
            int err = ERRNO();
            LOGERR(err,1,"epoll_ctl");
            THROWJSOCKEXCEPTION2(err);
        }
    }

    void opendummy()
    {
        CriticalBlock block(sect);
        if (!dummysockopen) {
#ifdef _USE_PIPE_FOR_SELECT_TRIGGER
            if(pipe(dummysock)) {
                WARNLOG("CSocketEpollThread: create pipe failed %d",ERRNO());
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
            epoll_op(epfd, EPOLL_CTL_ADD, dummysock[0], EPOLLIN);
#else
            if (IP6preferred)
                dummysock = ::socket(AF_INET6, SOCK_STREAM, PF_INET6);
            else
                dummysock = ::socket(AF_INET, SOCK_STREAM, 0);
            CHECKSOCKRANGE(dummysock);
            // added EPOLLIN also because cannot find anywhere MSG_OOB is sent
            // added here to match existing select() code above which sets
            // the except fd_set mask.
            epoll_op(epfd, EPOLL_CTL_ADD, dummysock, (EPOLLIN | EPOLLPRI));
#endif
            dummysockopen = true;
        }


    }

    void closedummy()
    {
        CriticalBlock block(sect);
        if (dummysockopen) {
#ifdef _USE_PIPE_FOR_SELECT_TRIGGER
            epoll_op(epfd, EPOLL_CTL_DEL, dummysock[0], 0);
#ifdef SOCKTRACE
            PROGLOG("SOCKTRACE: Closing dummy sockets %x %d %x %d (%p)", dummysock[0], dummysock[0], dummysock[1], dummysock[1], this);
#endif
            ::close(dummysock[0]);
            ::close(dummysock[1]);
#else
            epoll_op(epfd, EPOLL_CTL_DEL, dummysock, 0);
            ::close(dummysock);
#endif
            dummysockopen = false;
        }
    }


public:
    IMPLEMENT_IINTERFACE;
    CSocketEpollThread(const char *trc)
        : CSocketBaseThread("CSocketEpollThread")
    {
        dummysockopen = false;
        terminating = false;
        atomic_set(&tickwait,0);
        waitingchange = 0;
        selectvarschange = false;
        validateselecterror = 0;
        validateerrcount = 0;
        offset = 0;
        selecttrace = trc;
        epfd = ::epoll_create(XFD_SETSIZE);
        if (epfd < 0) {
          int err = ERRNO();
          LOGERR(err,1,"epoll_create()");
          THROWJSOCKEXCEPTION2(err);
        }
# ifdef EPOLLTRACE
        DBGLOG("CSocketEpollThread: creating epoll fd %d", epfd );
# endif
        try {
            epfdtbl = new int[XFD_SETSIZE];
        } catch (const std::bad_alloc &e) {
            int err = ERRNO();
            LOGERR(err,1,"epfdtbl alloc()");
            THROWJSOCKEXCEPTION2(err);
        }
        for (int i=0; i<XFD_SETSIZE; i++) {
            epfdtbl[i] = -1;
        }
        try {
            epevents = new struct epoll_event[XFD_SETSIZE];
        } catch (const std::bad_alloc &e) {
            int err = ERRNO();
            LOGERR(err,1,"epevents alloc()");
            THROWJSOCKEXCEPTION2(err);
        }
        opendummy();
    }

    ~CSocketEpollThread()
    {
        closedummy();
        ForEachItemIn(i,items) {
            try {
                SelectItem &si = items.element(i);
                epoll_op(epfd, EPOLL_CTL_DEL, si.handle, 0);
                si.sock->Release();
                si.nfy->Release();
            }
            catch (IException *e) {
                EXCLOG(e,"~CSocketEpollThread");
                e->Release();
            }
        }
        if (epfd >= 0) {
# ifdef EPOLLTRACE
            DBGLOG("EPOLL: closing epfd %d", epfd );
# endif
            ::close(epfd);
            epfd = -1;
            delete [] epfdtbl;
            delete [] epevents;
        }
    }

    Owned<IException> termexcept;

    void updateItems()
    {
        // must be in CriticalBlock block(sect);
        unsigned n = items.ordinality();
        bool reindex = false;
        for (unsigned i=0;i<n;) {
            SelectItem &si = items.element(i);
            if (si.add_epoll) {
                reindex = true;
            }
            if (si.del) {
                epoll_op(epfd, EPOLL_CTL_DEL, si.handle, 0);
                epfdtbl[si.handle] = -1;
                reindex = true;
                si.nfy->Release();
                try {
#ifdef SOCKTRACE
                    PROGLOG("CSocketEpollThread::updateItems release %d",si.handle);
#endif
                    si.sock->Release();
                }
                catch (IException *e) {
                    EXCLOG(e,"CSocketEpollThread::updateItems");
                    e->Release();
                }
                n--;
                if (i<n)
                    si = items.item(n);
                items.remove(n);
            }
            else
                i++;
        }
        assertex(n<=XFD_SETSIZE-1);
        if (reindex) {
# ifdef EPOLLTRACE
            int max_sockid = 0;
# endif
            ForEachItemIn(j,items) {
                SelectItem &si = items.element(j);
                epfdtbl[si.handle] = j;
                if (si.add_epoll) {
                    si.add_epoll = false;
                    int srtn, ep_mode;
                    struct epoll_event event;
                    if (si.mode != 0) {
                        ep_mode = 0;
                        if (si.mode & SELECTMODE_READ) {
                            ep_mode |= EPOLLIN;
                        }
                        if (si.mode & SELECTMODE_WRITE) {
                            ep_mode |= EPOLLOUT;
                        }
                        if (si.mode & SELECTMODE_EXCEPT) {
                            ep_mode |= EPOLLPRI;
                        }
                        if (ep_mode != 0) {
                            epoll_op(epfd, EPOLL_CTL_ADD, si.handle, ep_mode);
                        }
                    }
# ifdef EPOLLTRACE
                    max_sockid=std::max(si.handle, max_sockid);
# endif
                }
# ifdef EPOLLTRACE
                for(int ix=0; ix<=max_sockid; ix++) {
                    DBGLOG("EPOLL: epfdtbl[%d] = %d", ix, epfdtbl[ix]);
                }
# endif
            }
# ifdef EPOLLTRACE
            DBGLOG("EPOLL: leaving updateItems(), reindex = %d", reindex);
# endif
        }
    }

    bool add(ISocket *sock,unsigned mode,ISocketSelectNotify *nfy)
    {
        // maybe check once to prevent 1st delay? TBD
        CriticalBlock block(sect);
        unsigned n=0;
        ForEachItemIn(i,items) {
            SelectItem &si = items.element(i);
            if (!si.del) {
                if (si.sock==sock) {
                    si.del = true;
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
        sn.add_epoll = true;
        items.append(sn);
        selectvarschange = true;
        triggerselect();
        return true;
    }

    void updateEpollVars(unsigned &ni)
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
                PROGLOG("CSocketEpollThread::updateEpollVars cannot find socket error");
                if (validateerrcount>10)
                    throw MakeStringException(-1,"CSocketEpollThread:Socket epoll error %d",validateselecterror);
            }
        }
        else
            validateerrcount = 0;
        updateItems();
#ifndef _USE_PIPE_FOR_SELECT_TRIGGER
        opendummy();
#endif
        ni = items.ordinality();
        validateselecterror = 0;
    }

    int run()
    {
        try {
            unsigned ni = 0;
            unsigned numto = 0;
            unsigned lastnumto = 0;
            unsigned totnum = 0;
            unsigned total = 0;
            selectvarschange = true;

            while (!terminating) {
                if (selectvarschange) {
                        updateEpollVars(ni);
                }
                if (ni==0) {
                    validateerrcount = 0;
                    atomic_inc(&tickwait);
                    if(!selectvarschange&&!terminating)
                        ticksem.wait(SELECT_TIMEOUT_SECS*1000);
                    atomic_dec(&tickwait);

                    continue;
                }

                int n = ::epoll_wait(epfd, epevents, XFD_SETSIZE, 1000);

# ifdef EPOLLTRACE
                if(n > 0)
                    DBGLOG("EPOLL: after epoll_wait(), n = %d, ni = %d", n, ni);
# endif

                if (terminating)
                    break;
                if (n < 0) {
                    CriticalBlock block(sect);
                    int err = ERRNO();
                    if (err != EINTRCALL) {
                        if (dummysockopen) {
                            LOGERR(err,12,"CSocketEpollThread epoll error"); // should cache error ?
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
                else if (n>0) {
                    validateerrcount = 0;
                    numto = 0;
                    lastnumto = 0;
                    total += n;
                    totnum++;
                    SelectItemArray tonotify;
                    {
                        CriticalBlock block(sect);

                        for (int j=0;j<n;j++) {
# ifdef EPOLLTRACE
                            DBGLOG("EPOLL: epevents[%d].data.fd = %d, epfdtbl = %d, emask = %d", j, epevents[j].data.fd, epfdtbl[epevents[j].data.fd], epevents[j].events);
# endif
# ifdef _USE_PIPE_FOR_SELECT_TRIGGER
                            if ((dummysockopen) && (epevents[j].data.fd == dummysock[0])) {
                                resettrigger();
                                continue;
                            }
# endif
                            if (epevents[j].data.fd >= 0) {
                                // assertex(epfdtbl[epevents[j].data.fd] >= 0);
                                if (epfdtbl[epevents[j].data.fd] < 0)
                                {
                                    WARNLOG("epoll event for invalid fd: index = %d, fd = %d, eventmask = %u", j, epevents[j].data.fd, epevents[j].events);
                                    continue;
                                }
                                SelectItem *epsi = items.getArray(epfdtbl[epevents[j].data.fd]);
                                if (!epsi->del) {
                                    unsigned int ep_mode = 0;
                                    if (epevents[j].events & (EPOLLIN | EPOLLHUP | EPOLLERR)) {
                                        ep_mode |= SELECTMODE_READ;
                                    }
                                    if (epevents[j].events & EPOLLOUT) {
                                        ep_mode |= SELECTMODE_WRITE;
                                    }
                                    if (epevents[j].events & EPOLLPRI) {
                                        ep_mode |= SELECTMODE_EXCEPT;
                                    }
                                    if (ep_mode != 0) {
                                        tonotify.append(*epsi);
                                        tonotify.element(tonotify.length()-1).mode = ep_mode;
                                    }
                                }
                            }
                        }
                    }
                    ForEachItemIn(j,tonotify) {
                        const SelectItem &si = tonotify.item(j);
                        try {
                            si.nfy->notifySelected(si.sock,si.mode); // ignore return
                        }
                        catch (IException *e) { // should be acted upon by notifySelected
                            EXCLOG(e,"CSocketEpollThread notifySelected");
                            throw ;
                        }
                    }
                }
                else  {
                    validateerrcount = 0;
                    if ((++numto>=lastnumto*2)) {
                        lastnumto = numto;
                        if (selecttrace&&(numto>4))
                            PROGLOG("%s: Epoll Idle(%d), %d,%d,%0.2f",selecttrace,numto,totnum,total,totnum?((double)total/(double)totnum):0.0);
                    }
/*
                    if (numto&&(numto%100)) {
                        CriticalBlock block(sect);
                        if (!selectvarschange)
                            selectvarschange = checkSocks();
                    }
*/
                }
            }
        }
        catch (IException *e) {
            EXCLOG(e,"CSocketEpollThread");
            termexcept.setown(e);
        }
        CriticalBlock block(sect);
        try {
            updateItems();
        }
        catch (IException *e) {
            EXCLOG(e,"CSocketEpollThread(2)");
            if (!termexcept)
                termexcept.setown(e);
            else
                e->Release();
        }
        return 0;
    }

};

class CSocketEpollHandler: public CInterface, implements ISocketSelectHandler
{
    CSocketEpollThread *epollthread;
    CriticalSection sect;
    bool started;
    StringAttr epolltrace;
public:
    IMPLEMENT_IINTERFACE;
    CSocketEpollHandler(const char *trc)
        : epolltrace(trc)
    {
        epollthread = new CSocketEpollThread(epolltrace);
    }

    ~CSocketEpollHandler()
    {
        delete epollthread;
    }

    void start()
    {
        CriticalBlock block(sect);
        epollthread->start();
    }

    void add(ISocket *sock,unsigned mode,ISocketSelectNotify *nfy)
    {
        CriticalBlock block(sect); // JCS->MK - are these blocks necessary? epollthread->add() uses it's own CS.

        /* JCS->MK, the CSocketSelectHandler variety, checks result of thread->add and spins up another handler
         * Shouldn't epoll version do the same?
         */
        if (!epollthread->add(sock,mode,nfy))
            throw MakeStringException(-1, "CSocketEpollHandler: failed to add socket to epollthread handler: sock # = %d", sock->OShandle());
    }

    void remove(ISocket *sock)
    {
        CriticalBlock block(sect); // JCS->MK - are these blocks necessary? epollthread->add() uses it's own CS.
        epollthread->remove(sock);
    }

    void stop(bool wait)
    {
        IException *e=NULL;
        epollthread->stop(wait);           // not quite as quick as could be if wait true
        if (wait && !e && epollthread->termexcept)
            e = epollthread->termexcept.getClear();
#if 0 // don't throw error as too late
        if (e)
            throw e;
#else
        ::Release(e);
#endif
    }
};
#endif // _HAS_EPOLL_SUPPORT

enum EpollMethod { EPOLL_INIT = 0, EPOLL_DISABLED, EPOLL_ENABLED };
static EpollMethod epoll_method = EPOLL_INIT;
static CriticalSection epollsect;

ISocketSelectHandler *createSocketSelectHandler(const char *trc)
{
#ifdef _HAS_EPOLL_SUPPORT
    {
        CriticalBlock block(epollsect);
        // DBGLOG("createSocketSelectHandler(): epoll_method = %d",epoll_method);
        if (epoll_method == EPOLL_INIT) {
            Owned<IProperties> conf = createProperties(CONFIG_DIR PATHSEPSTR "environment.conf", true);
            if (conf->getPropBool("use_epoll", true)) {
                epoll_method = EPOLL_ENABLED;
            } else {
                epoll_method = EPOLL_DISABLED;
            }
            // DBGLOG("createSocketSelectHandler(): after reading conf file, epoll_method = %d",epoll_method);
        }
    }
    if (epoll_method == EPOLL_ENABLED)
        return new CSocketEpollHandler(trc);
    else
        return new CSocketSelectHandler(trc);
#else
    return new CSocketSelectHandler(trc);
#endif
}

ISocketSelectHandler *createSocketEpollHandler(const char *trc)
{
#ifdef _HAS_EPOLL_SUPPORT
    return new CSocketEpollHandler(trc);
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

// utility interface for simple conversations 
// conversation is always between two ends, 
// at any given time one end must be receiving and other sending (though these may swap during the conversation)
class CSingletonSocketConnection: public CInterface, implements IConversation
{
    Owned<ISocket> sock;
    Owned<ISocket> listensock;
    enum { Snone, Saccept, Sconnect, Srecv, Ssend, Scancelled } state;
    bool accepting;
    bool cancelling;
    SocketEndpoint ep;
    CriticalSection crit;
public:
    IMPLEMENT_IINTERFACE;

    CSingletonSocketConnection(SocketEndpoint &_ep)
    {
        ep = _ep;
        state = Snone;
        cancelling = false;
    }

    ~CSingletonSocketConnection()
    {
        try {
            if (sock)
                sock->close();
        }
        catch (IException *e) {
            if (e->errorCode()!=JSOCKERR_graceful_close)
                EXCLOG(e,"CSingletonSocketConnection close");
            e->Release();
        }
    }

    bool connect(unsigned timeoutms)
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

    bool send(MemoryBuffer &mb)
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

    unsigned short setRandomPort(unsigned short base, unsigned num)
    {
        loop {
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


    bool accept(unsigned timeoutms)
    {
        CriticalBlock block(crit);
        if (cancelling) 
            state = Scancelled;
        if (state==Scancelled)
            return false;
        if (!sock) {
            ISocket *newsock=NULL;
            state = Saccept;
            loop {
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

    bool recv(MemoryBuffer &mb, unsigned timeoutms)
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

    virtual void cancel()
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
};

    
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
class CSocketBufferReader: public CInterface, implements ISocketBufferReader
{
    class SocketElem: public CInterface, implements ISocketSelectNotify
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
            size32_t sz = blk.length();
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
            return blk.length();
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


extern jlib_decl void markNodeCentral(SocketEndpoint &ep)
{
#ifdef CENTRAL_NODE_RANDOM_DELAY
    CriticalBlock block(CSocket::crit);
    CentralNodeArray.append(ep);
#endif
}


static CSocket *prepareSocket(unsigned idx,const SocketEndpoint &ep, ISocketConnectNotify &inotify)
{
    Owned<CSocket> sock = new CSocket(ep,sm_tcp,NULL);
    int err = sock->pre_connect(false);
    if ((err == EINPROGRESS)||(err == EWOULDBLOCK)) 
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
    class SocketElem: public CInterface, implements ISocketSelectNotify
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
                else if ((err==ETIMEDOUT)||(err==ECONNREFUSED))  { 
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
        loop {
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
            : retsockets(_retsockets),sect(_sect)
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
            PROGLOG("multiConnect failed to %s with %d",ep.getUrlStr(s).str(),err);
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



StringBuffer &SocketEndpointArray::getText(StringBuffer &text)
{
    unsigned count = ordinality();
    if (!count)
        return text;
    if (count==1)
        return item(0).getUrlStr(text);
    byte lastip[4];
    const SocketEndpoint &first = item(0);
    bool lastis4 = first.getNetAddress(sizeof(lastip),&lastip)==sizeof(lastip);
    unsigned short lastport = first.port;
    first.getIpText(text);
    unsigned rep=0;
    unsigned range=0;
    for (unsigned i=1;i<count;i++) {
        byte ip[4];
        const SocketEndpoint &ep = item(i);
        bool is4 = ep.getNetAddress(sizeof(ip),&ip)==sizeof(ip);
        if (!lastis4||!is4) {
            flushText(text,lastport,rep,range);
            text.append(',');
            ep.getIpText(text);
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
        else if (*s=='-') {
            *s = 0;
            s = (char *)getnum(s+1,rng);
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
            ep.getIpText(tmp);
            size32_t l = tmp.length();
            dc++;
            loop { 
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

void SocketEndpointArray::fromText(const char *text,unsigned defport) 
{
    // this is quite complicated with (mixed) IPv4 and IPv6
    // only support 'full' IPv6 and no ranges


    char *str = strdup(text);
    char *s = str;
    SocketEndpoint ep;
    bool eol = false;
    loop {
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

StringBuffer IpSubNet::getNetText(StringBuffer &text) const
{
    char tmp[INET6_ADDRSTRLEN];
    const char *res  = ::isIp4(net) ? _inet_ntop(AF_INET, &net[3], tmp, sizeof(tmp))
                                    : _inet_ntop(AF_INET6, &net, tmp, sizeof(tmp));
    return text.append(res);
}

StringBuffer IpSubNet::getMaskText(StringBuffer &text) const
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

StringBuffer lookupHostName(const IpAddress &ip,StringBuffer &ret)
{
// not a common routine (no Jlib function!) only support IPv4 initially
    unsigned ipa;
    if (ip.getNetAddress(sizeof(ipa),&ipa)==sizeof(ipa)) {
        struct hostent *phostent = gethostbyaddr( (char *) &ipa, sizeof(ipa), PF_INET);
        if (phostent)
            ret.append(phostent->h_name);
        else
            ip.getIpText(ret);
    }
    else
        ip.getIpText(ret);
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
    ~CSocketEndpointHashTable() { kill(); }
    
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


class CSocketConnectWait: public CInterface, implements ISocketConnectWait
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
        char peer[256];
        peer[0] = 'C';
        peer[1] = '!';
        strcpy(peer+2,sock->hostname?sock->hostname:"(NULL)");
        free(sock->tracename);
        sock->tracename = strdup(peer);
#endif              
    }

    void failedConnect()
    {
        STATS.failedconnects++;
        STATS.failedconnecttime+=usTick()-startt;
        const char* tracename = sock->tracename;
        THROWJSOCKEXCEPTION(JSOCKERR_connection_failed);
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
            if (!isopen||initerr) {
                isopen = true;
                err = initerr?initerr:sock->pre_connect(false);
                initerr = 0;
                if ((err == EINPROGRESS)||(err == EWOULDBLOCK))
                    err = 0; // continue
                else {
                    if (err==0)
                        connectdone = true; // done immediately
                    else if(!oneshot) //  probably ECONNREFUSED but treat all errors same
                        refused_sleep((waitremaining==remaining)?waittm:connecttm,refuseddelay); // this stops becoming cpu bound
                }
            }
            if (!connectdone&&(err==0)) {
                SOCKET s = sock->sock;
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
                if (rc==0) 
                    break; // timeout
                done = true;
                err = 0;
                if (rc>0) {
                    // select succeeded - return error from socket (0 if connected)
                    socklen_t errlen = sizeof(err);
                    rc = getsockopt(s, SOL_SOCKET, SO_ERROR, (char *)&err, &errlen); // check for error
                    if ((rc!=0)&&!err) 
                        err = ERRNO();  // some implementations of getsockopt duff
                    if (err&&!oneshot) //  probably ECONNREFUSED but treat all errors same
                        refused_sleep((waitremaining==remaining)?waittm:connecttm,refuseddelay); // this stops becoming cpu bound
                }
                else { // select failed
                    err = ERRNO();
                    LOGERR(err,2,"CSocketConnectWait ::select");
                }
            }
            if (err==0) {
                err = sock->post_connect();
                if (err==0) {
                    successfulConnect();
                    return sock.getClear();
                }
            }
            sock->errclose();
            isopen = false;
        } while (!waittimedout&&!oneshot);
        if (connectimedout) {
            STATS.failedconnects++;
            STATS.failedconnecttime+=usTick()-startt;
            const char* tracename = sock->tracename;
            THROWJSOCKEXCEPTION(JSOCKERR_connection_failed);
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
        THROWJSOCKEXCEPTION2(JSOCKERR_bad_address);

    SOCKET maxSocket = 0;
    T_FD_SET fds;
    XFD_ZERO(&fds);

    //Add each SOCKET in array to T_FD_SET
#ifdef _DEBUG
    StringBuffer dbgSB("wait_multiple() on sockets :");
#endif
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
#ifdef _DEBUG
    DBGLOG("%s",dbgSB.str());
#endif

    //Check socket states
    int res;
    if (timeoutMS == WAIT_FOREVER)
        res = ::select( maxSocket + 1, isRead ? (fd_set *)&fds : NULL, isRead ? NULL : (fd_set *)&fds, NULL, NULL );
    else
    {
        struct timeval tv;
        tv.tv_sec = timeoutMS / 1000;
        tv.tv_usec = (timeoutMS % 1000)*1000;
        res = ::select( maxSocket + 1,  isRead ? (fd_set *)&fds : NULL, isRead ? NULL : (fd_set *)&fds, NULL, &tv );
    }

    if (res != SOCKET_ERROR)
    {
#ifdef _DEBUG
        StringBuffer dbgSB("wait_multiple() ready socket(s) :");
#endif
        //Build up list of socks which are ready for accept read/write without blocking
        for (aindex_t idx = 0; res && idx < socks.length(); idx++)
        {
            if (FD_ISSET(socks.item(idx), &fds))
            {
#ifdef _DEBUG
                dbgSB.appendf(" %d",socks.item(idx));
#endif
                readySocks.append(socks.item(idx));
                if (readySocks.length() == res)
                    break;
            }
        }
#ifdef _DEBUG
        if (res)
            DBGLOG("%s",dbgSB.str());
#endif
    }
    else
    {
        int err = ERRNO();
        if (err != EINTRCALL)
        {
            throw MakeStringException(-1,"wait_multiple::select error %d", err);
        }
    }
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
