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

// Some ssl prototypes use char* where they should be using const char *, resulting in lots of spurious warnings
#ifndef _MSC_VER
#pragma GCC diagnostic ignored "-Wwrite-strings"
#endif

//jlib
#include "jliball.hpp"
#include "string.h"

#ifdef _WIN32
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stddef.h>
#include <errno.h>
#endif
#include <signal.h>

//openssl
#include <opensslcommon.hpp>
#include <openssl/rsa.h>
#include <openssl/crypto.h>
#ifndef _WIN32
//x509.h includes evp.h, which in turn includes des.h which defines 
//crypt() that throws different exception than in unistd.h
//(this causes build break on linux) so exclude it
#define crypt DONT_DEFINE_CRYPT
#include <openssl/x509.h>
#undef  crypt
#else
#include <openssl/x509.h>
#endif
#include <openssl/ssl.h>
#include <openssl/pem.h>
#include <openssl/err.h>
#include <openssl/conf.h>
#include <openssl/x509v3.h>
#include <openssl/bn.h>
#include <openssl/x509.h>

#include "jsmartsock.ipp"
#include "cryptocommon.hpp"
#include "securesocket.hpp"

using namespace cryptohelper;

static JSocketStatistics *SSTATS;

#define CHK_NULL(x) if((x)==NULL) exit(1)
#define CHK_ERR(err, s) if((err)==-1){perror(s);exit(1);}
#define CHK_SSL(err) if((err) ==-1){ERR_print_errors_fp(stderr); exit(2);}

static int pem_passwd_cb(char* buf, int size, int rwflag, void* password)
{
    strncpy(buf, (char*)password, size);
    buf[size - 1] = '\0';
    return(strlen(buf));
}

static void readBio(BIO* bio, StringBuffer& buf)
{
    char readbuf[1024];

    int len = 0;
    while((len = BIO_read(bio, readbuf, 1024)) > 0)
    {
        buf.append(len, readbuf);
    }
}


interface ISecureSocketContextCallback : implements IInterface
{
    virtual unsigned getVersion() = 0; // Check the version of the context to see if the SSL context needs to be recreated
    virtual SSL * createActiveSSL() = 0; // Must be called after getVersion()
};

//Use a namespace to prevent clashes with a class of the same name in jhtree
namespace securesocket
{

class CStringSet : public CInterface
{
    class StringHolder : public CInterface
    {
    public:
        StringBuffer m_str;
        StringHolder(const char* str)
        {
            m_str.clear().append(str);
        }

        const char *queryFindString() const { return m_str.str(); }
    };

private:
    OwningStringSuperHashTableOf<StringHolder> strhash;

public:
    void add(const char* val)
    {
        StringHolder* h1 = strhash.find(val);
        if(h1 == NULL)
            strhash.add(*(new StringHolder(val)));
    }

    bool contains(const char* val)
    {
        StringHolder* h1 = strhash.find(val);
        return (h1 != NULL);
    }
};

class CSecureSocket : implements ISecureSocket, public CInterface
{
private:
    struct ScopedNonBlockingMode
    {
        CSecureSocket *socket = nullptr;
        bool prevMode = false;
        void init(CSecureSocket *_socket) { socket = _socket; prevMode = socket->set_nonblock(true); }
        ~ScopedNonBlockingMode() { if (socket) socket->set_nonblock(prevMode); }
    };

    SSL*        m_ssl;
    Linked<ISecureSocketContextCallback> contextCallback;
    Owned<ISocket> m_socket;
    bool        nonBlocking;
    bool        m_verify;
    bool        m_address_match;
    CStringSet* m_peers;
    int         m_loglevel;
    bool        m_isSecure;
    StringBuffer m_fqdn;
    size32_t    nextblocksize = 0;
    unsigned    blockflags = BF_ASYNC_TRANSFER;
    unsigned    blocktimeoutms = WAIT_FOREVER;
    unsigned    contextVersion;
#ifdef USERECVSEM
    static Semaphore receiveblocksem;
    bool             receiveblocksemowned; // owned by this socket
#endif
private:
    StringBuffer& get_cn(X509* cert, StringBuffer& cn);
    bool verify_cert(X509* cert);

public:
    IMPLEMENT_IINTERFACE;

    CSecureSocket(ISocket* sock, int sockfd, ISecureSocketContextCallback * callback, bool verify = false, bool addres_match = false, CStringSet* m_peers = NULL, int loglevel=SSLogNormal, const char *fqdn = nullptr);
    ~CSecureSocket();

    virtual int secure_accept(int logLevel);
    virtual int secure_connect(int logLevel);

    virtual int logPollError(unsigned revents, const char *rwstr);
    virtual int wait_read(unsigned timeoutms);
    virtual void read(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read,unsigned timeoutsecs);
    virtual void readtms(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read, unsigned timeoutms);
    virtual size32_t write(void const* buf, size32_t size);
    virtual size32_t writetms(void const* buf, size32_t minSize, size32_t size, unsigned timeoutms=WAIT_FOREVER);

    void checkForUpdatedContext()
    {
        //Check if a new ssl context should be created.
        //No need for a critical section because the socket functions are never accessed by multiple threads at the same time
        //It is possible that createActiveSSL() may be for a later version - but that will only mean that the same context
        //is recreated when the version number is seen to have changed.
        unsigned activeVersion = contextCallback->getVersion();
        if (activeVersion != contextVersion)
        {
            DBGLOG("CSecureSocket: Updating secure socket context from version %u to %u", contextVersion, activeVersion);
            contextVersion = activeVersion;
            SSL_free(m_ssl);
            m_ssl = contextCallback->createActiveSSL();
        }
    }


    virtual StringBuffer& get_ssl_version(StringBuffer& ver)
    {
        ver.set(SSL_get_version(m_ssl));
        return ver;
    }

    //The following are the functions from ISocket that haven't been implemented.


    virtual void read(void* buf, size32_t size)
    {
        size32_t size_read;
        // MCK - this was:
        // readTimeout(buf, size, size, size_read, 0, false);
        // but that is essentially a non-blocking read() and we want a blocking read() ...
        // read() is always expecting size bytes so min_size should be size
        readtms(buf, size, size, size_read, WAIT_FOREVER);
    }

    virtual size32_t get_max_send_size()
    {
        throw MakeStringException(-1, "CSecureSocket::get_max_send_size: not implemented");
    }

    //
    // This method is called by server to accept client connection
    //
    virtual ISocket* accept(bool allowcancel=false, SocketEndpoint *peerEp=nullptr) // not needed for UDP
    {
        throw MakeStringException(-1, "CSecureSocket::accept: not implemented");
    }

    //
    // This method is called to check whether a socket is ready to write (i.e. some free buffer space)
    // 
    virtual int wait_write(unsigned timeout)
    {
        return m_socket->wait_write(timeout);
    }

    //
    // can be used with write to allow it to return if it would block
    // be sure and restore to old state before calling other functions on this socket
    //
    virtual bool set_nonblock(bool on) // returns old state
    {
        if (on == nonBlocking)
            return nonBlocking;
        if (on == m_socket->set_nonblock(on)) // this call should return opposite state, if it doesn't it failed
            return nonBlocking;

        nonBlocking = on;
        return !nonBlocking; // previous state
    }

    // enable 'nagling' - small packet coalescing (implies delayed transmission)
    //
    virtual bool set_nagle(bool on) // returns old state
    {
        throw MakeStringException(-1, "CSecureSocket::set_nagle: not implemented");
    }

    // set 'linger' time - time close will linger so that outstanding unsent data will be transmited
    //
    virtual void set_linger(int lingersecs)  
    {
        m_socket->set_linger(lingersecs);
    }

    //
    // Cancel accept operation and close socket
    //
    virtual void  cancel_accept() // not needed for UDP
    {
        m_socket->cancel_accept();
    }

    //
    // Shutdown socket: prohibit write and read operations on socket
    //
    virtual void  shutdown(unsigned mode) // not needed for UDP
    {
        m_socket->shutdown(mode);
    }

    // Same as shutdown, but never throws an exception (to call from closedown destructors)
    virtual void  shutdownNoThrow(unsigned mode)
    {
        m_socket->shutdownNoThrow(mode);
    }

    // Get local name of accepted (or connected) socket and returns port
    virtual int name(char *name,size32_t namemax)
    {
        return m_socket->name(name, namemax);
    }

    // Get peer name of socket and returns port - in UDP returns return addr
    virtual int peer_name(char *name,size32_t namemax)
    {
        return m_socket->peer_name(name, namemax);
    }

    // Get peer endpoint of socket - in UDP returns return addr
    virtual SocketEndpoint &getPeerEndpoint(SocketEndpoint &ep)
    {
        return m_socket->getPeerEndpoint(ep);
    }

    // Get peer ip of socket - in UDP returns return addr
    virtual IpAddress &getPeerAddress(IpAddress &addr)
    {
        return m_socket->getPeerAddress(addr);
    }

    // Get local endpoint of socket
    virtual SocketEndpoint &getEndpoint(SocketEndpoint &ep) const override
    {
        return m_socket->getEndpoint(ep);
    }

    virtual unsigned __int64 getStatistic(StatisticKind kind) const override
    {
        return m_socket->getStatistic(kind);
    }

    //
    // Close socket
    //
    virtual bool connectionless() // true if accept need not be called (i.e. UDP)
    {
        throw MakeStringException(-1, "CSecureSocket::connectionless: not implemented");
    }

    virtual void set_return_addr(int port,const char *name) // used for UDP servers only
    {
        throw MakeStringException(-1, "CSecureSocket::set_return_addr: not implemented");
    }

    // Block functions 
    // TLS TODO: can we move these block* into a base class for both CSocket and CSecureSocket ?

    virtual void  set_block_mode(                // must be called before block operations
                            unsigned flags,      // BF_* flags (must match receive_block)
                            size32_t recsize=0,  // record size (required for rec compression)
                            unsigned timeout=0   // timeout in msecs (0 for no timeout)
                  );

    virtual bool  send_block( 
                            const void *blk,     // data to send
                            size32_t sz          // size to send (0 for eof)
                  );

    virtual size32_t receive_block_size();       // get size of next block (always must call receive_block after)

    virtual size32_t receive_block(
                            void *blk,           // receive pointer
                            size32_t sz          // max size to read (0 for sync eof) 
                     );                          // if less than block size truncates block

    virtual void  close()
    {
        m_socket->close();  
    }

    virtual unsigned OShandle() const             // for internal use
    {
        return m_socket->OShandle();
    }

    virtual bool isValid() const
    {
        return m_socket->isValid();
    }

    virtual size32_t avail_read()            // called after wait_read to see how much data available
    {
        int pending = SSL_pending(m_ssl);
        if(pending > 0)
            return pending;
        // pending == 0 : check if there still might be data to read
        // (often used as a check for if socket was closed by peer)
        size32_t avr = m_socket->avail_read();
        if (avr > 0)
        {
            // TLS TODO: MCK - needs to be thought out and refactored
            // Bytes here may be part of encrypted SSL record.
            // If SSL_MODE_AUTO_RETRY set (and it is) then bytes
            // here may also be from auto-renegotiation.
            // If return 0 here, caller may think socket was closed
            // If return >0 here, caller may block on next SSL_read()
            // Only two locations where this value other than !=0 is used:
            //     CRemoteFileServer->notifySelected() (dafsserver)
            //     CMPPacketReader->notifySelected() (mpcomm)
            return 1;
            /* --------------------------------
            // bytes may be SSL/TLS protocol and not part of msg
            byte c[2];
            // TODO this may block ...
            pending = SSL_peek(m_ssl, c, 1);
            // 0 almost always means socket was closed
            if (pending == 0)
                return 0;
            if (pending > 0)
                return SSL_pending(m_ssl);
            // pending < 0 : TODO should handle SSL_ERROR_WANT_READ/WRITE error
            if (m_loglevel >= SSLogNormal)
            {
                int ret = SSL_get_error(m_ssl, pending);
                char errbuf[512];
                ERR_error_string_n(ERR_get_error(), errbuf, 512);
                errbuf[511] = '\0';
                DBGLOG("SSL_peek (avail_read) returns error %d - %s", ret, errbuf);
            }
            -------------------------------- */
        }
        return 0;
    }

    virtual size32_t write_multiple(unsigned num, const void **buf, size32_t *size)
    {
        size32_t res = 0;
        for (int i=0; i<num; i++)
        {
            if (size[i] > 0)
            {
                // non-tls version tries to write equal 64k chunks, but unless we set
                // SSL_MODE_ENABLE_PARTIAL_WRITE, we should write each buf[i] entirely ...
                res += write(buf[i], size[i]);
            }
        }
        return res;
    }

    virtual size32_t get_send_buffer_size() // get OS send buffer
    {
        throw MakeStringException(-1, "CSecureSocket::get_send_buffer_size: not implemented");
    }

    void set_send_buffer_size(size32_t sz)  // set OS send buffer size
    {
        throw MakeStringException(-1, "CSecureSocket::set_send_buffer_size: not implemented");
    }

    bool join_multicast_group(SocketEndpoint &ep)   // for udp multicast
    {
        throw MakeStringException(-1, "CSecureSocket::join_multicast_group: not implemented");
        return false;
    }

    bool leave_multicast_group(SocketEndpoint &ep)  // for udp multicast
    {
        throw MakeStringException(-1, "CSecureSocket::leave_multicast_group: not implemented");
        return false;
    }

    void set_ttl(unsigned _ttl)   // set ttl
    {
        throw MakeStringException(-1, "CSecureSocket::set_ttl: not implemented");
    }

    size32_t get_receive_buffer_size()  // get OS send buffer
    {
        throw MakeStringException(-1, "CSecureSocket::get_receive_buffer_size: not implemented");
    }

    void set_receive_buffer_size(size32_t sz)   // set OS send buffer size
    {
        throw MakeStringException(-1, "CSecureSocket::set_receive_buffer_size: not implemented");
    }

    virtual void set_keep_alive(bool set) // set option SO_KEEPALIVE
    {
        m_socket->set_keep_alive(set);
    }

    virtual size32_t udp_write_to(const SocketEndpoint &ep, void const* buf, size32_t size)
    {
        throw MakeStringException(-1, "CSecureSocket::udp_write_to: not implemented");
    }

    virtual bool check_connection()
    {
        return m_socket->check_connection();
    }

    virtual bool isSecure() const override
    {
        return m_isSecure;
    }
};

#ifdef USERECVSEM
Semaphore CSecureSocket::receiveblocksem(2);
#endif

/**************************************************************************
 *  CSecureSocket -- secure socket layer implementation using openssl     *
 **************************************************************************/
CSecureSocket::CSecureSocket(ISocket* sock, int sockfd, ISecureSocketContextCallback * callback, bool verify, bool address_match, CStringSet* peers, int loglevel, const char *fqdn)
    : contextCallback(callback)
{
    if (sock)
        sockfd = sock->OShandle();
    m_socket.setown(sock);
    contextVersion = callback->getVersion();
    m_ssl = callback->createActiveSSL();

    m_verify = verify;
    m_address_match = address_match;
    m_peers = peers;;
    m_loglevel = loglevel;
    m_isSecure = false;

    nonBlocking = false;
#if defined(O_NONBLOCK) 
    int flags = fcntl(sockfd, F_GETFL, 0);
    if (-1 != flags)
        nonBlocking = 0 != (flags & O_NONBLOCK);
#elif defined(_WIN32)
    u_long flags;
    if (ioctlsocket(sockfd, FIONBIO, &flags)==0)
    {
        if (flags)
            nonBlocking = true;
    }
#else
    int flags;
    if (ioctl(sock, FIONBIO, &flags)==0)
    {
        if (flags)
            nonBlocking = true;
    }
#endif
    if(m_ssl == NULL)
    {
        throw MakeStringException(-1, "Can't create ssl");
    }

    // there is no MSG_NOSIGNAL or SO_NOSIGPIPE for SSL_write() ...
#ifndef _WIN32
    signal(SIGPIPE, SIG_IGN);
#endif

    SSL_set_fd(m_ssl, sockfd);

    if (fqdn)
        m_fqdn.set(fqdn);
}

CSecureSocket::~CSecureSocket()
{
    SSL_free(m_ssl);
}

StringBuffer& CSecureSocket::get_cn(X509* cert, StringBuffer& cn)
{
    X509_NAME *subj;
    char      data[256];
    int       extcount;
    int       found = 0;

    if ((extcount = X509_get_ext_count(cert)) > 0)
    {
        int i;

        for (i = 0;  i < extcount;  i++)
        {
            const char              *extstr;
            X509_EXTENSION    *ext;

            ext = X509_get_ext(cert, i);
            extstr = OBJ_nid2sn(OBJ_obj2nid(X509_EXTENSION_get_object(ext)));

            if (!strcmp(extstr, "subjectAltName"))
            {
                int                  j;
                unsigned char        *data;
                STACK_OF(CONF_VALUE) *val;
                CONF_VALUE           *nval;
#if (OPENSSL_VERSION_NUMBER > 0x00909000L) 
                const X509V3_EXT_METHOD    *meth;
#else
                X509V3_EXT_METHOD    *meth;
#endif 
                void                 *ext_str = NULL;

                if (!(meth = X509V3_EXT_get(ext)))
                    break;
#if OPENSSL_VERSION_NUMBER < 0x10100000L
                data = ext->value->data;
                auto length = ext->value->length;
#else
                data = X509_EXTENSION_get_data(ext)->data;
                auto length = X509_EXTENSION_get_data(ext)->length;
#endif
#if (OPENSSL_VERSION_NUMBER > 0x00908000L) 
                if (meth->it)
                    ext_str = ASN1_item_d2i(NULL, (const unsigned char **)&data, length,
                        ASN1_ITEM_ptr(meth->it));
                else
                    ext_str = meth->d2i(NULL, (const unsigned char **) &data, length);
#elif (OPENSSL_VERSION_NUMBER > 0x00907000L)     
                if (meth->it)
                    ext_str = ASN1_item_d2i(NULL, (unsigned char **)&data, length,
                        ASN1_ITEM_ptr(meth->it));
                else
                    ext_str = meth->d2i(NULL, (unsigned char **) &data, length);
#else
                    ext_str = meth->d2i(NULL, &data, length);
#endif
                val = meth->i2v(meth, ext_str, NULL);
                for (j = 0;  j < sk_CONF_VALUE_num(val);  j++)
                {
                    nval = sk_CONF_VALUE_value(val, j);
                    if (!strcmp(nval->name, "DNS"))
                    {
                        cn.append(nval->value);                     
                        found = 1;
                        break;
                    }
                }
            }
            if (found)
                break;
        }
    }

    if (!found && (subj = X509_get_subject_name(cert)) &&
        X509_NAME_get_text_by_NID(subj, NID_commonName, data, 256) > 0)
    {
        data[255] = 0;
        cn.append(data);
    }

    return cn;
}

bool CSecureSocket::verify_cert(X509* cert)
{
    char *s, oneline[1024];

    s = X509_NAME_oneline (X509_get_subject_name (cert), oneline, 1024);

    s = X509_NAME_oneline (X509_get_issuer_name  (cert), oneline, 1024);

    StringBuffer cn;
    get_cn(cert, cn);

    if(cn.length() == 0)
        throw MakeStringException(-1, "cn of the certificate can't be found");

    if(m_address_match)
    {
        SocketEndpoint ep;
        m_socket->getPeerEndpoint(ep);
        StringBuffer iptxt;
        ep.getHostText(iptxt);
        SocketEndpoint cnep(cn.str());
        StringBuffer cniptxt;
        cnep.getHostText(cniptxt);
        DBGLOG("peer ip=%s, certificate ip=%s", iptxt.str(), cniptxt.str());
        if(!(cniptxt.length() > 0 && stricmp(iptxt.str(), cniptxt.str()) == 0))
        {
            DBGLOG("Source address of the request doesn't match the certificate");
            return false;
        }
    }
    
    if (m_peers->contains("anyone") || m_peers->contains(cn.str()))
        return true;
    else
    {
        DBGLOG("%s not among trusted peers, verification failed", cn.str());
        return false;
    }
}

int CSecureSocket::secure_accept(int logLevel)
{
    checkForUpdatedContext();
    int err;
    err = SSL_accept(m_ssl);
    if(err == 0)
    {
        int ret = SSL_get_error(m_ssl, err);
        // if err == 0 && ret == SSL_ERROR_SYSCALL
        // then client closed connection gracefully before ssl neg
        // which can happen with port scan / VIP ...
        // NOTE: ret could also be SSL_ERROR_ZERO_RETURN if client closed
        // gracefully after ssl neg initiated ...
        if ( (logLevel > SSLogNormal) || (ret != SSL_ERROR_SYSCALL) )
        {
            char errbuf[512];
            ERR_error_string_n(ERR_get_error(), errbuf, 512);
            DBGLOG("SSL_accept returned 0, error - %s", errbuf);
        }
        if (ret == SSL_ERROR_SYSCALL)
            return PORT_CHECK_SSL_ACCEPT_ERROR;
        return -1;
    }
    else if(err < 0)
    {
        int ret = SSL_get_error(m_ssl, err);
        unsigned long errnum = ERR_get_error();
        // Since err < 0 we call ERR_get_error() for additional info
        // if ret == SSL_ERROR_SYSCALL and ERR_get_error() == 0 then
        // its most likely a port scan / load balancer check so do not log
        // with SSL 1.1.1e and 3.0 if ret == SSL_ERROR_SSL and ERR_get_error reason is EOF
        // its also most likely a port scan / load balancer check so do not log
        int srtn = err;
        if ( (ret == SSL_ERROR_SYSCALL) && (errnum == 0) )
            srtn = PORT_CHECK_SSL_ACCEPT_ERROR;
        // if ctx option SSL_OP_IGNORE_UNEXPECTED_EOF is set then will get SSL_ERROR_ZERO_RETURN ...
        if ( (ret == SSL_ERROR_ZERO_RETURN) && (errnum == 0) )
            srtn = PORT_CHECK_SSL_ACCEPT_ERROR;
        // otherwise will get SSL_ERROR_SSL and unexpected eof ...
#if defined(SSL_R_UNEXPECTED_EOF_WHILE_READING)
        if ( (ret == SSL_ERROR_SSL) && (ERR_GET_REASON(errnum) == SSL_R_UNEXPECTED_EOF_WHILE_READING) )
            srtn = PORT_CHECK_SSL_ACCEPT_ERROR;
#endif
        if ((logLevel <= SSLogNormal) && (srtn == PORT_CHECK_SSL_ACCEPT_ERROR))
            return srtn;
        char errbuf[512];
        ERR_error_string_n(errnum, errbuf, 512);
        errbuf[511] = '\0';
        DBGLOG("SSL_accept returned %d, SSL_get_error=%d, error - %s", err, ret, errbuf);
        if(strstr(errbuf, "error:1408F455:") != NULL)
        {
            DBGLOG("Unrecoverable SSL library error.");
            _exit(0);
        }
        return srtn;
    }

    if (logLevel > SSLogNormal)
        DBGLOG("SSL accept ok, using %s", SSL_get_cipher(m_ssl));

    if(m_verify)
    {
        bool verified = false;
        // Get client's certificate (note: beware of dynamic allocation) - opt 
        X509* client_cert = SSL_get_peer_certificate (m_ssl);
        if (client_cert != NULL) 
        {
            // We could do all sorts of certificate verification stuff here before
            // deallocating the certificate.
            verified = verify_cert(client_cert);
            X509_free (client_cert);
        }

        if(!verified)
            throw MakeStringException(-1, "certificate verification failed");

    }

    m_isSecure = true;
    return 0;
}

int CSecureSocket::secure_connect(int logLevel)
{
    if (m_fqdn.length() > 0)
    {
        if (!streq(m_fqdn.str(), "."))
            SSL_set_tlsext_host_name(m_ssl, m_fqdn.str());
    }

    int err = SSL_connect (m_ssl);                     
    if(err <= 0)
    {
        int ret = SSL_get_error(m_ssl, err);
        char errbuf[512];
        ERR_error_string_n(ERR_get_error(), errbuf, 512);
        DBGLOG("SSL_connect error - %s, SSL_get_error=%d, error - %d", errbuf,ret, err);
        throw MakeStringException(-1, "SSL_connect failed: %s", errbuf);
    }
    
    if (logLevel > SSLogNormal)
        DBGLOG("SSL connect ok, using %s", SSL_get_cipher (m_ssl));

    // Currently only do fake verify - simply logging the subject and issuer
    // The verify parameter makes it possible for the application to verify only
    // once per session and cache the result of the verification.
    if(m_verify)
    {
        // Following two steps are optional and not required for
        // data exchange to be successful.
        
        // Get server's certificate (note: beware of dynamic allocation) - opt
        X509* server_cert = SSL_get_peer_certificate (m_ssl);
        bool verified = false;
        if(server_cert != NULL)
        {
            // We could do all sorts of certificate verification stuff here before
            // deallocating the certificate.
            verified = verify_cert(server_cert);

            X509_free (server_cert);    
        }

        if(!verified)
            throw MakeStringException(-1, "certificate verification failed");

    }

    m_isSecure = true;
    return 0;
}

//
// log poll() errors
//
int CSecureSocket::logPollError(unsigned revents, const char *rwstr)
{
    return m_socket->logPollError(revents, rwstr);
}

//
// This method is called to check whether a socket has data ready
// 
int CSecureSocket::wait_read(unsigned timeoutms)
{
    int pending = SSL_pending(m_ssl);
    if(pending > 0)
        return pending;
    return m_socket->wait_read(timeoutms);
}

void CSecureSocket::readtms(void* buf, size32_t min_size, size32_t max_size, size32_t &sizeRead, unsigned timeoutMs)
{
    sizeRead = 0;
    CCycleTimer timer;

    // for semantics to work with a timeout, have to be non-blocking when reading SSL
    // because wait_read can't guarantee that there are bytes ready to read, only that
    // there are bytes pending on the underlying socket.
    // We put in non-blocking mode, so that if after wait_read says there's something,
    // SSL_read won't block and will respond with a SSL_ERROR_WANT_READ/SSL_ERROR_WANT_WRITE
    // if not ready.
    ScopedNonBlockingMode scopedNonBlockingMode;
    if (WAIT_FOREVER != timeoutMs)
        scopedNonBlockingMode.init(this);

    int ssl_err = SSL_ERROR_WANT_READ; // initially call will be wait_read
    while (true)
    {
        int rc;
        unsigned remainingMs = timer.remainingMs(timeoutMs);
        if (ssl_err == SSL_ERROR_WANT_READ)
            rc = wait_read(remainingMs);
        else // SSL_ERROR_WANT_WRITE
            rc = wait_write(remainingMs);
        if (rc < 0)
            THROWJSOCKEXCEPTION_MSG(SOCKETERRNO(), "wait_read error");
        if (rc == 0)
            THROWJSOCKEXCEPTION_MSG(JSOCKERR_timeout_expired, "timeout expired");

        ERR_clear_error();
        rc = SSL_read(m_ssl, (char*)buf + sizeRead, max_size - sizeRead);

        if (rc > 0)
        {
            sizeRead += rc;
            if (sizeRead >= min_size)
                break;
        }
        else if (0 == rc)
        {
            if (sizeRead >= min_size)
                break; // suppress graceful close exception if have already read minimum
            THROWJSOCKEXCEPTION(JSOCKERR_graceful_close);
        }
        else
        {
            ssl_err = SSL_get_error(m_ssl, rc);
            // NB: if timeout != WAIT_FOREVER, nonBlocking should always be true here
            if (nonBlocking && (ssl_err == SSL_ERROR_WANT_READ || ssl_err == SSL_ERROR_WANT_WRITE)) // NB: SSL_read can cause SSL_ERROR_WANT_WRITE
            {
                // NB: we must be below min_size if here (otherwise would have exited in (rc > 0) block above)

                // To maintain consistent semantics with jsocket, we continue waiting even in the min_size = 0 case.
                // NB: jsocket::readtms always blocks (wait_read) initially, meaning in effect min_size is always treated as >0
            }
            else
            {
                char errbuf[512];
                ERR_error_string_n(ssl_err, errbuf, 512);
                ERR_clear_error();
                VStringBuffer errmsg("SSL_read error %d - %s", ssl_err, errbuf);
                if (m_loglevel >= SSLogMax)
                    DBGLOG("Warning: %s", errmsg.str());
                THROWJSOCKEXCEPTION_MSG(ssl_err, errmsg);
            }
            // here only if nonBlocking && WANT_READ or WANT_WRITE
            // since we do not have size_min yet, loop around and wait for more.
        }
    }
}

void CSecureSocket::read(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read,unsigned timeoutsecs)
{
    unsigned timeoutMs = (timeoutsecs==WAIT_FOREVER) ? WAIT_FOREVER : (timeoutsecs * 1000);
    readtms(buf, min_size, max_size, size_read, timeoutMs);
}

size32_t CSecureSocket::writetms(void const* buf, size32_t minSize, size32_t size, unsigned timeoutMs)
{
    if (size == 0)
        return 0;

    CCycleTimer timer;

    ScopedNonBlockingMode scopedNonBlockingMode;
    if (WAIT_FOREVER != timeoutMs)
        scopedNonBlockingMode.init(this);

    while (true)
    {
        int rc = SSL_write(m_ssl, buf, size);
        if (rc > 0)
        {
            // NB: minSize not used here, because not using SSL_MODE_ENABLE_PARTIAL_WRITE
            dbgassertex(size == rc);
            return rc;
        }
        int ssl_err = SSL_get_error(m_ssl, rc);
        if (nonBlocking && (ssl_err == SSL_ERROR_WANT_READ || ssl_err == SSL_ERROR_WANT_WRITE)) // NB: SSL_write can cause SSL_ERROR_WANT_READ
        {
            unsigned remainingMs = timer.remainingMs(timeoutMs);
            if (ssl_err == SSL_ERROR_WANT_READ)
                rc = wait_read(remainingMs);
            else // SSL_ERROR_WANT_WRITE
                rc = wait_write(remainingMs);
            if (rc < 0)
            {
                const char *msg = (ssl_err == SSL_ERROR_WANT_READ) ? "wait_read error" : "wait_write error";
                THROWJSOCKEXCEPTION_MSG(SOCKETERRNO(), msg);
            }
            if (rc == 0)
                THROWJSOCKEXCEPTION_MSG(JSOCKERR_timeout_expired, "timeout expired");
        }
        else
        {
            char errbuf[512];
            ERR_error_string_n(ssl_err, errbuf, 512);
            ERR_clear_error();
            VStringBuffer errmsg("SSL_write error %d - %s", ssl_err, errbuf);
            if (ssl_err == SSL_ERROR_ZERO_RETURN)
                THROWJSOCKEXCEPTION(JSOCKERR_graceful_close);
            else
                THROWJSOCKEXCEPTION_MSG(JSOCKERR_broken_pipe, errmsg);
        }
    }
    throwUnexpected(); // should never get here
}

size32_t CSecureSocket::write(void const* buf, size32_t size)
{
    return writetms(buf, size, size, WAIT_FOREVER);
}

// ----------------------------

void CSecureSocket::set_block_mode(unsigned flags, size32_t recsize, unsigned _timeoutms)
{
    blockflags = flags;
    nextblocksize = UINT_MAX;
    blocktimeoutms = _timeoutms?_timeoutms:WAIT_FOREVER;
}

size32_t CSecureSocket::receive_block_size()
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

size32_t CSecureSocket::receive_block(void *blk, size32_t maxsize)
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
        SSTATS = getSocketStatPtr();
        if (SSTATS)
        {
            SSTATS->blockrecvtime+=elapsed;
            SSTATS->numblockrecvs++;
            SSTATS->blockrecvsize+=sz;
        }
    }
    return sz;
}

bool CSecureSocket::send_block(const void *blk, size32_t sz)
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
    SSTATS = getSocketStatPtr();
    if (SSTATS)
    {
        SSTATS->blocksendtime+=elapsed;
        SSTATS->numblocksends++;
        SSTATS->blocksendsize+=sz;
        if (elapsed>SSTATS->longestblocksend) {
            SSTATS->longestblocksend = elapsed;
            SSTATS->longestblocksize = sz;
        }
    }
#ifdef TRACE_SLOW_BLOCK_TRANSFER
    if (elapsed>1000000*60)  // over 1min
        IWARNLOG("send_block took %ds to %s  (%d,%d,%d)",elapsed/1000000,tracename,startt2-startt,startt3-startt2,nowt-startt3);
#endif
    return true;
}

// ----------------------------

int verify_callback(int ok, X509_STORE_CTX *store, bool accept_selfsigned)
{
    if(!ok)
    {
        X509 *cert = X509_STORE_CTX_get_current_cert(store);
        int err = X509_STORE_CTX_get_error(store);
        
        char issuer[256], subject[256];
        X509_NAME_oneline(X509_get_issuer_name(cert), issuer, 256);
        X509_NAME_oneline(X509_get_subject_name(cert), subject, 256);
        
        if(accept_selfsigned && (stricmp(issuer, subject) == 0))
        {
            DBGLOG("accepting selfsigned certificate, subject=%s", subject);
            ok = true;
        }
        else
            DBGLOG("Error with certificate: issuer=%s,subject=%s,err %d - %s", issuer, subject,err,X509_verify_cert_error_string(err));
    }
    return ok;
}

int verify_callback_allow_selfSigned(int ok, X509_STORE_CTX *store)
{
    return verify_callback(ok, store, true);
}

int verify_callback_reject_selfSigned(int ok, X509_STORE_CTX *store)
{
    return verify_callback(ok, store, false);
}

const char* strtok__(const char* s, const char* d, StringBuffer& tok)
{
    if(!s || !*s || !d || !*d)
        return s;

    while(*s && strchr(d, *s))
        s++;

    while(*s && !strchr(d, *s))
    {
        tok.append(*s);
        s++;
    }

    return s;
}

static bool usePrivateKeyPEMBuffer(SSL_CTX *ctx, const char *privKeyBuf, int privKeyLen=-1)
{
    // single RSA key in buffer

    OwnedEVPBio cbio(BIO_new_mem_buf(privKeyBuf, privKeyLen));
    if (!cbio)
        return false;

    OwnedEVPRSA rsa(PEM_read_bio_RSAPrivateKey(cbio, NULL, 0, NULL));
    if (!rsa)
        return false;

    if (!SSL_CTX_use_RSAPrivateKey(ctx, rsa))
        return false;

    return true;
}

static bool useCertificateChainPEMBuffer(SSL_CTX *ctx, const char *certBuf, int certLen=-1)
{
    // this routine based on code originally from:
    // https://stackoverflow.com/questions/3810058/read-certificate-files-from-memory-instead-of-a-file-using-openssl

    // can have multiple certs in buffer

    OwnedEVPBio cbio(BIO_new_mem_buf(certBuf, certLen));
    if (!cbio)
        return false;

    OwnedX509StkPtr infoStk(PEM_X509_INFO_read_bio(cbio, NULL, NULL, NULL));
    if (!infoStk)
        return false;

    bool first = true;
    X509_INFO *infoVal;
    for (int i=0; i<sk_X509_INFO_num(infoStk); i++)
    {
        infoVal = sk_X509_INFO_value(infoStk, i);
        if (infoVal->x509)
        {
            // First cert is server/main cert. Remaining, if any, are intermediate certs.
            if (first)
            {
                first = false;

                // Set server certificate. Note that this operation increments the
                // reference count, which means that it is okay for cleanup to free it.
                if (!SSL_CTX_use_certificate(ctx, infoVal->x509))
                    return false;

                // Get ready to store intermediate certs, if any.
                SSL_CTX_clear_chain_certs(ctx);
            }
            else
            {
                // Add intermediate cert to chain.
                if (!SSL_CTX_add0_chain_cert(ctx, infoVal->x509))
                    return false;

                // Above function doesn't increment cert reference count. NULL the
                // reference to it in order to prevent it from being freed during cleanup.
                infoVal->x509 = NULL;
            }
        }
    }

    return true;
}

static bool setVerifyCertsPEMBuffer(SSL_CTX *ctx, const char *caCertBuf, int caCertLen=-1)
{
    // this routine based on code originally from:
    // https://stackoverflow.com/questions/5052563/c-openssl-use-root-ca-from-buffer-rather-than-file-ssl-ctx-load-verify-locat

    // can have multiple certs in buffer

    OwnedEVPBio cbio(BIO_new_mem_buf(caCertBuf, caCertLen));
    if (!cbio)
        return false;

    OwnedX509Store store(X509_STORE_new());
    if (!store)
        return false;

    OwnedX509StkPtr infoStk(PEM_X509_INFO_read_bio(cbio, NULL, NULL, NULL));
    if (!infoStk)
        return false;

    X509_INFO *infoVal;
    for (int i = 0; i < sk_X509_INFO_num(infoStk); i++)
    {
        infoVal = sk_X509_INFO_value(infoStk, i);
        if (infoVal->x509)
        {
            if (!X509_STORE_add_cert(store, infoVal->x509))
                return false;

            infoVal->x509 = NULL;
        }
    }

    SSL_CTX_set_cert_store(ctx, store.getClear());

    return true;
}

class CSecureSocketContext : implements ISecureSocketContext, implements ISecureSocketContextCallback, public CInterface
{
private:
    SecureSocketType sockettype;
    OwnedSSLCTX m_ctx;
#if (OPENSSL_VERSION_NUMBER > 0x00909000L) 
    const SSL_METHOD* m_meth = nullptr;
#else
    SSL_METHOD* m_meth;
#endif 

    bool m_verify = false;
    bool m_address_match = false;
    Owned<CStringSet> m_peers;
    StringAttr password;
    CriticalSection cs;
    Linked<const ISyncedPropertyTree> syncedConfig;
    unsigned configVersion = 0;

    void setSessionIdContext()
    {
        SSL_CTX_set_session_id_context(m_ctx, (const unsigned char*)"hpccsystems", 11);
    }

    void initContext(SecureSocketType sockettype)
    {
        if(sockettype == ClientSocket)
            m_meth = SSLv23_client_method();
        else
            m_meth = SSLv23_server_method();

        m_ctx.setown(SSL_CTX_new(m_meth));

        if(!m_ctx)
            throw makeStringException(-1, "ctx can't be created");

        if (sockettype == ServerSocket)
            setSessionIdContext();
    }

    void setCertificate(const char *certFileOrBuf)
    {
        if (isEmptyString(certFileOrBuf))
            return;

        if (containsEmbeddedKey(certFileOrBuf))
        {
            // can have multiple certs in buffer
            if (!useCertificateChainPEMBuffer(m_ctx, certFileOrBuf))
                throw makeEVPException(-1, "error loading certificate chain");
        }
        else if (SSL_CTX_use_certificate_chain_file(m_ctx, certFileOrBuf) <= 0)
            throw makeEVPExceptionV(-1, "error loading certificate chain file %s", certFileOrBuf);
    }

    void setPrivateKey(const char *privKeyFileOrBuf)
    {
        if (isEmptyString(privKeyFileOrBuf))
            return;

        if (containsEmbeddedKey(privKeyFileOrBuf))
        {
            // single RSA key in buffer
            if (!usePrivateKeyPEMBuffer(m_ctx, privKeyFileOrBuf))
                throw makeEVPException(-1, "error loading private key");
        }
        else if (SSL_CTX_use_PrivateKey_file(m_ctx, privKeyFileOrBuf, SSL_FILETYPE_PEM) <= 0)
            throw makeEVPExceptionV(-1, "error loading private key file %s", privKeyFileOrBuf);

        if (!SSL_CTX_check_private_key(m_ctx))
            throw makeStringException(-1, "Private key does not match the certificate public key");
    }

    void setVerifyCerts(const char *caCertsPathOrBuf)
    {
        if (isEmptyString(caCertsPathOrBuf))
            return;

        if (containsEmbeddedKey(caCertsPathOrBuf))
        {
            // can have multiple certs in buffer
            if (!setVerifyCertsPEMBuffer(m_ctx, caCertsPathOrBuf))
                throw makeStringException(-1, "Error loading CA certificates");
        }
        else if (SSL_CTX_load_verify_locations(m_ctx, caCertsPathOrBuf, NULL) != 1)
            throw makeStringExceptionV(-1, "Error loading CA certificates from %s", caCertsPathOrBuf);
    }

    void createNewContext(const IPropertyTree* config)
    {
        initContext(sockettype);

        if (config)
        {
            const char *cipherList = config->queryProp("cipherList");
            if (!cipherList || !*cipherList)
                cipherList = "ECDH+AESGCM:DH+AESGCM:ECDH+AES256:DH+AES256:ECDH+AES128:DH+AES:ECDH+3DES:DH+3DES:RSA+AESGCM:RSA+AES:RSA+3DES:!aNULL:!MD5";
            SSL_CTX_set_cipher_list(m_ctx, cipherList);

            const char* passphrase = config->queryProp("passphrase");
            if (passphrase && *passphrase)
            {
                StringBuffer pwd;
                decrypt(pwd, passphrase);
                password.set(pwd);
                SSL_CTX_set_default_passwd_cb_userdata(m_ctx, (void*)password.str());
                SSL_CTX_set_default_passwd_cb(m_ctx, pem_passwd_cb);
            }

            const char *certFileOrBuf = config->queryProp("certificate_pem");
            if (!certFileOrBuf)
                certFileOrBuf = config->queryProp("certificate");
            if (certFileOrBuf && *certFileOrBuf)
                setCertificate(certFileOrBuf);

            const char *privKeyFileOrBuf = config->queryProp("privatekey_pem");
            if (!privKeyFileOrBuf)
                privKeyFileOrBuf = config->queryProp("privatekey");
            if (privKeyFileOrBuf && *privKeyFileOrBuf)
                setPrivateKey(privKeyFileOrBuf);

            m_verify = config->getPropBool("verify/@enable");
            m_address_match = config->getPropBool("verify/@address_match");

            if(m_verify)
            {
                const char *caCertPathOrBuf = config->queryProp("verify/ca_certificates/pem");
                if (!caCertPathOrBuf)
                    caCertPathOrBuf = config->queryProp("verify/ca_certificates/@path");
                if (caCertPathOrBuf && *caCertPathOrBuf)
                    setVerifyCerts(caCertPathOrBuf);

                bool acceptSelfSigned = config->getPropBool("verify/@accept_selfsigned");
                SSL_CTX_set_verify(m_ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT | SSL_VERIFY_CLIENT_ONCE, (acceptSelfSigned) ? verify_callback_allow_selfSigned : verify_callback_reject_selfSigned);

                m_peers.setown(new CStringSet());
                const char* peersstr = config->queryProp("verify/trusted_peers");
                while(peersstr && *peersstr)
                {
                    StringBuffer onepeerbuf;
                    peersstr = strtok__(peersstr, "|", onepeerbuf);
                    if(onepeerbuf.length() == 0)
                        break;

                    const char * onepeer = onepeerbuf.str();
                    if (isdigit(*onepeer))
                    {
                        const char *dash = strrchr(onepeer, '-');
                        const char *dot = strrchr(onepeer, '.');
                        //Allow a range in the form a.b.c.d-e
                        if (dash && dot && dot < dash)
                        {
                            int first = atoi(dot+1);
                            int last = atoi(dash+1);
                            for (int i = first; i <= last; i++)
                            {
                                StringBuffer t;
                                t.append(dot-onepeer, onepeer).append('.').append(i);
                                m_peers->add(t.str());
                            }
                        }
                        else
                            m_peers->add(onepeer);
                    }
                    else
                        m_peers->add(onepeer);
                }
            }
        }

        SSL_CTX_set_mode(m_ctx, SSL_CTX_get_mode(m_ctx) | SSL_MODE_AUTO_RETRY);
    }

    void checkForUpdatedContext()
    {
        //Check if a new context should be created - it must be called within a critical section
        //NOTE: The openssl ctx is reference counted internally, so any existing sockets will still be valid.
        if (syncedConfig)
        {
            unsigned activeVersion = syncedConfig->getVersion();
            if (activeVersion != configVersion)
            {
                DBGLOG("CSecureSocketContext: Updating secure socket context from version %u to %u", configVersion, activeVersion);
                configVersion = activeVersion;
                Owned<const IPropertyTree> config = syncedConfig->getTree();
                createNewContext(config);
            }
        }
    }

public:
    IMPLEMENT_IINTERFACE

    CSecureSocketContext(const IPropertyTree* config, SecureSocketType _sockettype) : sockettype(_sockettype)
    {
        createNewContext(config);
    }

    CSecureSocketContext(const ISyncedPropertyTree* _syncedConfig, SecureSocketType _sockettype) : syncedConfig(_syncedConfig), sockettype(_sockettype)
    {
        Owned<const IPropertyTree> config;
        if (syncedConfig)
        {
            configVersion = syncedConfig->getVersion();
            config.setown(syncedConfig->getTree());
        }
        createNewContext(config);
    }

//interface ISecureSocketContext
    ISecureSocket* createSecureSocket(ISocket* sock, int loglevel, const char *fqdn)
    {
        return new CSecureSocket(sock, 0, this, m_verify, m_address_match, m_peers, loglevel, fqdn);
    }

    ISecureSocket* createSecureSocket(int sockfd, int loglevel, const char *fqdn)
    {
        return new CSecureSocket(nullptr, sockfd, this, m_verify, m_address_match, m_peers, loglevel, fqdn);
    }

//interface ISecureSocketContextCallback
    virtual unsigned getVersion()
    {
        CriticalBlock block(cs);
        checkForUpdatedContext();
        return configVersion;
    }
    virtual SSL * createActiveSSL()
    {
        //If this function is called it is either a new socket or getVersion() has been called to check it is up to date
        CriticalBlock block(cs);
        return SSL_new(m_ctx);
    }

};

class CRsaCertificate : implements ICertificate, public CInterface
{
private:
    StringAttr m_destaddr;
    StringAttr m_passphrase;
    int        m_days;
    StringAttr m_c;
    StringAttr m_s;
    StringAttr m_l;
    StringAttr m_o;
    StringAttr m_ou;
    StringAttr m_e;
    int        m_bits;

    void addNameEntry(X509_NAME *subj, const char* name, const char* value)
    {
        int nid;
        X509_NAME_ENTRY *ent;
        
        if ((nid = OBJ_txt2nid ((char*)name)) == NID_undef)
            throw MakeStringException(-1, "Error finding NID for %s\n", name);
        
        if (!(ent = X509_NAME_ENTRY_create_by_NID(NULL, nid, MBSTRING_ASC, (unsigned char*)value, -1)))
            throw MakeStringException(-1, "Error creating Name entry from NID");
        
        if (X509_NAME_add_entry (subj, ent, -1, 0) != 1)
            throw MakeStringException(-1, "Error adding entry to subject");
    }

public:
    IMPLEMENT_IINTERFACE;
    
    CRsaCertificate()
    {
        m_days = 365;
        m_bits = 1024;
    }

    virtual ~CRsaCertificate()
    {
    }

    virtual void setDestAddr(const char* destaddr)
    {
        m_destaddr.set(destaddr);
    }

    virtual void setDays(int days)
    {
        m_days = days;
    }

    virtual void setPassphrase(const char* passphrase)
    {
        m_passphrase.set(passphrase);
    }

    virtual void setCountry(const char* country)
    {
        m_c.set(country);
    }

    virtual void setState(const char* state)
    {
        m_s.set(state);
    }

    virtual void setCity(const char* city)
    {
        m_l.set(city);
    }

    virtual void setOrganization(const char* o)
    {
        m_o.set(o);
    }

    virtual void setOrganizationalUnit(const char* ou)
    {
        m_ou.set(ou);
    }

    virtual void setEmail(const char* email)
    {
        m_e.set(email);
    }

    virtual int generate(StringBuffer& certificate, StringBuffer& privkey)
    {
        if(m_destaddr.length() == 0)
            throw MakeStringException(-1, "Common Name (server's hostname or IP address) not set for certificate");
        if(m_passphrase.length() == 0)
            throw MakeStringException(-1, "passphrase not set.");
        if(m_days <= 0)
            throw MakeStringException(-1, "The number of days should be a positive integer");
        
        if(m_c.length() == 0)
            m_c.set("US");

        if(m_o.length() == 0)
            m_o.set("Customer Of Seisint");

        BIO *bio_err;
        BIO *pmem, *cmem;
        X509 *x509=NULL;
        EVP_PKEY *pkey=NULL;

        bio_err=BIO_new_fp(stderr, BIO_NOCLOSE);

        if ((pkey=EVP_PKEY_new()) == NULL)
            throw MakeStringException(-1, "can't create private key");

        if ((x509=X509_new()) == NULL)
            throw MakeStringException(-1, "can't create X509 structure");

#if OPENSSL_VERSION_NUMBER < 0x10100000L
        RSA *rsa = RSA_generate_key(m_bits, RSA_F4, NULL, NULL);
#else
        RSA *rsa = RSA_new();
        if (rsa)
        {
            BIGNUM *e;
            e = BN_new();
            if (e)
            {
                BN_set_word(e, RSA_F4);
                RSA_generate_key_ex(rsa, m_bits, e, NULL);
                BN_free(e);
            }
        }
#endif
        if (!rsa || !EVP_PKEY_assign_RSA(pkey, rsa))
        {
            char errbuf[512];
            ERR_error_string_n(ERR_get_error(), errbuf, 512);
            throw MakeStringException(-1, "EVP_PKEY_ASSIGN_RSA error - %s", errbuf);
        }

        X509_NAME *name=NULL;
        X509_set_version(x509,3);
        ASN1_INTEGER_set(X509_get_serialNumber(x509), 0); // serial number set to 0
#if OPENSSL_VERSION_NUMBER < 0x10100000L
        X509_gmtime_adj(X509_get_notBefore(x509),0);
        X509_gmtime_adj(X509_get_notAfter(x509),(long)60*60*24*m_days);
#else
        X509_gmtime_adj(X509_getm_notBefore(x509),0);
        X509_gmtime_adj(X509_getm_notAfter(x509),(long)60*60*24*m_days);
#endif
        X509_set_pubkey(x509, pkey);

        name=X509_get_subject_name(x509);
        /* This function creates and adds the entry, working out the
         * correct string type and performing checks on its length.
         * Normally we'd check the return value for errors...
         */
        X509_NAME_add_entry_by_txt(name,"C",
                    MBSTRING_ASC, (unsigned char*)m_c.get(), -1, -1, 0);
        if(m_s.length() > 0)
        {
            X509_NAME_add_entry_by_txt(name,"S",
                    MBSTRING_ASC, (unsigned char*)m_s.get(), -1, -1, 0);
        }

        if(m_l.length() > 0)
        {
            X509_NAME_add_entry_by_txt(name,"L",
                    MBSTRING_ASC, (unsigned char*)m_l.get(), -1, -1, 0);
        }

        X509_NAME_add_entry_by_txt(name,"O",
                    MBSTRING_ASC, (unsigned char*)m_o.get(), -1, -1, 0);

        if(m_ou.length() > 0)
        {
            X509_NAME_add_entry_by_txt(name,"OU",
                    MBSTRING_ASC, (unsigned char*)m_ou.get(), -1, -1, 0);
        }

        if(m_e.length() > 0)
        {
            X509_NAME_add_entry_by_txt(name,"E",
                    MBSTRING_ASC, (unsigned char*)m_e.get(), -1, -1, 0);
        }

        X509_NAME_add_entry_by_txt(name,"CN",
                    MBSTRING_ASC, (unsigned char*)m_destaddr.get(), -1, -1, 0);

        X509_set_issuer_name(x509,name);

        /* Add extension using V3 code: we can set the config file as NULL
         * because we wont reference any other sections. We can also set
         * the context to NULL because none of these extensions below will need
         * to access it.
         */
        X509_EXTENSION *ex = X509V3_EXT_conf_nid(NULL, NULL, NID_netscape_cert_type, "server");
        if(ex != NULL)
        {
            X509_add_ext(x509, ex, -1);
            X509_EXTENSION_free(ex);
        }

        ex = X509V3_EXT_conf_nid(NULL, NULL, NID_netscape_ssl_server_name,
                                (char*)m_destaddr.get());
        if(ex != NULL)
        {
            X509_add_ext(x509, ex,-1);
            X509_EXTENSION_free(ex);
        }

        if (!X509_sign(x509, pkey, EVP_md5()))
        {
            char errbuf[512];
            ERR_error_string_n(ERR_get_error(), errbuf, 512);
            throw MakeStringException(-1, "X509_sign error %s", errbuf);
        }

        const EVP_CIPHER *enc = EVP_des_ede3_cbc();
        pmem = BIO_new(BIO_s_mem());
        PEM_write_bio_PrivateKey(pmem, pkey, enc, (unsigned char*)m_passphrase.get(), m_passphrase.length(), NULL, NULL);
        readBio(pmem, privkey);

        cmem = BIO_new(BIO_s_mem());
        PEM_write_bio_X509(cmem, x509);
        readBio(cmem, certificate);

        X509_free(x509);
        EVP_PKEY_free(pkey);
        RSA_free(rsa);

#ifndef OPENSSL_NO_CRYPTO_MDEBUG
        CRYPTO_mem_leaks(bio_err);
#endif
        BIO_free(bio_err);
        BIO_free(pmem);
        BIO_free(cmem);

        return 0;
    }

    virtual int generate(StringBuffer& certificate, const char* privkey)
    {
        if(m_destaddr.length() == 0)
            throw MakeStringException(-1, "Common Name (server's hostname or IP address) not set for certificate");
        if(m_passphrase.length() == 0)
            throw MakeStringException(-1, "passphrase not set.");
        if(m_days <= 0)
            throw MakeStringException(-1, "The number of days should be a positive integer");
        
        if(m_c.length() == 0)
            m_c.set("US");

        if(m_o.length() == 0)
            m_o.set("Customer Of Seisint");

        BIO *bio_err;
        BIO *pmem, *cmem;
        X509 *x509=NULL;
        EVP_PKEY *pkey=NULL;

        bio_err=BIO_new_fp(stderr, BIO_NOCLOSE);

# if OPENSSL_API_COMPAT < 0x10100000L
        OpenSSL_add_all_algorithms ();
        ERR_load_crypto_strings ();
#endif
        pmem = BIO_new(BIO_s_mem());
        BIO_puts(pmem, privkey);
        if (!(pkey = PEM_read_bio_PrivateKey (pmem, NULL, NULL, (void*)m_passphrase.get())))
            throw MakeStringException(-1, "Error reading private key");

        if ((x509=X509_new()) == NULL)
            throw MakeStringException(-1, "can't create X509 structure");

        X509_NAME *name=NULL;
        X509_set_version(x509,3);
        ASN1_INTEGER_set(X509_get_serialNumber(x509), 0); // serial number set to 0
#if OPENSSL_VERSION_NUMBER < 0x10100000L
        X509_gmtime_adj(X509_get_notBefore(x509),0);
        X509_gmtime_adj(X509_get_notAfter(x509),(long)60*60*24*m_days);
#else
        X509_gmtime_adj(X509_getm_notBefore(x509),0);
        X509_gmtime_adj(X509_getm_notAfter(x509),(long)60*60*24*m_days);
#endif
        X509_set_pubkey(x509, pkey);

        name=X509_get_subject_name(x509);
        /* This function creates and adds the entry, working out the
         * correct string type and performing checks on its length.
         * Normally we'd check the return value for errors...
         */
        X509_NAME_add_entry_by_txt(name,"C",
                    MBSTRING_ASC, (unsigned char*)m_c.get(), -1, -1, 0);
        if(m_s.length() > 0)
        {
            X509_NAME_add_entry_by_txt(name,"S",
                    MBSTRING_ASC, (unsigned char*)m_s.get(), -1, -1, 0);
        }

        if(m_l.length() > 0)
        {
            X509_NAME_add_entry_by_txt(name,"L",
                    MBSTRING_ASC, (unsigned char*)m_l.get(), -1, -1, 0);
        }

        X509_NAME_add_entry_by_txt(name,"O",
                    MBSTRING_ASC, (unsigned char*)m_o.get(), -1, -1, 0);

        if(m_ou.length() > 0)
        {
            X509_NAME_add_entry_by_txt(name,"OU",
                    MBSTRING_ASC, (unsigned char*)m_ou.get(), -1, -1, 0);
        }

        if(m_e.length() > 0)
        {
            X509_NAME_add_entry_by_txt(name,"E",
                    MBSTRING_ASC, (unsigned char*)m_e.get(), -1, -1, 0);
        }

        X509_NAME_add_entry_by_txt(name,"CN",
                    MBSTRING_ASC, (unsigned char*)m_destaddr.get(), -1, -1, 0);

        X509_set_issuer_name(x509,name);

        /* Add extension using V3 code: we can set the config file as NULL
         * because we wont reference any other sections. We can also set
         * the context to NULL because none of these extensions below will need
         * to access it.
         */
        X509_EXTENSION *ex = X509V3_EXT_conf_nid(NULL, NULL, NID_netscape_cert_type, "server");
        if(ex != NULL)
        {
            X509_add_ext(x509, ex, -1);
            X509_EXTENSION_free(ex);
        }

        ex = X509V3_EXT_conf_nid(NULL, NULL, NID_netscape_ssl_server_name,
                                (char*)m_destaddr.get());
        if(ex != NULL)
        {
            X509_add_ext(x509, ex,-1);
            X509_EXTENSION_free(ex);
        }

        if (!X509_sign(x509, pkey, EVP_md5()))
        {
            char errbuf[512];
            ERR_error_string_n(ERR_get_error(), errbuf, 512);
            throw MakeStringException(-1, "X509_sign error %s", errbuf);
        }

        cmem = BIO_new(BIO_s_mem());
        PEM_write_bio_X509(cmem, x509);
        readBio(cmem, certificate);

        X509_free(x509);
        EVP_PKEY_free(pkey);

#ifndef OPENSSL_NO_CRYPTO_MDEBUG
        CRYPTO_mem_leaks(bio_err);
#endif
        BIO_free(bio_err);
        BIO_free(pmem);
        BIO_free(cmem);

        return 0;
    }

    virtual int generateCSR(StringBuffer& privkey, StringBuffer& csr)
    {
        StringBuffer mycert;
        generate(mycert, privkey);
        return generateCSR(privkey.str(), csr);
    }

    virtual int generateCSR(const char* privkey, StringBuffer& csr)
    {
        if(m_destaddr.length() == 0)
            throw MakeStringException(-1, "Common Name (server's hostname or IP address) not set for certificate");
        if(m_passphrase.length() == 0)
            throw MakeStringException(-1, "passphrase not set.");
        
        if(m_c.length() == 0)
            m_c.set("US");

        if(m_o.length() == 0)
            m_o.set("Customer Of Seisint");

        BIO *bio_err;
        X509_REQ *req;
        X509_NAME *subj;
        EVP_PKEY *pkey = NULL;
        const EVP_MD *digest;
        BIO *pmem;

        bio_err=BIO_new_fp(stderr, BIO_NOCLOSE);

# if OPENSSL_API_COMPAT < 0x10100000L
        OpenSSL_add_all_algorithms ();
        ERR_load_crypto_strings ();
#endif

        pmem = BIO_new(BIO_s_mem());
        BIO_puts(pmem, privkey);

        if (!(pkey = PEM_read_bio_PrivateKey (pmem, NULL, NULL, (void*)m_passphrase.get())))
            throw MakeStringException(-1, "Error reading private key");

        /* create a new request and add the key to it */
        if (!(req = X509_REQ_new ()))
            throw MakeStringException(-1, "Failed to create X509_REQ object");

        X509_REQ_set_version(req,0L);

        X509_REQ_set_pubkey (req, pkey);

        if (!(subj = X509_NAME_new ()))
            throw MakeStringException(-1, "Failed to create X509_NAME object");

        addNameEntry(subj, "countryName", m_c.get());

        if(m_s.length() > 0)
            addNameEntry(subj, "stateOrProvinceName", m_s.get());

        if(m_l.length() > 0)
            addNameEntry(subj, "localityName", m_l.get());

        addNameEntry(subj, "organizationName", m_o.get());

        if(m_ou.length() > 0)
            addNameEntry(subj, "organizationalUnitName", m_ou.get());

        if(m_e.length() > 0)
            addNameEntry(subj, "emailAddress", m_e.get());

        addNameEntry(subj, "commonName", m_destaddr.get());

        if (X509_REQ_set_subject_name (req, subj) != 1)
            throw MakeStringException(-1, "Error adding subject to request");

        /* pick the correct digest and sign the request */
#if OPENSSL_VERSION_NUMBER < 0x10100000L
        auto type = EVP_PKEY_type(pkey->type);
#else
        auto type = EVP_PKEY_base_id(pkey);
#endif
        if (type == EVP_PKEY_DSA)
#if OPENSSL_VERSION_NUMBER < 0x10100000L
            digest = EVP_dss1 ();
#else
            throw MakeStringException(-1, "Error checking public key for a valid digest (DSA not supported by openSSL 1.1)");
#endif
        else if (type == EVP_PKEY_RSA)
            digest = EVP_sha1 ();
        else
            throw MakeStringException(-1, "Error checking public key for a valid digest");

        if (!(X509_REQ_sign (req, pkey, digest)))
            throw MakeStringException(-1, "Error signing request");

        /* write the completed request */
        BIO* reqmem = BIO_new(BIO_s_mem());
        if (PEM_write_bio_X509_REQ(reqmem, req) != 1)
            throw MakeStringException(-1, "Error while writing request");

        readBio(reqmem, csr);

#ifndef OPENSSL_NO_CRYPTO_MDEBUG
        CRYPTO_mem_leaks(bio_err);
#endif
        BIO_free(pmem);
        BIO_free(reqmem);
        EVP_PKEY_free (pkey);
        X509_REQ_free (req);
        return 0;   
    }
};

} // namespace securesocket

extern "C" {

SECURESOCKET_API ISecureSocketContext* createSecureSocketContext(SecureSocketType sockettype)
{
    return new securesocket::CSecureSocketContext((ISyncedPropertyTree *)nullptr, sockettype);
}

SECURESOCKET_API ISecureSocketContext* createSecureSocketContextSynced(const ISyncedPropertyTree * config, SecureSocketType sockettype)
{
    return new securesocket::CSecureSocketContext(config, sockettype);
}


SECURESOCKET_API ISecureSocketContext* createSecureSocketContextSecret(const char *issuer, SecureSocketType sockettype)
{
    Owned<const ISyncedPropertyTree> info = getIssuerTlsSyncedConfig(issuer);
    return createSecureSocketContextSynced(info, sockettype);
}


SECURESOCKET_API ISecureSocketContext* createSecureSocketContextSecretSrv(const char *issuer, const char *optTrustedPeers, bool requireMtlsFlag)
{
    if (requireMtlsFlag && !queryMtls())
        throw makeStringException(-100, "MTLS secure context required but not configured");

    Owned<const ISyncedPropertyTree> info = getIssuerTlsSyncedConfig(issuer, optTrustedPeers, false);

    if (!info->isValid())
        throw makeStringExceptionV(-101, "TLS issuer %s secure context requested but not configured (2)", issuer);

    return createSecureSocketContextSynced(info, ServerSocket);
}

IPropertyTree * createSecureSocketConfig(const char* certFileOrBuf, const char* privKeyFileOrBuf, const char* passphrase, bool createIfAllNull)
{
    if (!createIfAllNull)
    {
        if (!certFileOrBuf && !privKeyFileOrBuf && !passphrase)
            return nullptr;
    }

    Owned<IPropertyTree> config = createPTree("ssl");
    if (certFileOrBuf)
        config->setProp("certificate", certFileOrBuf);
    if (privKeyFileOrBuf)
        config->setProp("privatekey", privKeyFileOrBuf);
    if (passphrase)
        config->setProp("passphrase", passphrase);
    return config.getClear();
}

SECURESOCKET_API ISecureSocketContext* createSecureSocketContextSSF(ISmartSocketFactory* ssf)
{
    if (ssf == nullptr)
        return createSecureSocketContext(ClientSocket);

    return createSecureSocketContextSynced(ssf->queryTlsConfig(), ClientSocket);
}

//Legacy factory interfaces
SECURESOCKET_API ISecureSocketContext* createSecureSocketContextEx2(const IPropertyTree* config, SecureSocketType sockettype)
{
    return new securesocket::CSecureSocketContext(config, sockettype);
}

SECURESOCKET_API ICertificate *createCertificate()
{
    return new securesocket::CRsaCertificate();
}

SECURESOCKET_API int signCertificate(const char* csr, const char* ca_certificate, const char* ca_privkey, const char* ca_passphrase, int days, StringBuffer& certificate)
{
    EVP_PKEY *pkey, *CApkey;
    const EVP_MD *digest;
    X509 *cert, *CAcert;
    X509_REQ *req;
    X509_NAME *name;

    if(days <= 0)
        days = 365;

# if OPENSSL_API_COMPAT < 0x10100000L
    OpenSSL_add_all_algorithms ();
    ERR_load_crypto_strings ();
#endif
    
    BIO *bio_err;
    bio_err=BIO_new_fp(stderr, BIO_NOCLOSE);

    // Read in and verify signature on the request
    BIO *csrmem = BIO_new(BIO_s_mem());
    BIO_puts(csrmem, csr);
    if (!(req = PEM_read_bio_X509_REQ(csrmem, NULL, NULL, NULL)))
        throw MakeStringException(-1, "Error reading request from buffer");
    if (!(pkey = X509_REQ_get_pubkey(req)))
        throw MakeStringException(-1, "Error getting public key from request");
    if (X509_REQ_verify (req, pkey) != 1)
        throw MakeStringException(-1, "Error verifying signature on certificate");

    // read in the CA certificate and private key
    BIO *cacertmem = BIO_new(BIO_s_mem());
    BIO_puts(cacertmem, ca_certificate);
    if (!(CAcert = PEM_read_bio_X509(cacertmem, NULL, NULL, NULL)))
        throw MakeStringException(-1, "Error reading CA's certificate from buffer");
    BIO *capkeymem = BIO_new(BIO_s_mem());
    BIO_puts(capkeymem, ca_privkey);
    if (!(CApkey = PEM_read_bio_PrivateKey (capkeymem, NULL, NULL, (void*)ca_passphrase)))
        throw MakeStringException(-1, "Error reading CA private key");

    cert = X509_new();
    X509_set_version(cert,3);
    ASN1_INTEGER_set(X509_get_serialNumber(cert), 0); // serial number set to 0

    // set issuer and subject name of the cert from the req and the CA
    name = X509_REQ_get_subject_name (req);
    X509_set_subject_name (cert, name);
    name = X509_get_subject_name (CAcert);
    X509_set_issuer_name (cert, name);

    //set public key in the certificate
    X509_set_pubkey (cert, pkey);
    // set duration for the certificate
#if OPENSSL_VERSION_NUMBER < 0x10100000L
    X509_gmtime_adj (X509_get_notBefore (cert), 0);
    X509_gmtime_adj (X509_get_notAfter (cert), days*24*60*60);
#else
    X509_gmtime_adj (X509_getm_notBefore (cert), 0);
    X509_gmtime_adj (X509_getm_notAfter (cert), days*24*60*60);
#endif

    // sign the certificate with the CA private key
#if OPENSSL_VERSION_NUMBER < 0x10100000L
    auto type = EVP_PKEY_type(CApkey->type);
#else
    auto type = EVP_PKEY_base_id(CApkey);
#endif
    if (type == EVP_PKEY_DSA)
#if OPENSSL_VERSION_NUMBER < 0x10100000L
        digest = EVP_dss1 ();
#else
        throw MakeStringException(-1, "Error checking public key for a valid digest (DSA not supported by openSSL 1.1)");
#endif
    else if (type == EVP_PKEY_RSA)
        digest = EVP_sha1 ();
    else
        throw MakeStringException(-1, "Error checking public key for a valid digest");
    
    if (!(X509_sign (cert, CApkey, digest)))
        throw MakeStringException(-1, "Error signing certificate");

    // write the completed certificate
    BIO* cmem = BIO_new(BIO_s_mem());
    PEM_write_bio_X509(cmem, cert);
    readBio(cmem, certificate);

#ifndef OPENSSL_NO_CRYPTO_MDEBUG
    CRYPTO_mem_leaks(bio_err);
#endif
    BIO_free(csrmem);
    BIO_free(cacertmem);
    BIO_free(cmem);
    EVP_PKEY_free(pkey);
    X509_REQ_free(req);
    return 0;
}

} // extern C

class CSecureSmartSocketFactory : public CSmartSocketFactory
{
public:
    Owned<ISecureSocketContext> secureContext;

    CSecureSmartSocketFactory(const char *_socklist, IPropertyTree *_tlsConfig, bool _retry, unsigned _retryInterval, unsigned _dnsInterval) : CSmartSocketFactory(_socklist, _tlsConfig, _retry, _retryInterval, _dnsInterval)
    {
        secureContext.setown(createSecureSocketContext(ClientSocket));
    }

    CSecureSmartSocketFactory(IPropertyTree &service, bool _retry, unsigned _retryInterval, unsigned _dnsInterval) : CSmartSocketFactory(service, _retry, _retryInterval, _dnsInterval)
    {
        secureContext.setown(createSecureSocketContextSynced(queryTlsConfig(), ClientSocket));
    }

    virtual ISmartSocket *connect_timeout(unsigned timeoutms) override
    {
        SocketEndpoint ep;
        SmartSocketEndpoint *ss = nullptr;
        Owned<ISecureSocket> ssock;
        Owned<ISocket> sock = connect_sock(timeoutms, ss, ep);
        try
        {
            ssock.setown(secureContext->createSecureSocket(sock.getClear()));
            // secure_connect may also DBGLOG() errors ...
            int res = ssock->secure_connect();
            if (res < 0)
                throw MakeStringException(-1, "connect_timeout : Failed to establish secure connection");
        }
        catch (IException *)
        {
            ss->status = false;
            throw;
        }
        return new CSmartSocket(ssock.getClear(), ep, this);
    }
};

ISmartSocketFactory *createSecureSmartSocketFactory(const char *_socklist, IPropertyTree* _tlsConfig, bool _retry, unsigned _retryInterval, unsigned _dnsInterval)
{
    return new CSecureSmartSocketFactory(_socklist, _tlsConfig, _retry, _retryInterval, _dnsInterval);
}

ISmartSocketFactory *createSecureSmartSocketFactory(IPropertyTree &service, bool _retry, unsigned _retryInterval, unsigned _dnsInterval)
{
    return new CSecureSmartSocketFactory(service, _retry, _retryInterval, _dnsInterval);
}

class CSingletonSecureSocketConnection: public CSingletonSocketConnection
{
public:
    Owned<ISecureSocketContext> secureContextClient;
    Owned<ISecureSocketContext> secureContextServer;
    int tlsLogLevel;

    CSingletonSecureSocketConnection(SocketEndpoint &_ep)
    {
        ep = _ep;
        state = Snone;
        cancelling = false;
        secureContextClient.setown(createSecureSocketContextSecret("local", ClientSocket));
        secureContextServer.setown(createSecureSocketContextSecretSrv("local", nullptr, true));
#ifdef _CONTAINERIZED
        tlsLogLevel = getComponentConfigSP()->getPropInt("logging/@detail", SSLogMin);
        if (tlsLogLevel >= ExtraneousMsgThreshold) // or InfoMsgThreshold ?
            tlsLogLevel = SSLogMax;
#else
        tlsLogLevel = SSLogMin;
#endif
    }

    virtual ~CSingletonSecureSocketConnection()
    {
        shutdownAndCloseNoThrow(sock);
    }

    bool connect(unsigned timeoutms) override
    {
        bool srtn = CSingletonSocketConnection::connect(timeoutms);
        if (srtn)
        {
            Owned<ISecureSocket> ssock = secureContextClient->createSecureSocket(sock.getClear(), tlsLogLevel);
            int status = ssock->secure_connect(tlsLogLevel);
            if (status < 0)
            {
                ssock->close();
                return false;
            }
            else
            {
                sock.setown(ssock.getClear());
                return true;
            }
        }
        return srtn;
    }

    bool accept(unsigned timeoutms) override
    {
        bool srtn = CSingletonSocketConnection::accept(timeoutms);
        if (srtn)
        {
            Owned<ISecureSocket> ssock = secureContextServer->createSecureSocket(sock.getClear(), tlsLogLevel);
            int status = ssock->secure_accept(tlsLogLevel);
            if (status < 0)
            {
                ssock->close();
                return false;
            }
            else
            {
                sock.setown(ssock.getClear());
                return true;
            }
        }
        return srtn;
    }

};

IConversation *createSingletonSecureSocketConnection(unsigned short port,SocketEndpoint *_ep)
{
    SocketEndpoint ep;
    if (_ep)
        ep = *_ep;
    if (port)
        ep.port = port;
    return new CSingletonSecureSocketConnection(ep);
}
