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
#include <signal.h>  
#else
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stddef.h>
#include <errno.h>
#endif

//openssl
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

#include "jsmartsock.ipp"
#include "securesocket.hpp"

Owned<ISecureSocketContext> server_securesocket_context;
bool accept_selfsigned = false;

#define CHK_NULL(x) if((x)==NULL) exit(1)
#define CHK_ERR(err, s) if((err)==-1){perror(s);exit(1);}
#define CHK_SSL(err) if((err) ==-1){ERR_print_errors_fp(stderr); exit(2);}

#define THROWSECURESOCKETEXCEPTION(err) \
    throw MakeStringException(-1, "SecureSocket Exception Raised in: %s, line %d - %s", sanitizeSourceFile(__FILE__), __LINE__, err);


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
    SSL*        m_ssl;
    Owned<ISocket> m_socket;
    bool        m_verify;
    bool        m_address_match;
    CStringSet* m_peers;
    int         m_loglevel;
    bool        m_isSecure;
private:
    StringBuffer& get_cn(X509* cert, StringBuffer& cn);
    bool verify_cert(X509* cert);

public:
    IMPLEMENT_IINTERFACE;

    CSecureSocket(ISocket* sock, SSL_CTX* ctx, bool verify = false, bool addres_match = false, CStringSet* m_peers = NULL, int loglevel=SSLogNormal);
    CSecureSocket(int sockfd, SSL_CTX* ctx, bool verify = false, bool addres_match = false, CStringSet* m_peers = NULL, int loglevel=SSLogNormal);
    ~CSecureSocket();

    virtual int secure_accept(int logLevel);
    virtual int secure_connect(int logLevel);

    virtual int logPollError(unsigned revents, const char *rwstr);
    virtual int wait_read(unsigned timeoutms);
    virtual void read(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read,unsigned timeoutsecs);
    virtual void readtms(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read, unsigned timeoutms);
    virtual size32_t write(void const* buf, size32_t size);
    virtual size32_t writetms(void const* buf, size32_t size, unsigned timeoutms=WAIT_FOREVER);

    void readTimeout(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read, unsigned timeout, bool useSeconds);

    //The following are the functions from ISocket that haven't been implemented.


    virtual void   read(void* buf, size32_t size)
    {
        size32_t size_read;
        // MCK - this was:
        // readTimeout(buf, size, size, size_read, 0, false);
        // but that is essentially a non-blocking read() and we want a blocking read() ...
        // read() is always expecting size bytes so min_size should be size
        readTimeout(buf, size, size, size_read, WAIT_FOREVER, false);
    }

    virtual size32_t get_max_send_size()
    {
        throw MakeStringException(-1, "CSecureSocket::get_max_send_size: not implemented");
    }

    //
    // This method is called by server to accept client connection
    //
    virtual ISocket* accept(bool allowcancel=false) // not needed for UDP
    {
        throw MakeStringException(-1, "CSecureSocket::accept: not implemented");
    }

    //
    // This method is called to check whether a socket is ready to write (i.e. some free buffer space)
    // 
    virtual int wait_write(unsigned timeout)
    {
        throw MakeStringException(-1, "CSecureSocket::wait_write: not implemented");
    }

    //
    // can be used with write to allow it to return if it would block
    // be sure and restore to old state before calling other functions on this socket
    //
    virtual bool set_nonblock(bool on) // returns old state
    {
        throw MakeStringException(-1, "CSecureSocket::set_nonblock: not implemented");
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
        throw MakeStringException(-1, "CSecureSocket::cancel_accept: not implemented");
    }

    //
    // Shutdown socket: prohibit write and read operations on socket
    //
    virtual void  shutdown(unsigned mode) // not needed for UDP
    {
        m_socket->shutdown(mode);
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

    virtual void  set_block_mode (             // must be called before block operations
                            unsigned flags,    // BF_* flags (must match receive_block)
                          size32_t recsize=0,  // record size (required for rec compression)
                            unsigned timeout=0 // timeout in msecs (0 for no timeout)
                  ) 
    {
        throw MakeStringException(-1, "CSecureSocket::set_block_mode: not implemented");
    }



    virtual bool  send_block( 
                            const void *blk,   // data to send 
                            size32_t sz          // size to send (0 for eof)
                  )
    {
        throw MakeStringException(-1, "CSecureSocket::send_block: not implemented");
    }

    virtual size32_t receive_block_size ()     // get size of next block (always must call receive_block after) 
    {
        throw MakeStringException(-1, "CSecureSocket::receive_block_size: not implemented");
    }

    virtual size32_t receive_block(
                            void *blk,         // receive pointer 
                            size32_t sz          // max size to read (0 for sync eof) 
                                               // if less than block size truncates block
                  )
    {
        throw MakeStringException(-1, "CSecureSocket::receive_block: not implemented");
    }

    virtual void  close()
    {
        m_socket->close();  
    }

    virtual unsigned OShandle()              // for internal use
    {
        return m_socket->OShandle();
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
        }
        return 0;
    }

    virtual size32_t write_multiple(unsigned num,const void **buf, size32_t *size)
    {
        throw MakeStringException(-1, "CSecureSocket::write_multiple: not implemented");
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
        throw MakeStringException(-1, "CSecureSocket::set_keep_alive: not implemented");
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


/**************************************************************************
 *  CSecureSocket -- secure socket layer implementation using openssl     *
 **************************************************************************/
CSecureSocket::CSecureSocket(ISocket* sock, SSL_CTX* ctx, bool verify, bool address_match, CStringSet* peers, int loglevel)
{
    m_socket.setown(sock);
    m_ssl = SSL_new(ctx);

    m_verify = verify;
    m_address_match = address_match;
    m_peers = peers;;
    m_loglevel = loglevel;
    m_isSecure = false;

    if(m_ssl == NULL)
    {
        throw MakeStringException(-1, "Can't create ssl");
    }
    SSL_set_fd(m_ssl, sock->OShandle());
}

CSecureSocket::CSecureSocket(int sockfd, SSL_CTX* ctx, bool verify, bool address_match, CStringSet* peers, int loglevel)
{
    //m_socket.setown(sock);
    //m_socket.setown(ISocket::attach(sockfd));
    m_ssl = SSL_new(ctx);

    m_verify = verify;
    m_address_match = address_match;
    m_peers = peers;;
    m_loglevel = loglevel;
    m_isSecure = false;

    if(m_ssl == NULL)
    {
        throw MakeStringException(-1, "Can't create ssl");
    }
    SSL_set_fd(m_ssl, sockfd);
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
    DBGLOG ("peer's certificate:\n");

    char *s, oneline[1024];

    s = X509_NAME_oneline (X509_get_subject_name (cert), oneline, 1024);
    if(s != NULL)
    {
        DBGLOG ("\t subject: %s", oneline);
    }

    s = X509_NAME_oneline (X509_get_issuer_name  (cert), oneline, 1024);
    if(s != NULL)
    {
        DBGLOG ("\t issuer: %s", oneline);
    }

    StringBuffer cn;
    get_cn(cert, cn);

    if(cn.length() == 0)
        throw MakeStringException(-1, "cn of the certificate can't be found");

    if(m_address_match)
    {
        SocketEndpoint ep;
        m_socket->getPeerEndpoint(ep);
        StringBuffer iptxt;
        ep.getIpText(iptxt);
        SocketEndpoint cnep(cn.str());
        StringBuffer cniptxt;
        cnep.getIpText(cniptxt);
        DBGLOG("peer ip=%s, certificate ip=%s", iptxt.str(), cniptxt.str());
        if(!(cniptxt.length() > 0 && stricmp(iptxt.str(), cniptxt.str()) == 0))
        {
            DBGLOG("Source address of the request doesn't match the certificate");
            return false;
        }
    }
    
    if (m_peers->contains("anyone") || m_peers->contains(cn.str()))
    {
        DBGLOG("%s among trusted peers", cn.str());
        return true;
    }
    else
    {
        DBGLOG("%s not among trusted peers, verification failed", cn.str());
        return false;
    }
}

int CSecureSocket::secure_accept(int logLevel)
{
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
        if ( (logLevel >= 5) || (ret != SSL_ERROR_SYSCALL) )
        {
            char errbuf[512];
            ERR_error_string_n(ERR_get_error(), errbuf, 512);
            DBGLOG("SSL_accept returned 0, error - %s", errbuf);
        }
        return -1;
    }
    else if(err < 0)
    {
        int ret = SSL_get_error(m_ssl, err);
        char errbuf[512];
        ERR_error_string_n(ERR_get_error(), errbuf, 512);
        errbuf[511] = '\0';
        DBGLOG("SSL_accept returned %d, SSL_get_error=%d, error - %s", err, ret, errbuf);
        if(strstr(errbuf, "error:1408F455:") != NULL)
        {
            DBGLOG("Unrecoverable SSL library error.");
            _exit(0);
        }
        return err;
    }

    if (logLevel)
        DBGLOG("SSL connection using %s", SSL_get_cipher(m_ssl));

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
    int err = SSL_connect (m_ssl);                     
    if(err <= 0)
    {
        int ret = SSL_get_error(m_ssl, err);
        char errbuf[512];
        ERR_error_string_n(ERR_get_error(), errbuf, 512);
        DBGLOG("SSL_connect error - %s, SSL_get_error=%d, error - %d", errbuf,ret, err);
        throw MakeStringException(-1, "SSL_connect failed: %s", errbuf);
    }
    

    // Currently only do fake verify - simply logging the subject and issuer
    // The verify parameter makes it possible for the application to verify only
    // once per session and cache the result of the verification.
    if(m_verify)
    {
        // Following two steps are optional and not required for
        // data exchange to be successful.
        
        // Get the cipher - opt
        if (logLevel)
            DBGLOG("SSL connection using %s\n", SSL_get_cipher (m_ssl));

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

inline unsigned socketTime(bool useSeconds)
{
    if (useSeconds)
    {
        time_t timenow;
        return (unsigned) time(&timenow);
    }
    return msTick();
}

inline unsigned socketTimeRemaining(bool useSeconds, unsigned start, unsigned timeout)
{
    unsigned elapsed = socketTime(useSeconds) - start;
    if (elapsed < timeout)
    {
        unsigned timeleft = timeout - elapsed;
        if (useSeconds)
            timeleft *= 1000;
        return timeleft;
    }
    return 0;
}

void CSecureSocket::readTimeout(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read, unsigned timeout, bool useSeconds)
{
    size_read = 0;
    unsigned start;
    unsigned timeleft;

    if (timeout != WAIT_FOREVER) {
        start = socketTime(useSeconds);
        timeleft = timeout;
        if (useSeconds)
            timeleft *= 1000;
    }

    do {
        int rc;
        if (timeout != WAIT_FOREVER) {
            rc = wait_read(timeleft);
            if (rc < 0) {
                THROWSECURESOCKETEXCEPTION("wait_read error"); 
            }
            if (rc == 0) {
                THROWSECURESOCKETEXCEPTION("timeout expired"); 
            }
            timeleft = socketTimeRemaining(useSeconds, start, timeout);
        }

        rc = SSL_read(m_ssl, (char*)buf + size_read, max_size - size_read);
        if(rc > 0)
        {
            size_read += rc;
        }
        else
        {
            int err = SSL_get_error(m_ssl, rc);
            // Ignoring SSL_ERROR_SYSCALL because IE prompting user acceptance of the certificate 
            // causes this error, but is harmless.
            // Ignoring SSL_ERROR_SYSCALL also seems to ignore the timeout value being exceeded,
            //    but for persistence at least, treating zero bytes read can be treated as a graceful completion of connection
            if((err != SSL_ERROR_NONE) && (err != SSL_ERROR_SYSCALL))
            {
                if(m_loglevel >= SSLogMax)
                {
                    char errbuf[512];
                    ERR_error_string_n(ERR_get_error(), errbuf, 512);
                    DBGLOG("Warning: SSL_read error %d - %s", err, errbuf);
                }
            }
            break;
        }
    } while (size_read < min_size);
}


void CSecureSocket::readtms(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read, unsigned timeoutms)
{
    readTimeout(buf, min_size, max_size, size_read, timeoutms, false);
}

void CSecureSocket::read(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read,unsigned timeoutsecs)
{
    readTimeout(buf, min_size, max_size, size_read, timeoutsecs, true);
}

size32_t CSecureSocket::write(void const* buf, size32_t size)
{
    int numwritten = SSL_write(m_ssl, buf, size);
    return numwritten;
}

size32_t CSecureSocket::writetms(void const* buf, size32_t size, unsigned timeoutms)
{
    // timeoutms not implemented yet ...
    int numwritten = SSL_write(m_ssl, buf, size);
    return numwritten;
}

int verify_callback(int ok, X509_STORE_CTX *store)
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
            OERRLOG("Error with certificate: issuer=%s,subject=%s,err %d - %s", issuer, subject,err,X509_verify_cert_error_string(err));
    }
    return ok;
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

class CSecureSocketContext : implements ISecureSocketContext, public CInterface
{
private:
    SSL_CTX*    m_ctx;
#if (OPENSSL_VERSION_NUMBER > 0x00909000L) 
    const SSL_METHOD* m_meth;
#else
    SSL_METHOD* m_meth;
#endif 

    bool m_verify;
    bool m_address_match;
    Owned<CStringSet> m_peers;
    StringAttr password;

    void setSessionIdContext()
    {
        SSL_CTX_set_session_id_context(m_ctx, (const unsigned char*)"hpccsystems", 11);
    }

public:
    IMPLEMENT_IINTERFACE;
    CSecureSocketContext(SecureSocketType sockettype)
    {
        m_verify = false;
        m_address_match = false;

        if(sockettype == ClientSocket)
            m_meth = SSLv23_client_method();
        else
            m_meth = SSLv23_server_method();

        m_ctx = SSL_CTX_new(m_meth);

        if(!m_ctx)
        {
            throw MakeStringException(-1, "ctx can't be created");
        }

        if (sockettype == ServerSocket)
            setSessionIdContext();

        SSL_CTX_set_mode(m_ctx, SSL_CTX_get_mode(m_ctx) | SSL_MODE_AUTO_RETRY);
    }

    CSecureSocketContext(const char* certfile, const char* privkeyfile, const char* passphrase, SecureSocketType sockettype)
    {
        m_verify = false;
        m_address_match = false;

        if(sockettype == ClientSocket)
            m_meth = SSLv23_client_method();
        else
            m_meth = SSLv23_server_method();

        m_ctx = SSL_CTX_new(m_meth);

        if(!m_ctx)
        {
            throw MakeStringException(-1, "ctx can't be created");
        }

        if (sockettype == ServerSocket)
            setSessionIdContext();

        password.set(passphrase);
        SSL_CTX_set_default_passwd_cb_userdata(m_ctx, (void*)password.str());
        SSL_CTX_set_default_passwd_cb(m_ctx, pem_passwd_cb);

        if(SSL_CTX_use_certificate_file(m_ctx, certfile, SSL_FILETYPE_PEM) <= 0)
        {
            char errbuf[512];
            ERR_error_string_n(ERR_get_error(), errbuf, 512);
            throw MakeStringException(-1, "error loading certificate file %s - %s", certfile, errbuf);
        }

        if(SSL_CTX_use_PrivateKey_file(m_ctx, privkeyfile, SSL_FILETYPE_PEM) <= 0)
        {
            char errbuf[512];
            ERR_error_string_n(ERR_get_error(), errbuf, 512);
            throw MakeStringException(-1, "error loading private key file %s - %s", privkeyfile, errbuf);
        }

        if(!SSL_CTX_check_private_key(m_ctx))
        {
            throw MakeStringException(-1, "Private key does not match the certificate public key");
        }
        
        SSL_CTX_set_mode(m_ctx, SSL_CTX_get_mode(m_ctx) | SSL_MODE_AUTO_RETRY);
    }

    CSecureSocketContext(IPropertyTree* config, SecureSocketType sockettype)
    {
        assertex(config);
        m_verify = false;
        m_address_match = false;

        if(sockettype == ClientSocket)
            m_meth = SSLv23_client_method();
        else
            m_meth = SSLv23_server_method();

        m_ctx = SSL_CTX_new(m_meth);

        if(!m_ctx)
        {
            throw MakeStringException(-1, "ctx can't be created");
        }

        if (sockettype == ServerSocket)
            setSessionIdContext();

        const char *cipherList = config->queryProp("cipherList");
        if (!cipherList || !*cipherList)
            cipherList = "ECDH+AESGCM:DH+AESGCM:ECDH+AES256:DH+AES256:ECDH+AES128:DH+AES:ECDH+3DES:DH+3DES:RSA+AESGCM:RSA+AES:RSA+3DES:!aNULL:!MD5";
        SSL_CTX_set_cipher_list(m_ctx, cipherList);

        const char* passphrase = config->queryProp("passphrase");
        if(passphrase && *passphrase)
        {
            StringBuffer pwd;
            decrypt(pwd, passphrase);
            password.set(pwd);
            SSL_CTX_set_default_passwd_cb_userdata(m_ctx, (void*)password.str());
            SSL_CTX_set_default_passwd_cb(m_ctx, pem_passwd_cb);
        }

        const char* certfile = config->queryProp("certificate");
        if(certfile && *certfile)
        {
            if(SSL_CTX_use_certificate_file(m_ctx, certfile, SSL_FILETYPE_PEM) <= 0)
            {
                char errbuf[512];
                ERR_error_string_n(ERR_get_error(), errbuf, 512);
                throw MakeStringException(-1, "error loading certificate file %s - %s", certfile, errbuf);
            }
        }

        const char* privkeyfile = config->queryProp("privatekey");
        if(privkeyfile && *privkeyfile)
        {
            if(SSL_CTX_use_PrivateKey_file(m_ctx, privkeyfile, SSL_FILETYPE_PEM) <= 0)
            {
                char errbuf[512];
                ERR_error_string_n(ERR_get_error(), errbuf, 512);
                throw MakeStringException(-1, "error loading private key file %s - %s", privkeyfile, errbuf);
            }
            if(!SSL_CTX_check_private_key(m_ctx))
            {
                throw MakeStringException(-1, "Private key does not match the certificate public key");
            }
        }
    
        SSL_CTX_set_mode(m_ctx, SSL_CTX_get_mode(m_ctx) | SSL_MODE_AUTO_RETRY);

        m_verify = config->getPropBool("verify/@enable");
        m_address_match = config->getPropBool("verify/@address_match");
        accept_selfsigned = config->getPropBool("verify/@accept_selfsigned");

        if(m_verify)
        {
            const char* capath = config->queryProp("verify/ca_certificates/@path");
            if(capath && *capath)
            {
                if(SSL_CTX_load_verify_locations(m_ctx, capath, NULL) != 1)
                {
                    throw MakeStringException(-1, "Error loading CA certificates from %s", capath);
                }
            }

            SSL_CTX_set_verify(m_ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT | SSL_VERIFY_CLIENT_ONCE, verify_callback);

            m_peers.setown(new CStringSet());
            const char* peersstr = config->queryProp("verify/trusted_peers");
            while(peersstr && *peersstr)
            {
                StringBuffer onepeerbuf;
                peersstr = strtok__(peersstr, "|", onepeerbuf);
                if(onepeerbuf.length() == 0)
                    break;

                char*  onepeer = onepeerbuf.detach();
                if (isdigit(*onepeer))
                {
                    char *dash = strrchr(onepeer, '-');
                    if (dash)
                    {
                        *dash = 0;
                        int last = atoi(dash+1);
                        char *dot = strrchr(onepeer, '.');
                        *dot = 0;
                        int first = atoi(dot+1);
                        for (int i = first; i <= last; i++)
                        {
                            StringBuffer t;
                            t.append(onepeer).append('.').append(i);
                            m_peers->add(t.str());
                        }
                    }
                    else
                    {
                        m_peers->add(onepeer);
                    }
                }
                else
                {
                    m_peers->add(onepeer);
                }
                free(onepeer);
            }
        }
    }

    ~CSecureSocketContext()
    {
        SSL_CTX_free(m_ctx);
    }

    ISecureSocket* createSecureSocket(ISocket* sock, int loglevel)
    {
        return new CSecureSocket(sock, m_ctx, m_verify, m_address_match, m_peers, loglevel);
    }

    ISecureSocket* createSecureSocket(int sockfd, int loglevel)
    {
        return new CSecureSocket(sockfd, m_ctx, m_verify, m_address_match, m_peers, loglevel);
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

        CRYPTO_mem_ctrl(CRYPTO_MEM_CHECK_ON);

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
        X509_gmtime_adj(X509_get_notBefore(x509),0);
        X509_gmtime_adj(X509_get_notAfter(x509),(long)60*60*24*m_days);
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

        CRYPTO_mem_ctrl(CRYPTO_MEM_CHECK_ON);
        bio_err=BIO_new_fp(stderr, BIO_NOCLOSE);

        OpenSSL_add_all_algorithms ();
        ERR_load_crypto_strings ();

        pmem = BIO_new(BIO_s_mem());
        BIO_puts(pmem, privkey);
        if (!(pkey = PEM_read_bio_PrivateKey (pmem, NULL, NULL, (void*)m_passphrase.get())))
            throw MakeStringException(-1, "Error reading private key");

        if ((x509=X509_new()) == NULL)
            throw MakeStringException(-1, "can't create X509 structure");

        X509_NAME *name=NULL;
        X509_set_version(x509,3);
        ASN1_INTEGER_set(X509_get_serialNumber(x509), 0); // serial number set to 0
        X509_gmtime_adj(X509_get_notBefore(x509),0);
        X509_gmtime_adj(X509_get_notAfter(x509),(long)60*60*24*m_days);
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

        CRYPTO_mem_ctrl(CRYPTO_MEM_CHECK_ON);
        bio_err=BIO_new_fp(stderr, BIO_NOCLOSE);

        OpenSSL_add_all_algorithms ();
        ERR_load_crypto_strings ();

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

}

extern "C" {
CriticalSection factoryCrit;

SECURESOCKET_API ISecureSocketContext* createSecureSocketContext(SecureSocketType sockettype)
{
    CriticalBlock b(factoryCrit);
    if(sockettype == ClientSocket)
    {
        return new securesocket::CSecureSocketContext(sockettype);
    }
    else
    {
        if(server_securesocket_context.get() == NULL)
            server_securesocket_context.setown(new securesocket::CSecureSocketContext(sockettype));
        return server_securesocket_context.getLink();
    }
}

SECURESOCKET_API ISecureSocketContext* createSecureSocketContextEx(const char* certfile, const char* privkeyfile, const char* passphrase, SecureSocketType sockettype)
{
    CriticalBlock b(factoryCrit);
    if(sockettype == ClientSocket)
    {
        return new securesocket::CSecureSocketContext(certfile, privkeyfile, passphrase, sockettype);
    }
    else
    {
        if(server_securesocket_context.get() == NULL)
            server_securesocket_context.setown(new securesocket::CSecureSocketContext(certfile, privkeyfile, passphrase, sockettype));
        return server_securesocket_context.getLink();
    }
}

SECURESOCKET_API ISecureSocketContext* createSecureSocketContextEx2(IPropertyTree* config, SecureSocketType sockettype)
{
    if(config == NULL)
        return createSecureSocketContext(sockettype);

    CriticalBlock b(factoryCrit);
    if(sockettype == ClientSocket)
    {
        return new securesocket::CSecureSocketContext(config, sockettype);
    }
    else
    {
        if(server_securesocket_context.get() == NULL)
            server_securesocket_context.setown(new securesocket::CSecureSocketContext(config, sockettype));
        return server_securesocket_context.getLink();
    }
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

    OpenSSL_add_all_algorithms ();
    ERR_load_crypto_strings ();
    
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
    X509_gmtime_adj (X509_get_notBefore (cert), 0);
    X509_gmtime_adj (X509_get_notAfter (cert), days*24*60*60);

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

}

class CSecureSmartSocketFactory : public CSmartSocketFactory
{
public:
    Owned<ISecureSocketContext> secureContext;

    CSecureSmartSocketFactory(const char *_socklist, bool _retry, unsigned _retryInterval, unsigned _dnsInterval) : CSmartSocketFactory(_socklist, _retry, _retryInterval, _dnsInterval)
    {
        secureContext.setown(createSecureSocketContext(ClientSocket));
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

ISmartSocketFactory *createSecureSmartSocketFactory(const char *_socklist, bool _retry, unsigned _retryInterval, unsigned _dnsInterval)
{
    return new CSecureSmartSocketFactory(_socklist, _retry, _retryInterval, _dnsInterval);
}
