/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#ifndef _ZCRYPT_IPP__
#define _ZCRYPT_IPP__

#include "zcrypt.hpp"
#include "mutex.ipp"

#include <zlib.h>
#include <zconf.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <stddef.h>
#include <errno.h>
#endif

#include <openssl/rsa.h>
#include <openssl/crypto.h>
#include <openssl/rand.h>

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

typedef struct linkedlist_filetozip_s
{
    struct linkedlist_filetozip_s* next_filetozip;
    char*   file_name;       
    void*   file_content;   
    time_t  file_time;    
    uLong   content_length;
} linkedlist_filetozip;

class ZBuffer : public IZBuffer
{
private:
    int m_len;
    unsigned char* m_buffer;

public:
    ZBuffer()
    {
        m_len = 0;
        m_buffer = NULL;
    }

    ZBuffer(int len)
    {
        if(len <= 0)
        {
            m_len = 0;
            m_buffer = NULL;
        }
        else
        {
            m_len = len;
            m_buffer = (unsigned char*)malloc(len + 1);
        }
    }

    ZBuffer(int len, unsigned char* buf)
    {
        if(len <= 0 || !buf)
        {
            m_len = 0;
            m_buffer = NULL;
        }
        else
        {
            m_len = len;
            m_buffer = (unsigned char*)malloc(len + 1);
            memcpy(m_buffer, buf, len);
        }
    }
    
    virtual ~ZBuffer()
    {
        if(m_len > 0 && m_buffer)
        {
            free(m_buffer);
            m_buffer = NULL;
            m_len = 0;
        }
    }

    void append(int len, unsigned char* buf)
    {
        if(len <= 0 || !buf)
            return;
        m_buffer = (unsigned char*)realloc(m_buffer, m_len + len + 1);
        memcpy(m_buffer + m_len, buf, len);
        m_len += len;
        m_buffer[m_len] = '\0';
    }

    void append(ZBuffer& buf)
    {
        if(buf.length() <= 0)
            return;
        append(buf.length(), buf.buffer());
    }

    unsigned char* detach()
    {
        if(m_len > 0 && m_buffer)
        {
            unsigned char* ptr = m_buffer;
            m_len = 0;
            m_buffer = NULL;
            return ptr;
        }
        else
            return NULL;
    }

    void setBuffer(int len, unsigned char* buf)
    {
        clear();
        m_len = len;
        m_buffer = buf;
    }

    unsigned char* buffer()
    {
        return m_buffer;
    }

    int length()
    {
        return m_len;
    }
    
    void setLength(int len)
    {
        m_len = len;
    }

    ZBuffer& clear()
    {
        if(m_buffer)
        {
            free(m_buffer);
            m_buffer = NULL; 
        }

        m_len = 0;
        return *this;
    }

    unsigned char* reserve(int len)
    {
        clear();

        if(len > 0)
        {
            m_len = len;
            m_buffer = (unsigned char*)malloc(len + 1);
            m_buffer[len] = '\0';
        }

        return m_buffer;
    }

    bool equal(int len, unsigned char* buf)
    {
        if(m_len == 0 || len == 0)
            return m_len == len;

        return (m_len == len && (strncmp((const char*)m_buffer, (const char*)buf, m_len) == 0));
    }
};

class ZKeyCache;

class RSAZCryptor : public IZEncryptor, public IZDecryptor, public IZZIPor
{
private:
    unsigned m_trace_level;

    BIO *bio_err;
    BIO *priv_mem, *pub_mem;
    EVP_PKEY *privkey, *pubkey;
    RSA* priv_rsa, *pub_rsa;
    int priv_size, pub_size;

    ZBuffer m_sessionkey;
    ZBuffer m_encrypted_sessionkey;
    
    bool m_encoding;

    ZKeyCache* m_keycache;

    linkedlist_filetozip* m_filesToBeZIP;
    
    void throw_error();
    int zip(int in_len, unsigned char* in, ZBuffer& outbuf);
    int unzip(int in_len, unsigned char* in, ZBuffer& outbuf);

    void cleanFileList(linkedlist_filetozip* pFileList);

    void seed_prng();
    ZBuffer& generate_key(int len, ZBuffer& keybuf);

    void init();
    virtual int setPublicKey(const char* publickey);
    virtual int setPrivateKey(const char* privatekey, const char* passphrase);

public:
    RSAZCryptor();
    RSAZCryptor(const char* publickey);
    RSAZCryptor(const char* privatekey, const char* passphrase);
    virtual ~RSAZCryptor();

    virtual ZBuffer& publickey_encrypt(int in_len, unsigned char* in, ZBuffer& result);
    virtual ZBuffer& privatekey_decrypt(int in_len, unsigned char* in, ZBuffer& result);

    virtual void setTraceLevel(unsigned trace_level);
    virtual void setEncoding(bool yes);

    virtual int encrypt(int in_len, unsigned char* in, IZBuffer*& session_key, IZBuffer*& encrypted_data);
    virtual IZBuffer* decrypt(int key_len, unsigned char* keybuf, int in_len, unsigned char* inbuf);
    virtual IZBuffer* decrypt(unsigned char* keybuf, unsigned char* inbuf);
    virtual int gzipToFile(unsigned in_len, void const *in, const char* fileToBeZipped);
    
    virtual int addContentToZIP(unsigned contentLength, void *content, char* fileName,  bool append = false);
    virtual int addContentToZIP(unsigned contentLength, void *content, char* fileName,  time_t tm, bool append = false);
    virtual int zipToFile(const char* zipFileName, bool cleanFileListAfterUsed = true);
    virtual int zipToFile(unsigned contentLength, void const *content, const char* fileToBeZipped, const char* fileOut);
    
};

class ZKeyEntry : public IZInterface
{
private:
    time_t m_timestamp;
    ZBuffer* m_ekey;
    ZBuffer* m_key;

public:
    ZKeyEntry(int elen, unsigned char* ekey, int len, unsigned char* key)
    {
        m_ekey = new ZBuffer(elen, ekey);
        m_key = new ZBuffer(len, key);

        time_t t;
        time(&t);
        m_timestamp = t;            
    }

    ZBuffer* getEKey()
    {
        return m_ekey;
    }

    time_t getTimestamp()
    {
        return m_timestamp;
    }

    ZBuffer* getKey()
    {
        return m_key;
    }

    virtual ~ZKeyEntry() 
    {
        if(m_ekey)
            delete m_ekey;
        if(m_key)
            delete m_key;
    }
};

#define ZKEYCACHESIZE 256
class ZKeyCache : public IZInterface
{
private:
    ZKeyEntry* m_cache[ZKEYCACHESIZE];

    ZMutex m_mutex;

    unsigned long simpleHash(int len, unsigned char* buf)
    {
        if(!len || !buf)
            return 0;

        unsigned long keyhash = 5381;
        for(int i = 0; i < len; i++)
        {
            int c = buf[i];
            keyhash = ((keyhash << 5) + keyhash) + c;
        }
        return keyhash;
    }
public:
    ZKeyCache()
    {
        for(int i = 0; i < ZKEYCACHESIZE; i++)
            m_cache[i] = NULL;
    }

    virtual ~ZKeyCache() 
    {
        for(int i = 0; i < ZKEYCACHESIZE; i++)
        {
            ZKeyEntry* cur = m_cache[i];
            if(cur)
                delete cur;
        }
    }
    
    ZBuffer* getCachedKey(RSAZCryptor* zc, int elen, unsigned char* ekey);
};


#endif
