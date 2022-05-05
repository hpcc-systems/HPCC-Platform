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

#ifdef _WIN32
#pragma warning(disable: 4996)
#endif
#include "zcrypt.ipp"
#include "aes.hpp"
#include "base64.ipp"

#include "zip.h"
#include "jexcept.hpp"
#include <math.h>

#ifdef WIN32
#define USEWIN32IOAPI
#include "iowin32.h"
#endif

IZBuffer::~IZBuffer()
{
}

IZEncryptor::~IZEncryptor()
{
}

IZDecryptor::~IZDecryptor()
{
}

IZZIPor::~IZZIPor()
{
}

RSAZCryptor::RSAZCryptor(unsigned traceLevel)
{
    setTraceLevel(traceLevel);
    init();
}

RSAZCryptor::RSAZCryptor(const char* publickeyBuff, unsigned traceLevel)
{
    setTraceLevel(traceLevel);
    init();
    setPublicKey(publickeyBuff);
}

RSAZCryptor::RSAZCryptor(const char* privatekeyBuff, const char* passphrase, unsigned traceLevel)
{
    setTraceLevel(traceLevel);
    init();
    setPrivateKey(privatekeyBuff, passphrase);
}

void RSAZCryptor::init()
{
    m_encoding = true;

    bio_err = NULL;
    priv_mem = NULL;
    pub_mem = NULL;
    privkey = NULL;
    pubkey = NULL;
    priv_rsa = NULL;
    pub_rsa = NULL;
    priv_size = 0;
    pub_size = 0;

    bio_err=BIO_new_fp(stderr, BIO_NOCLOSE);

#if defined(_WIN32) || defined(__linux__)
    OpenSSL_add_all_ciphers();
#else
    SSL_library_init();
#endif

    ERR_load_crypto_strings ();

    seed_prng();

    m_keycache = new ZKeyCache();

    m_filesToBeZIP = NULL;

}

int RSAZCryptor::setPublicKey(const char* publickeyBuff)
{
    if(!publickeyBuff || !*publickeyBuff)
    {
        if(m_trace_level > 0)
            printf("Warning: publickeyBuff is empty\n");

        return -1;
    }

    if(pubkey)
    {
        EVP_PKEY_free(pubkey);
        pubkey = NULL;
    }

    if(pub_mem)
    {
        BIO_free(pub_mem);
        pub_mem = NULL;
    }

    if(pub_rsa)
    {
        RSA_free(pub_rsa);
        pub_rsa = NULL;
    }

    pub_size = 0;

    pub_mem = BIO_new(BIO_s_mem());
    BIO_puts(pub_mem, publickeyBuff);
    if (!(pubkey = PEM_read_bio_PUBKEY(pub_mem, NULL, NULL, NULL)))
    {
        throw_error();
    }

    if(!(pub_rsa = EVP_PKEY_get1_RSA(pubkey)))
    {
        throw_error();
    }

    pub_size = RSA_size(pub_rsa);       

    generate_key(32, m_sessionkey);
    ZBuffer ekeybuf;
    publickey_encrypt(m_sessionkey.length(), m_sessionkey.buffer(), ekeybuf);
    base64_encode(ekeybuf.length(), ekeybuf.buffer(), m_encrypted_sessionkey);

    return 0;
}

int RSAZCryptor::setPrivateKey(const char* privatekeyBuff, const char* passphrase)
{
    if(!privatekeyBuff || !*privatekeyBuff)
    {
        if(m_trace_level > 0)
            printf("Warning: privatekeyBuff is empty\n");

        return -1;
    }

    if(privkey)
    {
        EVP_PKEY_free(privkey);
        privkey = NULL;
    }

    if(priv_mem)
    {
        BIO_free(priv_mem);
        priv_mem = NULL;
    }

    if(priv_rsa)
    {
        RSA_free(priv_rsa);
        priv_rsa = NULL;
    }

    priv_size = 0;

    priv_mem = BIO_new(BIO_s_mem());
    BIO_puts(priv_mem, privatekeyBuff);
    if (!(privkey = PEM_read_bio_PrivateKey (priv_mem, NULL, NULL, (void*)passphrase)))
    {
        throw_error();
    }

    if(!(priv_rsa = EVP_PKEY_get1_RSA(privkey)))
    {
        throw_error();
    }

    priv_size = RSA_size(priv_rsa);

    return 0;
}

RSAZCryptor::~RSAZCryptor()
{
    if(m_keycache)
        delete m_keycache;

    if(bio_err)
    {
#ifndef OPENSSL_NO_CRYPTO_MDEBUG
        CRYPTO_mem_leaks(bio_err);
#endif
        BIO_free(bio_err);
    }

    if(privkey)
        EVP_PKEY_free(privkey);

    if(pubkey)
        EVP_PKEY_free(pubkey);

    if(priv_mem)
        BIO_free(priv_mem);

    if(pub_mem)
        BIO_free(pub_mem);

    if(priv_rsa)
        RSA_free(priv_rsa); 

    if(pub_rsa)
        RSA_free(pub_rsa);      

    EVP_cleanup();
}

void RSAZCryptor::throw_error()
{
    char errbuf[512];
    ERR_error_string_n(ERR_get_error(), errbuf, 512);
    
    if(m_trace_level > 0)
        printf("Error: %s\n", errbuf);

    throw string(errbuf);
}

void RSAZCryptor::setTraceLevel(unsigned trace_level)
{
    m_trace_level = trace_level;
}

int RSAZCryptor::gzipToFile(unsigned in_len, void const *in, const char* tempFile)
{
    if (in_len < 1)
        return -1;

    if (!in)
        return -2;

    if (!tempFile || !*tempFile)
        return -3;

    gzFile fp = gzopen(tempFile, "wb");
    if (!fp)
        return -4;

    gzwrite(fp, in, in_len);
    gzclose(fp);

    return 0;
}

//a caller should be responsible for releasing memory for 
//the 'content' after the zipToFile function is called. 
int RSAZCryptor::addContentToZIP(unsigned contentLength, void *content, char* fileName,  bool append)
{
    time_t simple;
    time(&simple);

    return addContentToZIP(contentLength, content, fileName, simple, append);
}

//a caller should be responsible for releasing memory for 
//the 'content' after the zipToFile function is called. 
int RSAZCryptor::addContentToZIP(unsigned contentLength, void *content, char* fileName, time_t fileTM,  bool append)
{
    if (!append && m_filesToBeZIP)
    {
        cleanFileList(m_filesToBeZIP);
        m_filesToBeZIP = NULL;
    }

    linkedlist_filetozip* pFile = new linkedlist_filetozip();
    pFile->next_filetozip = NULL;
    pFile->file_name = NULL;

    if (fileName && *fileName)
    {
        int len = strlen(fileName);
        pFile->file_name = (char*)malloc(len+1);
        strcpy(pFile->file_name, fileName);
    }
    pFile->file_content = content;
    pFile->content_length = contentLength;
    pFile->file_time = fileTM;

    if (!m_filesToBeZIP)
        m_filesToBeZIP = pFile;
    else
    {
        linkedlist_filetozip* ppFile = m_filesToBeZIP;
        while (ppFile->next_filetozip)
        {
            ppFile = ppFile->next_filetozip;
        }
            
        ppFile->next_filetozip = pFile;
    }
    return 0;
}

void RSAZCryptor::cleanFileList(linkedlist_filetozip* pFileList)
{
    if (!pFileList) //nothing to clean
        return;

    if (pFileList->next_filetozip)
    {
        cleanFileList(pFileList->next_filetozip);
    }

    if (pFileList->file_name)
        free(pFileList->file_name);

    delete pFileList;
    pFileList = NULL;
}

int RSAZCryptor::zipToFile(const char* zipFileName, bool cleanFileListAfterUsed)
{
    unsigned len=(int)strlen(zipFileName);
    char* filename_try = (char*)malloc(len+16);
   if (filename_try==NULL)
   {
        return ZIP_INTERNALERROR;
   }

    strcpy(filename_try, zipFileName);

    bool dot_found = false;
    for (unsigned i=0; i<len; i++)
    {
        if (filename_try[i]=='.')
        {
             dot_found=true;
             break;
        }
    }

    if (!dot_found)
        strcat(filename_try,".zip");

    zipFile zf;
    int opt_overwrite=0; //?1

#ifdef USEWIN32IOAPI
    zlib_filefunc_def ffunc;
    fill_win32_filefunc(&ffunc);
    zf = zipOpen2(filename_try,(opt_overwrite==2) ? 2 : 0,NULL,&ffunc);
#else
    zf = zipOpen(filename_try,(opt_overwrite==2) ? 2 : 0);
#endif

    int err=0;
    if (zf == NULL)
    {
        printf("error opening %s\n",filename_try);
        err= ZIP_ERRNO;
    }

    unsigned count = 0;
    linkedlist_filetozip* pFileList = m_filesToBeZIP;
    while (pFileList && (err==ZIP_OK))
    {
        count++;

        unsigned contentLength = pFileList->content_length;
        void const *content = pFileList->file_content;
        char* fileName = NULL;
        char fileName0[16];

        if (pFileList->file_name)
            fileName = pFileList->file_name;
        else
        {
            sprintf(fileName0, "file%d", count);
            fileName = fileName0;
        }
        struct tm * ts = gmtime(&pFileList->file_time);

        zip_fileinfo zi;
        zi.tmz_date.tm_sec = ts->tm_sec;
        zi.tmz_date.tm_min = ts->tm_min;   
        zi.tmz_date.tm_hour = ts->tm_hour; 
        zi.tmz_date.tm_mday = ts->tm_mday;  
        zi.tmz_date.tm_mon = ts->tm_mon;
        zi.tmz_date.tm_year = ts->tm_year;  

        zi.dosDate = 0;
        zi.internal_fa = 0;
        zi.external_fa = 0;

        err = zipOpenNewFileInZip3(zf,fileName,&zi,
                                NULL,0,NULL,0,NULL /* comment*/,
                                Z_DEFLATED,
                                Z_DEFAULT_COMPRESSION,0,
                                /* -MAX_WBITS, DEF_MEM_LEVEL, Z_DEFAULT_STRATEGY, */
                                -MAX_WBITS, DEF_MEM_LEVEL, Z_DEFAULT_STRATEGY,
                                NULL, 0);

        if (err != ZIP_OK)
            printf("error in opening %s in zipfile\n",fileName);

        if (contentLength>0)
        {
            err = zipWriteInFileInZip (zf,content,contentLength);
            if (err<0)
            {
                printf("error in writing %s in the zipfile\n", fileName);
            }
        }

        if (err<0)
            err=ZIP_ERRNO;
        else
        {
            err = zipCloseFileInZip(zf);
            if (err!=ZIP_OK)
                printf("error in closing %s in the zipfile\n", fileName);
        }

        pFileList = pFileList->next_filetozip;
    }

    if (zipClose(zf,NULL) != ZIP_OK)
        printf("error in closing %s\n",filename_try);

    free(filename_try);

    if (cleanFileListAfterUsed)
    {
        cleanFileList(m_filesToBeZIP);
        m_filesToBeZIP = NULL;
    }
    return 0;
}

int RSAZCryptor::zipToFile(unsigned contentLength, void const *content, const char* fileToBeZipped, const char* fileOut)
{
    unsigned len=(int)strlen(fileOut);
    char* filename_try = (char*)malloc(len+16);
   if (filename_try==NULL)
   {
        return ZIP_INTERNALERROR;
   }

    strcpy(filename_try, fileOut);

    bool dot_found = false;
    for (unsigned i=0; i<len; i++)
    {
        if (filename_try[i]=='.')
        {
             dot_found=true;
             break;
        }
    }

    if (!dot_found)
        strcat(filename_try,".zip");

    zipFile zf;
    int opt_overwrite=0; //?1

#ifdef USEWIN32IOAPI
    zlib_filefunc_def ffunc;
    fill_win32_filefunc(&ffunc);
    zf = zipOpen2(filename_try,(opt_overwrite==2) ? 2 : 0,NULL,&ffunc);
#else
    zf = zipOpen(filename_try,(opt_overwrite==2) ? 2 : 0);
#endif

    int err=0;
    if (zf == NULL)
    {
        printf("error opening %s\n",filename_try);
        err= ZIP_ERRNO;
    }

    zip_fileinfo zi;
    zi.tmz_date.tm_sec = zi.tmz_date.tm_min = zi.tmz_date.tm_hour =
    zi.tmz_date.tm_mday = zi.tmz_date.tm_mon = zi.tmz_date.tm_year = 0;
    zi.dosDate = 0;
    zi.internal_fa = 0;
    zi.external_fa = 0;

    err = zipOpenNewFileInZip3(zf,fileToBeZipped,&zi,
                            NULL,0,NULL,0,NULL /* comment*/,
                            Z_DEFLATED,
                            Z_DEFAULT_COMPRESSION,0,
                            /* -MAX_WBITS, DEF_MEM_LEVEL, Z_DEFAULT_STRATEGY, */
                            -MAX_WBITS, DEF_MEM_LEVEL, Z_DEFAULT_STRATEGY,
                            NULL, 0);

    if (err != ZIP_OK)
        printf("error in opening %s in zipfile\n",fileToBeZipped);

    if (contentLength>0)
    {
        err = zipWriteInFileInZip (zf,content,contentLength);
        if (err<0)
        {
            printf("error in writing %s in the zipfile\n", fileToBeZipped);
        }
    }

    if (err<0)
        err=ZIP_ERRNO;
    else
    {
        err = zipCloseFileInZip(zf);
        if (err!=ZIP_OK)
            printf("error in closing %s in the zipfile\n", fileToBeZipped);
    }

    if (zipClose(zf,NULL) != ZIP_OK)
        printf("error in closing %s\n",filename_try);

    free(filename_try);
    return 0;
}

int RSAZCryptor::zip(int in_len, unsigned char* in, ZBuffer& outbuf)
{
    if(in_len <= 0 || !in || !*in)
    {
        if(m_trace_level > 0)
            printf("Warning: input to zip() is empty\n");

        return Z_DATA_ERROR;
    }

    int ret;
    unsigned have;
    z_stream strm;

    int buflen = in_len / 10;
    if(buflen <= 1)
        buflen = 1;

    /* allocate deflate state */
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    ret = deflateInit(&strm, Z_DEFAULT_COMPRESSION);
    if (ret != Z_OK)
    {
        if(m_trace_level > 0)
            printf("Warning: deflateInit returned %d\n", ret);

        return ret;
    }

    strm.avail_in = in_len;
    strm.next_in = in;
    strm.total_out = 0;
    ZBuffer onebuf(buflen);
    do 
    {
        strm.avail_out = buflen;
        strm.next_out = onebuf.buffer();
        ret = deflate(&strm, Z_FINISH);    /* no bad return value */
        if(ret == Z_STREAM_ERROR)
        {
            if(m_trace_level > 0)
                printf("Warning: deflate() returned Z_STREAM_ERROR\n");

            return ret;
        }

        have = buflen - strm.avail_out;
        if(have > 0)
            outbuf.append(have, onebuf.buffer());
    } 
    while (strm.avail_out == 0);

    (void)deflateEnd(&strm);
    return Z_OK;
}

int RSAZCryptor::unzip(int in_len, unsigned char* in, ZBuffer& outbuf)
{
    if(in_len <= 0 || !in || !*in)
    {
        if(m_trace_level > 0)
            printf("Warning: input to unzip() is empty\n");

        return Z_DATA_ERROR;
    }

    int ret;
    unsigned have;
    z_stream strm;

    /* allocate inflate state */
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    strm.avail_in = 0;
    strm.next_in = Z_NULL;
    ret = inflateInit(&strm);
    if (ret != Z_OK)
    {
        if(m_trace_level > 0)
            printf("Warning: inflateInit returned %d\n", ret);

        return ret;
    }

    strm.avail_in = in_len;
    strm.next_in = in;

    int buflen = in_len * 2;
    ZBuffer onebuf(buflen);

    /* run inflate() on input until output buffer not full */
    do 
    {
        strm.avail_out = buflen;
        strm.next_out = onebuf.buffer();
        ret = inflate(&strm, Z_NO_FLUSH);
        if(ret == Z_STREAM_ERROR)
        {
            if(m_trace_level > 0)
                printf("Warning: deflate() returned Z_STREAM_ERROR\n");

            return ret;
        }

        switch (ret) 
        {
        case Z_NEED_DICT:
            ret = Z_DATA_ERROR;     /* and fall through */
        case Z_DATA_ERROR:
        case Z_MEM_ERROR:
            if(m_trace_level > 0)
                printf("Warning: deflate() returned %d\n", ret);

            (void)inflateEnd(&strm);
            return ret;
        }
        have = buflen - strm.avail_out;

        if(have > 0)
        {
            outbuf.append(have, onebuf.buffer());
        }
    }
    while (strm.avail_out == 0);

    /* clean up and return */
    (void)inflateEnd(&strm);
    
    return ret == Z_STREAM_END ? Z_OK : Z_DATA_ERROR;
}

void RSAZCryptor::setEncoding(bool yes)
{
    m_encoding = yes;
}

void RSAZCryptor::seed_prng()
{
#ifdef _WIN32
    __int64 curtime = __rdtsc();//read timestamp count
    void* ptr = (void*)&curtime;
    RAND_seed(ptr, 8);
#else
    RAND_load_file("/dev/urandom", 1024);
#endif
}


ZBuffer& RSAZCryptor::publickey_encrypt(int in_len, unsigned char* in, ZBuffer& result)
{
    if(in_len <= 0)
    {
        if(m_trace_level > 0)
            printf("Warning: input to publickey_encrypt is empty\n");

        return result;
    }

    ZBuffer onebuf(pub_size);
    int onelen = 0;
    int inc = pub_size - 42;
    int resultlen = (in_len/inc + (((in_len%inc) == 0)?0:1)) * pub_size;
    result.reserve(resultlen);
    int curpos = 0;
    unsigned char* resultptr = result.buffer();
    
    while(curpos < in_len)
    {
        int cur_inc = (in_len - curpos > inc)?inc:(in_len - curpos);

        onelen = RSA_public_encrypt(cur_inc, in + curpos, onebuf.buffer(), pub_rsa, RSA_PKCS1_OAEP_PADDING);
        if(onelen < 0)
        {
            throw_error();
        }

        memcpy(resultptr, onebuf.buffer(), onelen);
        curpos += cur_inc;
        resultptr += onelen;
    }

    return result;
}


ZBuffer& RSAZCryptor::privatekey_decrypt(int in_len, unsigned char* in, ZBuffer& result)
{
    if(in_len <= 0)
    {
        if(m_trace_level > 0)
            printf("Warning: input to privatekey_decrypt is empty\n");
        return result;
    }

    ZBuffer onebuf(priv_size);
    int onelen = 0;
    int inc = priv_size;
    int est_len = (in_len/priv_size + (((in_len%priv_size)==0)?0:1)) * (priv_size - 42);
    result.reserve(est_len);
    unsigned char* resultptr = result.buffer();
    unsigned resultlen = 0;

    int curpos = 0;
    while(curpos < in_len)
    {
        int cur_inc = (in_len - curpos > inc)?inc:(in_len - curpos);
        onelen = RSA_private_decrypt(cur_inc, in + curpos, onebuf.buffer(), priv_rsa, RSA_PKCS1_OAEP_PADDING);
        if(onelen < 0)
        {
            throw_error();
        }

        memcpy(resultptr + resultlen, onebuf.buffer(), onelen);
        resultlen += onelen;

        curpos += cur_inc;
    }

    resultptr[resultlen] = '\0';
    result.setLength(resultlen);

    return result;
}

ZBuffer& RSAZCryptor::generate_key(int len, ZBuffer& keybuf)
{
    if(len <= 0)
        return keybuf;

    if(m_trace_level > 5)
        printf("Info: generating a %d byte session key\n", len);

    keybuf.reserve(len);
    RAND_bytes(keybuf.buffer(), len);

    /*
    for(int i = 0; i < len - 1; i++)
    {
        printf("%02X", keybuf.buffer()[i]);
    }
    printf("%02X\n", keybuf.buffer()[len - 1]);
    */

    return keybuf;
}

int RSAZCryptor::encrypt(int in_len, unsigned char* in, IZBuffer*& session_key, IZBuffer*& encrypted_data)
{
    if(in_len <= 0 || !in || !*in)
    {
        if(m_trace_level > 0)
            printf("Warning: input to encrypt is empty\n");

        return 0;
    }
    
    if(m_trace_level > 10)
        printf("Info: encrypt input: \n%s\n", in);

    session_key = new ZBuffer;
    ((ZBuffer*)session_key)->append(m_encrypted_sessionkey.length(), m_encrypted_sessionkey.buffer());

    ZBuffer zippedbuf;
    int ret = zip(in_len, in, zippedbuf);
    int zippedlen = zippedbuf.length();
    if(ret != Z_OK || zippedlen <= 0)
        return -1;

    if(m_trace_level > 5)
        printf("Info: length before zipping: %d, length after zipping: %d, compression ratio: %6.2f:1\n", in_len, zippedlen, in_len*1.0/zippedlen);

    ZBuffer outbuf;
    aes_encrypt(m_sessionkey.buffer(), m_sessionkey.length(), zippedbuf.buffer(), zippedbuf.length(), outbuf);

    if(m_trace_level > 5)
        printf("Info: length after encryption: %d\n", outbuf.length());

    // base64 encode
    encrypted_data = new ZBuffer();
    if(m_encoding)
    {
        base64_encode(outbuf.length(), outbuf.buffer(), *(ZBuffer*)encrypted_data);
        if(m_trace_level > 5)
        {
            printf("Info: length after base64 encoding: %d\n", encrypted_data->length());
            if(m_trace_level > 10)
                printf("Info: zip/encryption/base64-encoding result:\n%s\n", encrypted_data->buffer());
        }
    }
    else
    {
        int result_len = outbuf.length();
        ((ZBuffer*)encrypted_data)->setBuffer(result_len, outbuf.detach());
    }

    return 0;
}

IZBuffer* RSAZCryptor::decrypt(int key_len, unsigned char* keybuf, int in_len, unsigned char* inbuf)
{
    if(key_len <= 0 || !keybuf)
    {
        if(m_trace_level > 0)
            printf("Warning: Please specify a key for decryption.\n");
        return NULL;
    }

    if(in_len <= 0 || !inbuf)
    {
        return NULL;
    }

    if(m_trace_level > 5)
    {
        printf("Info: length of keybuf %d\n", key_len);
        printf("Info: length of inbuf %d\n", in_len);
    }

    if(m_trace_level > 10)
    {
        printf("Info: key for decryption: \n%s\n", keybuf);
    }

    ZBuffer* dkeybuf = m_keycache->getCachedKey(this, key_len, keybuf);
    if(!dkeybuf)
    {
        return NULL;
    }

    // Base64 decode first
    ZBuffer dbuf;
    int dlen = in_len;
    unsigned char* dptr = inbuf;
    if(m_encoding)
    {
        base64_decode(in_len, (const char*)inbuf, dbuf);
        if(m_trace_level > 5)
            printf("Info: data length after base64 decode: %d\n", dbuf.length());
        dlen = dbuf.length();
        dptr = dbuf.buffer();
    }

    ZBuffer buf;
    aes_decrypt(dkeybuf->buffer(), dkeybuf->length(), dptr, dlen, buf);

    if(m_trace_level > 5)
        printf("Info: data length after aes_decryption: %d\n", buf.length());

    ZBuffer* outbuf = new ZBuffer();
    int ret = unzip(buf.length(), buf.buffer(), *outbuf);
    
    if(ret != Z_OK || outbuf->length() <= 0)
        return NULL;

    if(m_trace_level > 5)
        printf("Info: data length after base64-decode/decryption/unzip: %d\n", outbuf->length());

    if(m_trace_level > 10)
        printf("info: base64-decode/decryption/unzip result:\n%s\n", outbuf->buffer());

    return outbuf;
}

IZBuffer* RSAZCryptor::decrypt(unsigned char* keybuf, unsigned char* inbuf)
{
    if(!keybuf || !inbuf)
    {
        if(m_trace_level > 0)
            printf("Warning: Please specify key and input for decryption.\n");
        
        return NULL;
    }

    return decrypt(strlen((const char*)keybuf), keybuf, strlen((const char*)inbuf), inbuf);
}


ZBuffer* ZKeyCache::getCachedKey(RSAZCryptor* zc, int elen, unsigned char* ekey)
{
    if(!elen || !ekey)
        return NULL;

    zsynchronized block(m_mutex);

    int start = simpleHash(elen, ekey) % ZKEYCACHESIZE;
    int ind = start;
    int oldest = start;

    do
    {
        ZKeyEntry* entry = m_cache[ind];
        if(!entry)
            break;

        ZBuffer* cur_ekey = entry->getEKey();
        if(!cur_ekey)
        {
            delete entry;
            m_cache[ind] = NULL;
            break;
        }

        if(cur_ekey->equal(elen, ekey))
            return entry->getKey();
        
        if(entry->getTimestamp() < m_cache[oldest]->getTimestamp())
            oldest = ind;
        
        ind = (ind+1) % ZKEYCACHESIZE;
    }
    while(ind != start);

    if(m_cache[ind] != NULL)
    {
        if(m_cache[oldest] != NULL)
        {
            delete m_cache[oldest];
            m_cache[oldest] = NULL;
        }
        ind = oldest;
    }

    ZBuffer buf;
    base64_decode(elen, (char*)ekey, buf);
    ZBuffer keybuf;
    zc->privatekey_decrypt(buf.length(), buf.buffer(), keybuf);
    m_cache[ind] = new ZKeyEntry(elen, ekey, keybuf.length(), keybuf.buffer());

    return m_cache[ind]->getKey();

    return NULL;
}

extern "C" {

ZCRYPT_API IZEncryptor* createZEncryptor(const char* publickey)
{
    return new RSAZCryptor(publickey);
}

ZCRYPT_API IZDecryptor* createZDecryptor(const char* privatekey, const char* passphrase)
{
    return new RSAZCryptor(privatekey, passphrase);
}

ZCRYPT_API IZZIPor* createZZIPor()
{
    return new RSAZCryptor();
}

ZCRYPT_API void releaseIZ(IZInterface* iz)
{
    if(iz)
        delete iz;
}

}

inline const char *getZlibHeaderTypeName(ZlibCompressionType zltype)
{
    if (zltype==ZlibCompressionType::GZIP)
        return "gzip";
    if (zltype==ZlibCompressionType::ZLIB_DEFLATE)
        return "zlib_deflate";
    //DEFLATE, no header
    return "deflate";
}

static void throwZlibException(const char* operation, int errorCode, ZlibCompressionType zltype)
{
    const char* errorMsg;
    switch (errorCode)
    {
        case Z_ERRNO:
            errorMsg = "Error occured while reading file";
            break;
        case Z_STREAM_ERROR:
            errorMsg = "The stream state was inconsistent";
            break;
        case Z_DATA_ERROR:
            errorMsg = "The deflate data was invalid or incomplete";
            break;
        case Z_MEM_ERROR:
            errorMsg = "Memory could not be allocated for processing";
            break;
        case Z_BUF_ERROR:
            errorMsg = "Insufficient output buffer";
            break;
        case Z_VERSION_ERROR:
            errorMsg = "The version mismatch between zlib.h and the library linked";
            break;
        default:
            errorMsg = "Unknown exception";
            break;
    }
    throw MakeStringException(500, "Exception in %s %s: %s.", getZlibHeaderTypeName(zltype), operation, errorMsg);
}

inline int getWindowBits(ZlibCompressionType zltype)
{
    if (zltype==ZlibCompressionType::GZIP)
        return 15+16;
    if (zltype==ZlibCompressionType::ZLIB_DEFLATE)
        return 15;
    //DEFLATE, no header
    return -15;
}

// Compress a character buffer using zlib in gzip/zlib_deflate format with given compression level
//
void zlib_deflate(MemoryBuffer &mb, const char* inputBuffer, unsigned int inputSize, int compressionLevel, ZlibCompressionType zltype)
{
    if (inputBuffer == NULL || inputSize == 0)
        throw MakeStringException(500, "%s failed: input buffer is empty!", getZlibHeaderTypeName(zltype));

    /* Before we can begin compressing (aka "deflating") data using the zlib
     functions, we must initialize zlib. Normally this is done by calling the
     deflateInit() function; in this case, however, we'll use deflateInit2() so
     that the compressed data will have gzip headers if requested. This will make
     it easy to decompress the data later using a tool like gunzip, WinZip, etc.
     deflateInit2() accepts many parameters, the first of which is a C struct of
     type "z_stream" defined in zlib.h. The properties of this struct are used to
     control how the compression algorithms work. z_stream is also used to
     maintain pointers to the "input" and "output" byte buffers (next_in/out) as
     well as information about how many bytes have been processed, how many are
     left to process, etc. */
    z_stream zs;        // z_stream is zlib's control structure
    zs.zalloc = Z_NULL; // Set zalloc, zfree, and opaque to Z_NULL so
    zs.zfree  = Z_NULL; // that when we call deflateInit2 they will be
    zs.opaque = Z_NULL; // updated to use default allocation functions.
    zs.total_out = 0;   // Total number of output bytes produced so far

    /* Initialize the zlib deflation (i.e. compression) internals with deflateInit2().
     The parameters are as follows:
     z_streamp strm - Pointer to a zstream struct
     int level      - Compression level. Must be Z_DEFAULT_COMPRESSION, or between
                      0 and 9: 1 gives best speed, 9 gives best compression, 0 gives
                      no compression.
     int method     - Compression method. Only method supported is "Z_DEFLATED".
     int windowBits - Base two logarithm of the maximum window size (the size of
                      the history buffer). It should be in the range 8..15. Add
                      16 to windowBits to write a simple gzip header and trailer
                      around the compressed data instead of a zlib wrapper. The
                      gzip header will have no file name, no extra data, no comment,
                      no modification time (set to zero), no header crc, and the
                      operating system will be set to 255 (unknown).
     int memLevel   - Amount of memory allocated for internal compression state.
                      1 uses minimum memory but is slow and reduces compression
                      ratio; 9 uses maximum memory for optimal speed. Default value
                      is 8.
     int strategy   - Used to tune the compression algorithm. Use the value
                      Z_DEFAULT_STRATEGY for normal data, Z_FILTERED for data
                      produced by a filter (or predictor), or Z_HUFFMAN_ONLY to
                      force Huffman encoding only (no string match) */
    int ret = deflateInit2(&zs, compressionLevel, Z_DEFLATED, getWindowBits(zltype), 8, Z_DEFAULT_STRATEGY);
    if (ret != Z_OK)
        throwZlibException("initialization", ret, zltype);

    // set the z_stream's input
    zs.next_in = (Bytef*)inputBuffer;
    zs.avail_in = inputSize;

    // Create output memory buffer for compressed data. The zlib documentation states that
    // destination buffer size must be at least 0.1% larger than avail_in plus 12 bytes.
    const unsigned long outsize = (unsigned long) inputSize + inputSize / 1000 + 13;
    Bytef* outbuf = (Bytef*) mb.reserveTruncate(outsize);

    do
    {
        // Store location where next byte should be put in next_out
        zs.next_out = outbuf + zs.total_out;

        // Calculate the amount of remaining free space in the output buffer
        // by subtracting the number of bytes that have been written so far
        // from the buffer's total capacity
        zs.avail_out = outsize - zs.total_out;

        /* deflate() compresses as much data as possible, and stops/returns when
        the input buffer becomes empty or the output buffer becomes full. If
        deflate() returns Z_OK, it means that there are more bytes left to
        compress in the input buffer but the output buffer is full; the output
        buffer should be expanded and deflate should be called again (i.e., the
        loop should continue to rune). If deflate() returns Z_STREAM_END, the
        end of the input stream was reached (i.e.g, all of the data has been
        compressed) and the loop should stop. */
        ret = deflate(&zs, Z_FINISH);
    } while (ret == Z_OK);

    // Free data structures that were dynamically created for the stream.
    deflateEnd(&zs);

    if (ret != Z_STREAM_END)          // an error occurred that was not EOS
    {
        mb.clear();
        throwZlibException("compression", ret, zltype);
    }

    mb.setLength(zs.total_out);
}

// Compress a character buffer using zlib in gzip format with given compression level
//
void gzip(MemoryBuffer &mb, const char* inputBuffer, unsigned int inputSize, int compressionLevel)
{
    zlib_deflate(mb, inputBuffer, inputSize, compressionLevel, ZlibCompressionType::GZIP);
}

bool zlib_inflate(const byte* compressed, unsigned int comprLen, StringBuffer& sOutput, ZlibCompressionType zltype, bool inflateException)
{
    if (comprLen == 0)
        return true;

    const int CHUNK_OUT = 16384;
    z_stream d_stream; // decompression stream
    memset( &d_stream, 0, sizeof(z_stream));
    d_stream.next_in = (byte*) compressed;
    d_stream.avail_in = comprLen;
    int ret = inflateInit2(&d_stream, getWindowBits(zltype));
    if (ret != Z_OK)
        throwZlibException("initialization", ret, zltype); //don't ignore this

    unsigned int outLen = 0;

    do
    {
        sOutput.ensureCapacity( outLen + CHUNK_OUT );
        d_stream.avail_out = CHUNK_OUT; //free space in the output buffer
        d_stream.next_out = (byte*)sOutput.str() + outLen;

        ret = inflate(&d_stream, Z_NO_FLUSH);
        if (ret < Z_OK)
            break;

        outLen += CHUNK_OUT - d_stream.avail_out;
        sOutput.setLength( outLen );

    } while (d_stream.avail_out == 0 || ret != Z_STREAM_END);

    inflateEnd(&d_stream);
    if (ret != Z_STREAM_END)
    {
        sOutput.clear();
        if (!inflateException)
            return false;
        throwZlibException("decompression", ret, zltype);
    }
    return true;
}

void gunzip(const byte* compressed, unsigned int comprLen, StringBuffer& sOutput)
{
    zlib_inflate(compressed, comprLen, sOutput, ZlibCompressionType::GZIP, true);
}

void httpInflate(const byte* compressed, unsigned int comprLen, StringBuffer& sOutput, bool use_gzip)
{
    if (use_gzip)
    {
        zlib_inflate(compressed, comprLen, sOutput, ZlibCompressionType::GZIP, true);
    }
    else if (!zlib_inflate(compressed, comprLen, sOutput, ZlibCompressionType::ZLIB_DEFLATE, false)) //this is why gzip is preferred, deflate can mean 2 things
    {
        zlib_inflate(compressed, comprLen, sOutput, ZlibCompressionType::DEFLATE, true);
    }
}

bool isgzipped(const byte * content, size_t length)
{
    if (length < 2)
        return false;
    return (content[0] == 0x1f) && (content[1] == 0x8b);
}

void removeZipExtension(StringBuffer & target, const char * source)
{
    target.set(source);
    const char * extension = strrchr(target.str(), '.');
    if (extension && streq(extension, ".gz"))
        target.remove(extension-target.str(), 3);
}
