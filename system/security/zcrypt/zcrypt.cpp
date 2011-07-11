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

#pragma warning(disable: 4996)
#include "zcrypt.ipp"
#include "aes.hpp"
#include "base64.ipp"

#include "zip.h"

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

RSAZCryptor::RSAZCryptor()
{
    init();
}

RSAZCryptor::RSAZCryptor(const char* publickey)
{
    init();
    setPublicKey(publickey);
}

RSAZCryptor::RSAZCryptor(const char* privatekey, const char* passphrase)
{
    init();
    setPrivateKey(privatekey, passphrase);
}

void RSAZCryptor::init()
{
    m_trace_level = 0;
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

    CRYPTO_mem_ctrl(CRYPTO_MEM_CHECK_ON);
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

int RSAZCryptor::setPublicKey(const char* publickey)
{
    if(!publickey || !*publickey)
    {
        if(m_trace_level > 0)
            printf("Warning: publickey is empty\n");

        return -1;
    }

    if(m_trace_level > 10)
        printf("setting publickey:\n%s\n", publickey);

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
    BIO_puts(pub_mem, publickey);
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

int RSAZCryptor::setPrivateKey(const char* privatekey, const char* passphrase)
{
    if(!privatekey || !*privatekey)
    {
        if(m_trace_level > 0)
            printf("Warning: privatekey is empty\n");

        return -1;
    }

    if(m_trace_level > 10)
        printf("setting privatekey:\n%s\n", privatekey);

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
    BIO_puts(priv_mem, privatekey);
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
        CRYPTO_mem_leaks(bio_err);
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

