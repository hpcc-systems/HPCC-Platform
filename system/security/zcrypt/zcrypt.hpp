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

#ifndef ZCRYPT_HPP__
#define ZCRYPT_HPP__

#ifndef ZCRYPT_API
#ifdef _WIN32
    #ifndef ZCRYPT_EXPORTS
        #define ZCRYPT_API __declspec(dllimport)
    #else
        #define ZCRYPT_API __declspec(dllexport)
    #endif
#else
    #define ZCRYPT_API
#endif //_WIN32
#endif 

#include <string>

using namespace std;

class ZCRYPT_API IZInterface
{
public:
    virtual ~IZInterface() {}
};

class ZCRYPT_API IZBuffer : public IZInterface
{
public:
    virtual ~IZBuffer();

    virtual int length() = 0;
    virtual unsigned char* buffer() = 0;
};

class ZCRYPT_API IZEncryptor : public IZInterface
{
public:
    virtual ~IZEncryptor();
    virtual void setTraceLevel(unsigned trace_level) = 0;
    virtual void setEncoding(bool yes = true) = 0;
    virtual int encrypt(int in_len, unsigned char* in, IZBuffer*& session_key, IZBuffer*& encrypted_data) = 0;
};

class ZCRYPT_API IZDecryptor : public IZInterface
{
public:
    virtual ~IZDecryptor();
    virtual void setTraceLevel(unsigned trace_level) = 0;
    virtual void setEncoding(bool yes = true) = 0;  
    virtual IZBuffer* decrypt(int key_len, unsigned char* keybuf, int in_len, unsigned char* inbuf) = 0;
    virtual IZBuffer* decrypt(unsigned char* keybuf, unsigned char* inbuf) = 0;
};

class ZCRYPT_API IZZIPor : public IZInterface
{
public:
    virtual ~IZZIPor();
    virtual void setTraceLevel(unsigned trace_level) = 0;
    virtual int gzipToFile(unsigned in_len, void const *in, const char* fileToBeZipped) = 0;
    virtual int addContentToZIP(unsigned contentLength, void *content, char* fileName, bool append = false) = 0;
    virtual int addContentToZIP(unsigned contentLength, void *content, char* fileName, time_t tm, bool append = false) = 0;
    virtual int zipToFile(unsigned in_len, void const *in, const char* fileToBeZipped, const char* zippedFileName) = 0;
    virtual int zipToFile(const char* zipFileName, bool cleanFileListAfterUsed = true) = 0;
};

extern "C"
{
ZCRYPT_API IZZIPor* createZZIPor();
ZCRYPT_API IZEncryptor* createZEncryptor(const char* publickey);
ZCRYPT_API IZDecryptor* createZDecryptor(const char* privatekey, const char* passphrase);
ZCRYPT_API void releaseIZ(IZInterface* iz);
}

#endif
