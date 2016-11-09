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

#ifndef SECURESOCKET_HPP__
#define SECURESOCKET_HPP__

#ifndef SECURESOCKET_API

#ifndef SECURESOCKET_EXPORTS
    #define SECURESOCKET_API DECL_IMPORT
#else
    #define SECURESOCKET_API DECL_EXPORT
#endif //SECURESOCKET_EXPORTS

#endif 

#include "jsocket.hpp"
#include "jptree.hpp"

#ifdef _WIN32
#define SSLIB "securesocket.dll"
#else
#define SSLIB "libsecuresocket.so"
#endif

enum SecureSocketType
{
    ClientSocket = 0,
    ServerSocket = 1
};

#define SSLogNone   0
#define SSLogMin    1
#define SSLogNormal 5
#define SSLogMax    10

// One instance per connection
interface ISecureSocket : implements ISocket
{
    virtual int secure_accept() = 0;
    virtual int secure_connect() = 0;
};

// One instance per program running
interface ISecureSocketContext : implements IInterface
{
    virtual ISecureSocket* createSecureSocket(ISocket* sock, int loglevel = SSLogNormal) = 0;
    virtual ISecureSocket* createSecureSocket(int sockfd, int loglevel = SSLogNormal) = 0;
};

interface ICertificate : implements IInterface
{
    virtual void setDestAddr(const char* destaddr) = 0;
    virtual void setDays(int days) = 0;
    virtual void setPassphrase(const char* passphrase) = 0;
    virtual void setCountry(const char* country) = 0;
    virtual void setState(const char* state) = 0;
    virtual void setCity(const char* city) = 0;
    virtual void setOrganization(const char* o) = 0;
    virtual void setOrganizationalUnit(const char* ou) = 0;
    virtual void setEmail(const char* email) = 0;

    virtual int generate(StringBuffer& certificate, StringBuffer& privkey) = 0;
    virtual int generate(StringBuffer& certificate, const char* privkey) = 0;
    virtual int generateCSR(StringBuffer& privkey, StringBuffer& csr) = 0;
    virtual int generateCSR(const char* privkey, StringBuffer& csr) = 0;
};

typedef ISecureSocketContext* (*createSecureSocketContext_t)(SecureSocketType);
typedef ISecureSocketContext* (*createSecureSocketContextEx_t)(const char* certfile, const char* privkeyfile, const char* passphrase, SecureSocketType);
typedef ISecureSocketContext* (*createSecureSocketContextEx2_t)(IPropertyTree* config, SecureSocketType);

extern "C" {

SECURESOCKET_API ISecureSocketContext* createSecureSocketContext(SecureSocketType);
SECURESOCKET_API ISecureSocketContext* createSecureSocketContextEx(const char* certfile, const char* privkeyfile, const char* passphrase, SecureSocketType);
SECURESOCKET_API ISecureSocketContext* createSecureSocketContextEx2(IPropertyTree* config, SecureSocketType);
SECURESOCKET_API ICertificate *createCertificate();
SECURESOCKET_API int signCertificate(const char* csr, const char* ca_certificate, const char* ca_privkey, const char* ca_passphrase, int days, StringBuffer& certificate);
};

#endif

