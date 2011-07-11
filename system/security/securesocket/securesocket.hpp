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

#ifndef SECURESOCKET_HPP__
#define SECURESOCKET_HPP__

#ifndef SECURESOCKET_API

#ifdef _WIN32
    #ifndef SECURESOCKET_EXPORTS
        #define SECURESOCKET_API __declspec(dllimport)
    #else
        #define SECURESOCKET_API __declspec(dllexport)
    #endif //SECURESOCKET_EXPORTS
#else
    #define SECURESOCKET_API
#endif //_WIN32

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

