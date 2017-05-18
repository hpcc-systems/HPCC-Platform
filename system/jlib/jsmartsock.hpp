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


#ifndef JSMARTSOCK_HPP
#define JSMARTSOCK_HPP

#include "jsocket.hpp"

interface jlib_decl ISmartSocket : extends IInterface
{
    // unique to ISmartSocket
    virtual ISocket *querySocket() = 0;

    // subset of ISocket
    virtual void read(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read,
                        unsigned timeoutsecs = WAIT_FOREVER) = 0;
    virtual void read(void* buf, size32_t size)=0;


    virtual size32_t write(void const* buf, size32_t size) = 0;

    virtual void close() = 0;
};


interface jlib_decl ISmartSocketFactory : extends IInterface
{
    virtual int run()=0;

    virtual ISmartSocket *connect( )=0;

    virtual ISmartSocket *connect_timeout(unsigned timeoutms)=0;

    virtual ISmartSocket *connectNextAvailableSocket() = 0;

    virtual SocketEndpoint& nextEndpoint() = 0;

    virtual bool getStatus(SocketEndpoint &ep) = 0;
    virtual void setStatus(SocketEndpoint &ep, bool status) = 0;

    virtual void stop() = 0;

    virtual void resolveHostnames() = 0;

    virtual StringBuffer & getUrlStr(StringBuffer &str, bool useHostName) = 0;
};


interface jlib_thrown_decl ISmartSocketException : extends IException
{
};


jlib_decl ISmartSocketFactory *createSmartSocketFactory(const char *_socklist, bool _retry = false, unsigned _retryInterval = 60, unsigned _dnsInterval = (unsigned) -1);

jlib_decl ISmartSocketException *createSmartSocketException(int errorCode, const char *msg);

#endif

