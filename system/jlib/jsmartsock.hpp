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
};


interface jlib_thrown_decl ISmartSocketException : extends IException
{
};


jlib_decl ISmartSocketFactory *createSmartSocketFactory(const char *_socklist, bool _retry = false, unsigned _retryInterval = 60, unsigned _dnsInterval = (unsigned) -1);


#endif

