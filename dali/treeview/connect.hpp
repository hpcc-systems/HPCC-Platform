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


#ifndef _connect_hpp
#define _connect_hpp


#include "jptree.hpp"
#include "jsocket.hpp"


enum Connection_t               
{
    CT_none,                        // not connected to property tree
    CT_local,                       // connected to local property tree
    CT_remote                       // connected to remote property tree
};


interface IConnection : public IInterface
{
    virtual bool lockWrite() = 0;
    virtual bool unlockWrite() = 0;
    virtual Connection_t getType() = 0;
    virtual void commit() = 0;
    virtual IPropertyTree * queryRoot(const char * xpath = NULL) = 0;
    virtual LPCSTR queryName() = 0;
};


IConnection * createLocalConnection(LPCSTR filename);
IConnection * createRemoteConnection(SocketEndpointArray & epa);


#endif
