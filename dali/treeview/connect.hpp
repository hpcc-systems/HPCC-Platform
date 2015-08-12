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
