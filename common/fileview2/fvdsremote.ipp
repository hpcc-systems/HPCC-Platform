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

#ifndef FVDSREMOTE_IPP
#define FVDSREMOTE_IPP

#include "fvdatasource.hpp"
#include "fvsource.ipp"
#include "dadfs.hpp"
#include "dasess.hpp"
#include "mpbuff.hpp"

enum { 
    FVRemoteVersion1 = 1,
    CurRemoteVersion = FVRemoteVersion1
};

class RemoteDataSource : public ADataSource
{
public:
    RemoteDataSource(const SocketEndpoint & _serverEP, unique_id_t _id, IFvDataSourceMetaData * _metaData, __int64 _cachedNumRows, bool _isIndex);

    virtual bool init() { return true; }
    virtual IFvDataSourceMetaData * queryMetaData();
    virtual __int64 numRows(bool force = false);
    virtual bool fetchRow(MemoryBuffer & out, __int64 offset);
    virtual bool fetchRawRow(MemoryBuffer & out, __int64 offset);
    virtual bool getRow(MemoryBuffer & out, __int64 row);
    virtual bool getRawRow(MemoryBuffer & out, __int64 row);
    virtual bool isIndex() { return index; }
    virtual void onClose();
    virtual void onOpen();

    virtual void beforeDispose();
    virtual bool optimizeFilter(unsigned offset, unsigned len, const void * data) { return false; } // MORE: Needs implementing if this is ever used.

protected:
    bool getARow(MemoryBuffer & out, RowCache & cache, byte cmd, __int64 row);
    void sendReceive(CMessageBuffer & msg);

protected:
    const SocketEndpoint & serverEP;
    unique_id_t id;
    Owned<IFvDataSourceMetaData> metaData;
    Owned<INode> serverNode;
    RowCache rawRows;
    RowCache translatedRows;
    __int64 cachedNumRows;
    unsigned openCount;
    bool index;
};


class RemoteDataEntry : public CInterface
{
public:
    ~RemoteDataEntry();
    
public:
    unique_id_t id;
    SessionId session;
    SubscriptionId subscription;
    Owned<IFvDataSource> ds;
};

class RemoteDataSourceServer : public Thread, public ISessionNotify
{
public:
    RemoteDataSourceServer(const char * _queue, const char * _cluster);
    IMPLEMENT_IINTERFACE

//Thread
    virtual int run();

//ISessionNotify
    virtual void closed(SessionId id);
    virtual void aborted(SessionId id);
    void stop();

protected:
    unique_id_t addDataSource(SessionId session, IFvDataSource * ds);
    void doCmdFetch(bool raw, MemoryBuffer & in, MemoryBuffer & out);
    void doCmdFetchRaw(bool raw, MemoryBuffer & in, MemoryBuffer & out);
    void doCmdRow(bool raw, MemoryBuffer & in, MemoryBuffer & out);
    void doCmdRaw(MemoryBuffer & in, MemoryBuffer & out);
    void doCmdNumRows(MemoryBuffer & in, MemoryBuffer & out);
    void doCmdCreateWorkunit(MemoryBuffer & in, MemoryBuffer & out);
    void doCmdCreateFile(MemoryBuffer & in, MemoryBuffer & out);
    void doCmdDestroy(MemoryBuffer & in, MemoryBuffer & out);
    IFvDataSource * getDataSource(unique_id_t id);
    IFvDataSource * readDataSource(MemoryBuffer & in);
    void removeSession(SessionId id);

protected:
    bool alive;
    unique_id_t nextId;
    CriticalSection cs;
    StringAttr queue;
    StringAttr cluster;
    CIArrayOf<RemoteDataEntry> entries;
};

#endif
