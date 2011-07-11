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

#include "jliball.hpp"
#include "eclrtl.hpp"

#include "hqlexpr.hpp"
#include "hqlthql.hpp"
#include "fvresultset.ipp"
#include "fileview.hpp"
#include "fvdisksource.ipp"
#include "fvwugen.hpp"
#include "fvdsremote.ipp"
#include "fverror.hpp"
#include "mpcomm.hpp"


#define TIMEOUT             60000
#define REMOTE_DATA_SIZE    8000        // roughly how much is sent back for each request

enum { FVCMDnone, FVCMDrow, FVCMDraw, FVCMDnumrows, FVCMDcreatewu, FVCMDcreatefile, FVCMDdestroy, FVCMDfetch, FVCMDfetchraw, FVCMDmax };

//---------------------------------------------------------------------------

static void sendReceive(INode * serverNode, CMessageBuffer & msg)
{
    if (!queryWorldCommunicator().sendRecv(msg, serverNode, MPTAG_FILEVIEW, TIMEOUT))
        throwError(FVERR_TimeoutRemoteFileView);

    msg.setEndian(__BIG_ENDIAN);
    IException * error = deserializeException(msg);
    if (error)
        throw error;
}


RemoteDataSource::RemoteDataSource(const SocketEndpoint & _serverEP, unique_id_t _id, IFvDataSourceMetaData * _metaData, __int64 _cachedNumRows, bool _isIndex) : serverEP(_serverEP)
{
    id = _id;
    metaData.set(_metaData);
    serverNode.setown(createINode(serverEP));
    cachedNumRows = _cachedNumRows;
    index = _isIndex;
    openCount = 0;
}


IFvDataSourceMetaData * RemoteDataSource::queryMetaData()
{
    return metaData;
}

void RemoteDataSource::beforeDispose()
{
    CMessageBuffer msg;
    msg.setEndian(__BIG_ENDIAN);
    msg.append((byte)FVCMDdestroy);
    msg.append(id);

    sendReceive(msg);
}


bool RemoteDataSource::getARow(MemoryBuffer & out, RowCache & cache, byte cmd, __int64 row)
{
    RowLocation location;

    if (cache.getCacheRow(row, location))
    {
        out.append(location.matchLength, location.matchRow);
        return true;
    }

    CMessageBuffer msg;
    msg.setEndian(__BIG_ENDIAN);
    msg.append(cmd);
    msg.append(id);
    msg.append(row);

    sendReceive(msg);

    bool ok;
    msg.read(ok);
    if (!ok) return false;

    __int64 start;
    msg.read(start);

    VariableRowBlock * next = new VariableRowBlock(msg, start);
    cache.addRowsOwn(next);

    if (!cache.getCacheRow(row, location))
        assertex(!"Internal Error!");
    out.append(location.matchLength, location.matchRow);
    return true;
}


bool RemoteDataSource::fetchRow(MemoryBuffer & out, __int64 offset)
{
    CMessageBuffer msg;
    msg.setEndian(__BIG_ENDIAN);
    msg.append(FVCMDfetch);
    msg.append(id);
    msg.append(offset);

    sendReceive(msg);

    bool ok;
    msg.read(ok);
    if (!ok) return false;
    size32_t len;
    msg.read(len);
    out.append(len, msg.readDirect(len));
    return true;
}

bool RemoteDataSource::fetchRawRow(MemoryBuffer & out, __int64 offset)
{
    CMessageBuffer msg;
    msg.setEndian(__BIG_ENDIAN);
    msg.append(FVCMDfetchraw);
    msg.append(id);
    msg.append(offset);

    sendReceive(msg);

    bool ok;
    msg.read(ok);
    if (!ok) return false;
    size32_t len;
    msg.read(len);
    out.append(len, msg.readDirect(len));
    return true;
}

bool RemoteDataSource::getRow(MemoryBuffer & out, __int64 row)
{
    return getARow(out, translatedRows, FVCMDrow, row);
}

bool RemoteDataSource::getRawRow(MemoryBuffer & out, __int64 row)
{
    return getARow(out, rawRows, FVCMDraw, row);
}


__int64 RemoteDataSource::numRows(bool force)
{
    if (!force)
        return cachedNumRows;
    CMessageBuffer msg;
    msg.setEndian(__BIG_ENDIAN);
    msg.append((byte)FVCMDnumrows);
    msg.append(id);

    sendReceive(msg);

    __int64 result;
    msg.read(result);
    return result;
}


void RemoteDataSource::onClose()
{ 
    if (--openCount == 0)
    {
        //MORE: Should tell the server...
    }
}

void RemoteDataSource::onOpen() 
{ 
    //MORE: critical section
    if (openCount++ == 0)
    {
        //MORE - tell the server...
    }
}

void RemoteDataSource::sendReceive(CMessageBuffer & msg)
{
    ::sendReceive(serverNode, msg);
}


IFvDataSource * createRemoteDataSource(const SocketEndpoint & server, const char * username, const char * password, const char * wuid, unsigned sequence, const char * name)
{
    Owned<INode> serverNode = createINode(server);

    CMessageBuffer msg;
    msg.setEndian(__BIG_ENDIAN);
    msg.append((byte)FVCMDcreatewu);
    msg.append(myProcessSession());
    msg.append(username);
    msg.append(password);
    msg.append(wuid);
    msg.append(sequence);
    msg.append(name);

    sendReceive(serverNode, msg);

    unsigned short version;
    unique_id_t id;
    __int64 numRows;
    bool isIndex;
    msg.read(version);
    msg.read(id);
    msg.read(numRows);
    Owned<IFvDataSourceMetaData> meta = deserializeDataSourceMeta(msg);
    msg.read(isIndex);
    if (id)
        return new RemoteDataSource(server, id, meta, numRows, isIndex);
    return 0;
}

IFvDataSource * createRemoteFileDataSource(const SocketEndpoint & server, const char * username, const char * password, const char * logicalName)
{
    Owned<INode> serverNode = createINode(server);

    CMessageBuffer msg;
    msg.setEndian(__BIG_ENDIAN);
    msg.append((byte)FVCMDcreatefile);
    msg.append(myProcessSession());
    msg.append(username);
    msg.append(password);
    msg.append(logicalName);

    sendReceive(serverNode, msg);

    unsigned short version;
    unique_id_t id;
    __int64 numRows;
    bool isIndex;
    msg.read(version);
    msg.read(id);
    msg.read(numRows);
    Owned<IFvDataSourceMetaData> meta = deserializeDataSourceMeta(msg);
    msg.read(isIndex);

    if (id)
        return new RemoteDataSource(server, id, meta, numRows, isIndex);
    return 0;
}



//---------------------------------------------------------------------------

static RemoteDataSourceServer * server;

RemoteDataEntry::~RemoteDataEntry()
{
    if (subscription)
        querySessionManager().unsubscribeSession(subscription);
}


RemoteDataSourceServer::RemoteDataSourceServer(const char * _queue, const char * _cluster) : Thread("Remote File View Server")
{
    alive = true;
    nextId = 0;
    queue.set(_queue);
    cluster.set(_cluster);
}


unique_id_t RemoteDataSourceServer::addDataSource(SessionId session, IFvDataSource * ds)
{
    RemoteDataEntry * newEntry = new RemoteDataEntry;
    newEntry->id = ++nextId;
    newEntry->session = session;
    newEntry->ds.set(ds);
    newEntry->subscription = querySessionManager().subscribeSession(session, this);

    //MORE: Register the session so if it dies then we get notified.
    CriticalBlock procedure(cs);
    entries.append(*newEntry);
    return newEntry->id;
}

void RemoteDataSourceServer::doCmdFetch(bool raw, MemoryBuffer & in, MemoryBuffer & out)
{
    Owned<IFvDataSource> ds = readDataSource(in);
    if (!ds)
    {
        out.append(false);
        return;
    }

    __int64 requestedOffset;
    in.read(requestedOffset);

    MemoryBuffer temp;
    bool ok = ds->fetchRow(temp, requestedOffset);
    out.append(ok);                     // ok
    out.append(temp.length());
    out.append(temp.length(), temp.toByteArray());
}

void RemoteDataSourceServer::doCmdFetchRaw(bool raw, MemoryBuffer & in, MemoryBuffer & out)
{
    Owned<IFvDataSource> ds = readDataSource(in);
    if (!ds)
    {
        out.append(false);
        return;
    }

    __int64 requestedOffset;
    in.read(requestedOffset);

    MemoryBuffer temp;
    bool ok = ds->fetchRawRow(temp, requestedOffset);
    out.append(ok);                     // ok
    out.append(temp.length());
    out.append(temp.length(), temp.toByteArray());
}

void RemoteDataSourceServer::doCmdRow(bool raw, MemoryBuffer & in, MemoryBuffer & out)
{
    Owned<IFvDataSource> ds = readDataSource(in);
    if (!ds)
    {
        out.append(false);
        return;
    }

    __int64 requestedRow;
    in.read(requestedRow);

    unsigned startPos = out.length();
    unsigned numRows = 0;
    out.append(true);                       // ok
    out.append(requestedRow);       // start 

    unsigned numRowsPos = out.length();
    out.append(numRows);                // total number of rows;
    loop
    {
        unsigned lengthPos = out.length();
        out.append((unsigned)0);                // size of this row.
        unsigned startRow = out.length();
        if (raw)
        {
            if (!ds->getRawRow(out, requestedRow+numRows))
                break;
        }
        else
        {
            if (!ds->getRow(out, requestedRow+numRows))
                break;
        }
        if ((numRows != 0) && (out.length() > REMOTE_DATA_SIZE))
            break;
        unsigned endRow = out.length();
        out.rewrite(lengthPos);
        out.append(endRow-startRow);
        out.rewrite(endRow);
        numRows++;
    }

    if (numRows == 0)
    {
        out.rewrite(startPos);
        out.append(false);
        return;
    }

    unsigned totalLength = out.length();
    out.rewrite(numRowsPos);
    out.append(numRows);
    out.rewrite(totalLength);
}

void RemoteDataSourceServer::doCmdNumRows(MemoryBuffer & in, MemoryBuffer & out)
{
    Owned<IFvDataSource> ds = readDataSource(in);
    __int64 numRows = ds ? ds->numRows(true) : 0;
    out.append(numRows);
}

void RemoteDataSourceServer::doCmdCreateWorkunit(MemoryBuffer & in, MemoryBuffer & out)
{
    SessionId session;
    StringAttr wuid, username, password;
    unsigned sequence;
    StringAttr name;

    in.read(session);
    in.read(username).read(password);
    in.read(wuid);
    in.read(sequence);
    in.read(name);

    DBGLOG("RemoteFileView:CreateWorkunit('%s',%d,'%s') by[%s:%"I64F"d", wuid.get(), sequence, name ? name.get() : "", username.get(), session);
    Owned<IConstWUResult> wuResult = resolveResult(wuid, sequence, name);
    Owned<IFvDataSource> ds = createDataSource(wuResult, wuid, username, password);
    unique_id_t id = addDataSource(session, ds);

    out.append((unsigned short)CurRemoteVersion);
    out.append(id);
    out.append(ds->numRows(false));
    ds->queryMetaData()->serialize(out);
    out.append(ds->isIndex());

    DBGLOG("RemoteFileView:CreateWorkunit returns %"I64F"d", id);
}

void RemoteDataSourceServer::doCmdCreateFile(MemoryBuffer & in, MemoryBuffer & out)
{
    SessionId session;
    StringAttr username, password, logicalName;

    in.read(session);
    in.read(username).read(password);
    in.read(logicalName);

    DBGLOG("RemoteFileView:CreateFile('%s') by[%s:%"I64F"d", logicalName.get(), username.get(), session);
    Owned<IFvDataSource> ds = createFileDataSource(logicalName, cluster, username, password);
    unique_id_t id = addDataSource(session, ds);

    out.append((unsigned short)CurRemoteVersion);
    out.append(id);
    out.append(ds->numRows(false));
    ds->queryMetaData()->serialize(out);
    out.append(ds->isIndex());

    DBGLOG("RemoteFileView:CreateFile returns %"I64F"d", id);
}

void RemoteDataSourceServer::doCmdDestroy(MemoryBuffer & in, MemoryBuffer & out)
{
    unique_id_t id;
    in.read(id);

    DBGLOG("RemoteFileView:Destroy(%"I64F"d)", id);
    CriticalBlock block(cs);
    ForEachItemIn(idx, entries)
    {
        RemoteDataEntry & cur = entries.item(idx);
        if (cur.id == id)
        {
            entries.remove(idx);
            return;
        }
    }
}


IFvDataSource * RemoteDataSourceServer::getDataSource(unique_id_t id)
{
    CriticalBlock block(cs);
    ForEachItemIn(idx, entries)
    {
        RemoteDataEntry & cur = entries.item(idx);
        if (cur.id == id)
            return LINK(cur.ds);
    }
    return NULL;
}


void RemoteDataSourceServer::closed(SessionId id)
{
    removeSession(id);
}

void RemoteDataSourceServer::aborted(SessionId id)
{
    removeSession(id);
}

IFvDataSource * RemoteDataSourceServer::readDataSource(MemoryBuffer & in)
{
    unique_id_t id;
    in.read(id);
    return getDataSource(id);
}


void RemoteDataSourceServer::removeSession(SessionId id)
{
    DBGLOG("RemoteFileView:Session Died");
    CriticalBlock block(cs);
    ForEachItemInRev(idx, entries)
    {
        RemoteDataEntry & cur = entries.item(idx);
        if (cur.session == id)
        {
            DBGLOG("RemoteFileView:Instance Died %"I64F"d", cur.id);
            entries.remove(idx);
        }
    }
}   


//MORE: If this is ever actually used then it should probably have several threads
//      processing the commands, especially if the commands can involve lots of processing.
int RemoteDataSourceServer::run()
{
    CMessageBuffer msg;
    MemoryBuffer result;
    INode * sender;
    while (alive)
    {
        msg.clear();
        if (queryWorldCommunicator().recv(msg, 0, MPTAG_FILEVIEW, &sender))
        {
            msg.setEndian(__BIG_ENDIAN);
            result.setEndian(__BIG_ENDIAN);


            try
            {
                serializeException(NULL, result.clear());
                byte cmd;
                msg.read(cmd);
                switch (cmd)
                {
                case FVCMDrow:          doCmdRow(false, msg, result); break;
                case FVCMDraw:          doCmdRow(true, msg, result); break;
                case FVCMDnumrows:      doCmdNumRows(msg, result); break;
                case FVCMDcreatewu:     doCmdCreateWorkunit(msg, result); break;
                case FVCMDcreatefile:   doCmdCreateFile(msg, result); break;
                case FVCMDdestroy:      doCmdDestroy(msg, result); break;
                case FVCMDfetch:        doCmdFetch(false, msg, result); break;
                case FVCMDfetchraw:     doCmdFetchRaw(false, msg, result); break;
                default:
                    throwError(FVERR_UnknownRemoteCommand);
                }
                msg.clear().append(result);
            }
            catch (IException * e)
            {
                serializeException(e, msg.clear());
                e->Release();
            }

            queryWorldCommunicator().reply(msg, MP_ASYNC_SEND);
            ::Release(sender);
        }
    }
    server = NULL;
    return 0;
}



void RemoteDataSourceServer::stop()
{
    alive = false;
    queryWorldCommunicator().cancel(0, MPTAG_FILEVIEW);
    join();
}

extern FILEVIEW_API void startRemoteDataSourceServer(const char * queue, const char * cluster)
{
    //This isn't properly thread safe - it also isn't ever used in practice, so not a problem.
    if (!server)
    {
        server = new RemoteDataSourceServer(queue, cluster);
        server->start();
    }
}

extern FILEVIEW_API void stopRemoteDataSourceServer()
{
    if (server)
        server->stop();
}


IConstWUResult * resolveResult(const char * wuid, unsigned sequence, const char * name)
{
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    Owned<IConstWorkUnit> wu = factory->openWorkUnit(wuid, false);
    return getWorkUnitResult(wu, name, sequence);
}

IConstWUResult * secResolveResult(ISecManager &secmgr, ISecUser &secuser, const char * wuid, unsigned sequence, const char * name)
{
    Owned<IWorkUnitFactory> factory = getSecWorkUnitFactory(secmgr, secuser);
    Owned<IConstWorkUnit> wu = factory->openWorkUnit(wuid, false);
    return (wu) ? getWorkUnitResult(wu, name, sequence) : NULL;
}
