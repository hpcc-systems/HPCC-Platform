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

#ifndef ROXIEHELPER_HPP
#define ROXIEHELPER_HPP

#include "thorxmlwrite.hpp"
#include "roxiehelper.ipp"
#include "roxiemem.hpp"
#include "mpbase.hpp"

#ifdef _WIN32
 #ifdef ROXIEHELPER_EXPORTS
  #define ROXIEHELPER_API __declspec(dllexport)
 #else
  #define ROXIEHELPER_API __declspec(dllimport)
 #endif
#else
 #define ROXIEHELPER_API
#endif

//#pragma message("**** ROXIEHELPER.HPP ***")

//========================================================================================= 

class ROXIEHELPER_API HttpHelper : public CInterface
{
private:
    bool _isHttp;
    StringBuffer authToken;
public:
    IMPLEMENT_IINTERFACE;
    HttpHelper() { _isHttp = false; };
    bool isHttp() { return _isHttp; };
    void setIsHttp(bool __isHttp) { _isHttp = __isHttp; };
    const char *queryAuthToken() { return authToken.str(); };
    void setAuthToken(const char *_authToken) { authToken.clear().append(_authToken); };
};

//========================================================================================= 

interface SafeSocket : extends IInterface
{
    virtual ISocket *querySocket()  = 0;
    virtual size32_t write(const void *buf, size32_t size, bool takeOwnership=false) = 0;
    virtual bool readBlock(MemoryBuffer &ret, unsigned maxBlockSize, unsigned timeout = (unsigned) WAIT_FOREVER) = 0;
    virtual bool readBlock(StringBuffer &ret, unsigned timeout, HttpHelper *pHttpHelper, bool &, bool &, unsigned maxBlockSize) = 0;
    virtual void setHttpMode(const char *queryName, bool arrayMode) = 0;
    virtual void setHeartBeat() = 0;
    virtual bool sendHeartBeat(const IContextLogger &logctx) = 0;
    virtual void flush() = 0;
    virtual unsigned bytesOut() const = 0;
    virtual bool checkConnection() const = 0;
    virtual void sendException(const char *source, unsigned code, const char *message, bool isBlocked, const IContextLogger &logctx) = 0;

    // TO be removed and replaced with better mechanism when SafeSocket merged with tht new output sequencer...
    // until then you may need to lock using this if you are making multiple calls and they need to stay together in the output
    virtual CriticalSection &queryCrit() = 0;
};

class ROXIEHELPER_API CSafeSocket : public CInterface, implements SafeSocket
{
protected:
    Linked<ISocket> sock;
    bool httpMode;
    bool heartbeat;
    StringBuffer xmlhead;
    StringBuffer xmltail;
    PointerArray queued;
    UnsignedArray lengths;
    unsigned sent;
    CriticalSection crit;

public:
    IMPLEMENT_IINTERFACE;
    CSafeSocket(ISocket *_sock);
    ~CSafeSocket();
    
    ISocket *querySocket() { return sock; }
    virtual CriticalSection &queryCrit() { return crit; };
    size32_t write(const void *buf, size32_t size, bool takeOwnership=false);
    bool readBlock(MemoryBuffer &ret, unsigned maxBlockSize, unsigned timeout = (unsigned) WAIT_FOREVER);
    bool readBlock(StringBuffer &ret, unsigned timeout, HttpHelper *pHttpHelper, bool &, bool &, unsigned maxBlockSize);
    void setHttpMode(const char *queryName, bool arrayMode);
    void setHeartBeat();
    bool sendHeartBeat(const IContextLogger &logctx);
    void flush();
    unsigned bytesOut() const;
    bool checkConnection() const;
    void sendException(const char *source, unsigned code, const char *message, bool isBlocked, const IContextLogger &logctx);
};

//==============================================================================================================
class ROXIEHELPER_API FlushingStringBuffer : extends CInterface, implements IXmlStreamFlusher, implements IInterface
{
    // MORE this code is yukky. Overdue for cleanup!

    SafeSocket *sock;
    StringBuffer name;
    StringBuffer tail;
    unsigned sequenceNumber;
    unsigned rowCount;
    unsigned emptyLength;
    const IContextLogger &logctx;
    CriticalSection crit;
    PointerArray queued;
    UnsignedArray lengths;

    bool needsFlush(bool closing);
    void startBlock();
public:
    bool isXml;      // controls whether xml elements are output
    bool isRaw;      // controls whether output as binary or ascii
    bool isBlocked;
    bool isHttp;
    bool isSoap;
    bool isEmpty;
    bool extend;
    bool trim;
    bool tagClosed;
    StringAttr queryName;
    StringBuffer s;

    IMPLEMENT_IINTERFACE;

    FlushingStringBuffer(SafeSocket *_sock, bool _isBlocked, bool _isXml, bool _isRaw, bool _isHttp, const IContextLogger &_logctx);
    ~FlushingStringBuffer();
    void append(char data) {append(1, &data);}
    void append(const char *data);
    void append(unsigned len, const char *data);
    void appendf(const char *format, ...) __attribute__((format(printf, 2, 3)));
    void encodeXML(const char *x, unsigned flags=0, unsigned len=(unsigned)-1, bool utf8=false);
    virtual void flushXML(StringBuffer &current, bool isClosing);
    void flush(bool closing) ;
    void *getPayload(size32_t &length);
    void startDataset(const char *elementName, const char *resultName, unsigned sequence, bool _extend = false);
    void startScalar(const char *resultName, unsigned sequence);
    void incrementRowCount();
};

//==============================================================================================================

class ROXIEHELPER_API OwnedRowArray
{
public:
    OwnedRowArray() {}
    ~OwnedRowArray() { clear(); }


    void clear();
    void clearPart(aindex_t from, aindex_t to);
    void replace(const void * row, aindex_t pos);
    void append(const void * row) { buff.append(row); }
    aindex_t ordinality() const { return buff.ordinality(); }
    const void * * getArray() { return buff.getArray(); }
    bool isItem(aindex_t pos) const { return buff.isItem(pos); }
    const void * item(aindex_t pos) { return buff.item(pos); }
    const void * itemClear(aindex_t pos) { const void * ret = buff.item(pos); buff.replace(NULL, pos); return ret; }

protected:
    ConstPointerArray buff;
};

//==============================================================================================================

interface IFileDescriptor;
interface IAgentContext;
class ROXIEHELPER_API ClusterWriteHandler : public CInterface
{
public:
    ClusterWriteHandler(char const * _logicalName, char const * _activityType);
    void addCluster(char const * cluster);
    void getLocalPhysicalFilename(StringAttr & out) const;
    void splitPhysicalFilename(StringBuffer & dir, StringBuffer & base) const;
    void copyPhysical(IFile * source, bool noCopy) const;
    void setDescriptorParts(IFileDescriptor * desc, char const * basename, IPropertyTree * attrs) const;
    void finish(IFile * file) const;
    void getClusters(StringArray &clusters) const;

private:
    virtual void getTempFilename(StringAttr & out) const ;

private:
    StringAttr logicalName;
    StringBuffer physicalName;
    StringBuffer physicalDir;
    StringBuffer physicalBase;
    StringAttr activityType;
    StringAttr localClusterName;
    Owned<IGroup> localCluster; 
    IArrayOf<IGroup> remoteNodes;
    StringArray remoteClusters;
};

//==============================================================================================================

#endif // ROXIEHELPER_HPP
