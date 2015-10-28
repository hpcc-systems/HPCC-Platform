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

#ifndef ROXIEHELPER_HPP
#define ROXIEHELPER_HPP

#include "thorxmlwrite.hpp"
#include "roxiehelper.ipp"
#include "roxiemem.hpp"
#include "mpbase.hpp"
#include "workunit.hpp"
#include "thorhelper.hpp"

//========================================================================================= 

class THORHELPER_API HttpHelper : public CInterface
{
private:
    bool _isHttp;
    bool useEnvelope;
    StringAttr url;
    StringAttr authToken;
    StringAttr contentType;
    StringArray pathNodes;
    StringArray *validTargets;
    Owned<IProperties> parameters;
private:
    inline void setHttpHeaderValue(StringAttr &s, const char *v, bool ignoreExt)
    {
        if (!v || !*v)
            return;
        unsigned len=0;
        while (v[len] && v[len]!='\r' && (!ignoreExt || v[len]!=';'))
            len++;
        if (len)
            s.set(v, len);
    }
    void parseURL();

public:
    IMPLEMENT_IINTERFACE;
    HttpHelper(StringArray *_validTargets) : validTargets(_validTargets) { _isHttp = false; useEnvelope=true; parameters.setown(createProperties(true));}
    bool isHttp() { return _isHttp; }
    bool getUseEnvelope(){return useEnvelope;}
    void setUseEnvelope(bool _useEnvelope){useEnvelope=_useEnvelope;}
    bool getTrim() {return parameters->getPropBool(".trim", true); /*http currently defaults to true, maintain compatibility */}
    void setIsHttp(bool __isHttp) { _isHttp = __isHttp; }
    const char *queryAuthToken() { return authToken.str(); }
    const char *queryTarget() { return (pathNodes.length()) ? pathNodes.item(0) : NULL; }
    const char *queryQueryName() { return (pathNodes.length()>1) ? pathNodes.item(1) : NULL; }

    inline void setAuthToken(const char *v)
    {
        setHttpHeaderValue(authToken, v, false);
    };
    const char *queryContentType() { return contentType.str(); };
    inline void setContentType(const char *v)
    {
        setHttpHeaderValue(contentType, v, true);
    };
    inline void parseHTTPRequestLine(const char *v)
    {
        const char *end = strstr(v, " HTTP");
        if (end)
        {
            url.set(v, end - v);
            parseURL();
        }
    }
    TextMarkupFormat queryContentFormat()
    {
        if (!contentType.length())
        {
            if (pathNodes.length()>2 && strieq(pathNodes.item(2), "json"))
                contentType.set("application/json");
            else
                contentType.set("text/xml");
        }

        return (strieq(queryContentType(), "application/json")) ? MarkupFmt_JSON : MarkupFmt_XML;
    }
    IProperties *queryUrlParameters(){return parameters;}
    bool validateTarget(const char *target){return (validTargets) ? validTargets->contains(target) : false;}
};

//==============================================================================================================

typedef enum { heapSortAlgorithm, insertionSortAlgorithm,
              quickSortAlgorithm, stableQuickSortAlgorithm, spillingQuickSortAlgorithm, stableSpillingQuickSortAlgorithm,
              mergeSortAlgorithm, spillingMergeSortAlgorithm,
              parallelMergeSortAlgorithm, spillingParallelMergeSortAlgorithm,
              unknownSortAlgorithm } RoxieSortAlgorithm;

interface ISortAlgorithm : extends IInterface
{
    virtual void prepare(IInputBase *input) = 0;
    virtual const void *next() = 0;
    virtual void reset() = 0;
    virtual void getSortedGroup(ConstPointerArray & result) = 0;
};

extern THORHELPER_API ISortAlgorithm *createQuickSortAlgorithm(ICompare *_compare);
extern THORHELPER_API ISortAlgorithm *createStableQuickSortAlgorithm(ICompare *_compare);
extern THORHELPER_API ISortAlgorithm *createInsertionSortAlgorithm(ICompare *_compare, roxiemem::IRowManager *_rowManager, unsigned _activityId);
extern THORHELPER_API ISortAlgorithm *createHeapSortAlgorithm(ICompare *_compare);
extern THORHELPER_API ISortAlgorithm *createSpillingQuickSortAlgorithm(ICompare *_compare, roxiemem::IRowManager &_rowManager, IOutputMetaData * _rowMeta, ICodeContext *_ctx, const char *_tempDirectory, unsigned _activityId, bool _stable);
extern THORHELPER_API ISortAlgorithm *createMergeSortAlgorithm(ICompare *_compare);
extern THORHELPER_API ISortAlgorithm *createParallelMergeSortAlgorithm(ICompare *_compare);

extern THORHELPER_API ISortAlgorithm *createSortAlgorithm(RoxieSortAlgorithm algorithm, ICompare *_compare, roxiemem::IRowManager &_rowManager, IOutputMetaData * _rowMeta, ICodeContext *_ctx, const char *_tempDirectory, unsigned _activityId);

//=========================================================================================

interface IGroupedInput : extends IInterface, extends IInputBase
{
};

extern THORHELPER_API IGroupedInput *createGroupedInputReader(IInputBase *_input, const ICompare *_groupCompare);
extern THORHELPER_API IGroupedInput *createDegroupedInputReader(IInputBase *_input);
extern THORHELPER_API IGroupedInput *createSortedInputReader(IInputBase *_input, ISortAlgorithm *_sorter);
extern THORHELPER_API IGroupedInput *createSortedGroupedInputReader(IInputBase *_input, const ICompare *_groupCompare, ISortAlgorithm *_sorter);

//=========================================================================================

interface SafeSocket : extends IInterface
{
    virtual ISocket *querySocket()  = 0;
    virtual size32_t write(const void *buf, size32_t size, bool takeOwnership=false) = 0;
    virtual bool readBlock(MemoryBuffer &ret, unsigned maxBlockSize, unsigned timeout = (unsigned) WAIT_FOREVER) = 0;
    virtual bool readBlock(StringBuffer &ret, unsigned timeout, HttpHelper *pHttpHelper, bool &, bool &, unsigned maxBlockSize) = 0;
    virtual void checkSendHttpException(HttpHelper &httphelper, IException *E, const char *queryName) = 0;
    virtual void sendSoapException(IException *E, const char *queryName) = 0;
    virtual void sendJsonException(IException *E, const char *queryName) = 0;
    virtual void setHttpMode(const char *queryName, bool arrayMode, HttpHelper &httphelper) = 0;
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

class THORHELPER_API CSafeSocket : public CInterface, implements SafeSocket
{
protected:
    Linked<ISocket> sock;
    bool httpMode;
    bool heartbeat;
    TextMarkupFormat mlFmt;
    StringAttr contentHead;
    StringAttr contentTail;
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
    void setHttpMode(const char *queryName, bool arrayMode, HttpHelper &httphelper);
    void checkSendHttpException(HttpHelper &httphelper, IException *E, const char *queryName);
    void sendSoapException(IException *E, const char *queryName);
    void sendJsonException(IException *E, const char *queryName);

    void setHeartBeat();
    bool sendHeartBeat(const IContextLogger &logctx);
    void flush();
    unsigned bytesOut() const;
    bool checkConnection() const;
    void sendException(const char *source, unsigned code, const char *message, bool isBlocked, const IContextLogger &logctx);
};

//==============================================================================================================
class THORHELPER_API FlushingStringBuffer : extends CInterface, implements IXmlStreamFlusher, implements IInterface
{
    // MORE this code is yukky. Overdue for cleanup!
protected:
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
    TextMarkupFormat mlFmt;      // controls whether xml/json elements are output
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

    FlushingStringBuffer(SafeSocket *_sock, bool _isBlocked, TextMarkupFormat _mlFmt, bool _isRaw, bool _isHttp, const IContextLogger &_logctx);
    ~FlushingStringBuffer();
    virtual void append(char data) {append(1, &data);}
    virtual void append(const char *data);
    virtual void append(unsigned len, const char *data);
    virtual void append(double data);
    virtual void appendf(const char *format, ...) __attribute__((format(printf, 2, 3)));
    virtual void encodeString(const char *x, unsigned len, bool utf8=false);
    virtual void encodeData(const void *data, unsigned len);
    virtual void flushXML(StringBuffer &current, bool isClosing);
    virtual void flush(bool closing) ;
    virtual void addPayload(StringBuffer &s, unsigned int reserve=0);
    virtual void *getPayload(size32_t &length);
    virtual void startDataset(const char *elementName, const char *resultName, unsigned sequence, bool _extend = false, const IProperties *xmlns=NULL);
    virtual void startScalar(const char *resultName, unsigned sequence);
    virtual void setScalarInt(const char *resultName, unsigned sequence, __int64 value, unsigned size);
    virtual void setScalarUInt(const char *resultName, unsigned sequence, unsigned __int64 value, unsigned size);
    virtual void incrementRowCount();
};

class THORHELPER_API FlushingJsonBuffer : public FlushingStringBuffer
{
public:
    FlushingJsonBuffer(SafeSocket *_sock, bool _isBlocked, bool _isHttp, const IContextLogger &_logctx) :
        FlushingStringBuffer(_sock, _isBlocked, MarkupFmt_JSON, false, _isHttp, _logctx)
    {
    }

    void append(double data);
    void encodeString(const char *x, unsigned len, bool utf8=false);
    void encodeData(const void *data, unsigned len);
    void startDataset(const char *elementName, const char *resultName, unsigned sequence, bool _extend = false, const IProperties *xmlns=NULL);
    void startScalar(const char *resultName, unsigned sequence);
    virtual void setScalarInt(const char *resultName, unsigned sequence, __int64 value, unsigned size);
    virtual void setScalarUInt(const char *resultName, unsigned sequence, unsigned __int64 value, unsigned size);
};

inline const char *getFormatName(TextMarkupFormat fmt)
{
    if (fmt==MarkupFmt_XML)
        return "xml";
    if (fmt==MarkupFmt_JSON)
        return "json";
    return "raw";
}
//==============================================================================================================

class THORHELPER_API OwnedRowArray
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
class THORHELPER_API ClusterWriteHandler : public CInterface
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

THORHELPER_API StringBuffer & mangleHelperFileName(StringBuffer & out, const char * in, const char * wuid, unsigned int flags);
THORHELPER_API StringBuffer & mangleLocalTempFilename(StringBuffer & out, char const * in);
THORHELPER_API StringBuffer & expandLogicalFilename(StringBuffer & logicalName, const char * fname, IConstWorkUnit * wu, bool resolveLocally, bool ignoreForeignPrefix);

#endif // ROXIEHELPER_HPP
