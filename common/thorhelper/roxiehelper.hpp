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

#include "rtlformat.hpp"
#include "thorherror.h"
#include "thorxmlwrite.hpp"
#include "roxiehelper.ipp"
#include "roxiemem.hpp"
#include "mpbase.hpp"
#include "workunit.hpp"
#include "thorhelper.hpp"

//========================================================================================= 

void parseHttpParameterString(IProperties *p, const char *str);

enum class HttpMethod {NONE, GET, POST};
enum class HttpCompression {NONE, GZIP, DEFLATE, ZLIB_DEFLATE};

class THORHELPER_API HttpHelper : public CInterface
{
private:
    HttpMethod method;
    bool useEnvelope = false;
    StringAttr version;
    StringAttr url;
    StringAttr authToken;
    StringAttr contentType;
    StringAttr queryName;
    StringArray pathNodes;
    StringArray *validTargets;
    Owned<IProperties> parameters;
    Owned<IProperties> form;
    Owned<IProperties> reqHeaders;
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
    HttpHelper(StringArray *_validTargets) : method(HttpMethod::NONE), validTargets(_validTargets)  {parameters.setown(createProperties(true));}
    inline bool isHttp() { return method!=HttpMethod::NONE; }
    inline bool isHttpGet(){ return method==HttpMethod::GET; }
    inline bool allowKeepAlive()
    {
        const char *connection = queryRequestHeader("Connection");
        if (!connection)
            return !streq(version, "1.0");
        return strieq(connection, "Keep-Alive");
    }
    inline bool isControlUrl()
    {
        const char *control = queryTarget();
        return (control && strieq(control, "control"));
    }
    inline HttpCompression getReqCompression()
    {
        const char *encoding = queryRequestHeader("Content-Encoding");
        if (encoding)
        {
            if (strieq(encoding, "gzip") || strieq(encoding, "x-gzip"))
                return HttpCompression::GZIP;
            if (strieq(encoding, "deflate") || strieq(encoding, "x-deflate"))
                return HttpCompression::DEFLATE;
        }
        return HttpCompression::NONE;
    }
    inline HttpCompression getRespCompression()
    {
        const char *encoding = queryRequestHeader("Accept-Encoding");
        if (encoding)
        {
            StringArray encodingList;
            encodingList.appendList(encoding, ",");
            if (encodingList.contains("gzip"))
                return HttpCompression::GZIP;
            if (encodingList.contains("deflate"))
                return HttpCompression::DEFLATE;
            //The reason gzip is preferred is that deflate can mean either of two formats
            //"x-deflate" isn't any clearer, but since either works either way, we can use the alternate name
            //to our advantage.  Differentiating here just gives us a way of allowing clients to specify
            //in case they can't handle one or the other (e.g. SOAPUI can't handle ZLIB_DEFLATE which I think
            //is the "most proper" choice)
            if (encodingList.contains("x-deflate"))
                return HttpCompression::ZLIB_DEFLATE;
        }
        return HttpCompression::NONE;
    }
    bool getUseEnvelope(){return useEnvelope;}
    void setUseEnvelope(bool _useEnvelope){useEnvelope=_useEnvelope;}
    bool getTrim() {return parameters->getPropBool(".trim", true); /*http currently defaults to true, maintain compatibility */}
    void setHttpMethod(HttpMethod _method) { method = _method; }
    const char *queryAuthToken() { return authToken.str(); }
    const char *queryTarget() { return (pathNodes.length()) ? pathNodes.item(0) : NULL; }
    const char *queryQueryName()
    {
        if (!queryName.isEmpty())
            return queryName.str();
        if (!pathNodes.isItem(1))
            return nullptr;
        const char *name = pathNodes.item(1);
        const char *at = strchr(name, ';');
        if (!at)
            queryName.set(name);
        else
            queryName.set(name, at-name-1);
        return queryName.str();
    }

    inline const char *queryRequestHeader(const char *header)
    {
        if (!reqHeaders)
            return nullptr;
        return reqHeaders->queryProp(header);
    }
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
            v=end+5;
            if (*v=='/')
            {
                end=strstr(++v, "\r\n");
                if (end)
                    version.set(v, end-v);
            }
        }
    }
    void parseRequestHeaders(const char *headers);
    inline bool isFormPost()
    {
        return (strnicmp(queryContentType(), "application/x-www-form-urlencoded", strlen("application/x-www-form-urlencoded"))==0);
    }
    TextMarkupFormat getUrlResponseFormat()
    {
        if (pathNodes.length()>2 && strieq(pathNodes.item(2), "json"))
            return MarkupFmt_JSON;
        return MarkupFmt_XML;
    }
    TextMarkupFormat getContentTypeMlFormat()
    {
        if (!contentType.length())
        {
            TextMarkupFormat fmt = getUrlResponseFormat();
            if (fmt == MarkupFmt_JSON)
                contentType.set("application/json");
            else
                contentType.set("text/xml");
            return fmt;
        }

        return (strieq(queryContentType(), "application/json")) ? MarkupFmt_JSON : MarkupFmt_XML;
    }
    TextMarkupFormat queryResponseMlFormat()
    {
        if (isFormPost())
            return getUrlResponseFormat();
        return getContentTypeMlFormat();
    }
    TextMarkupFormat queryRequestMlFormat()
    {
        if (isHttpGet() || isFormPost())
            return MarkupFmt_URL;
        return getContentTypeMlFormat();
    }
    IProperties *queryUrlParameters(){return parameters;}
    bool validateTarget(const char *target)
    {
        if (!target)
            return false;
        if (validTargets && validTargets->contains(target))
            return true;
        if (strieq(target, "control") && (isHttpGet() || isFormPost()))
            return true;
        return false;
    }
    inline void checkTarget()
    {
        const char *target = queryTarget();
        if (!target || !*target)
            throw MakeStringException(THORHELPER_DATA_ERROR, "HTTP-GET Target not specified");
        else if (!validateTarget(target))
            throw MakeStringException(THORHELPER_DATA_ERROR, "HTTP-GET Target not found");
    }

    inline void checkSetFormPostContent(const char *content)
    {
        while ((*content==' ') || (*content=='\t'))
            content++;
        if (*content=='<')
        {
            contentType.set("text/xml"); //backward compatible.  Some clients have a bug where XML is sent as "form-urlenncoded"
            return;
        }
        checkTarget();
        if (!form)
            form.setown(createProperties(false));
        parseHttpParameterString(form, content);
    }
    IPropertyTree *createPTreeFromParameters(byte flags)
    {
        if (!pathNodes.isItem(1))
            throw MakeStringException(THORHELPER_DATA_ERROR, "HTTP-GET Query not specified");
        StringBuffer query;
        appendDecodedURL(query, pathNodes.item(1));
        aindex_t count = pathNodes.ordinality();
        if (count>2)
            for (aindex_t x = 2; x<count; ++x)
                appendDecodedURL(query.append('/'), pathNodes.item(x));
        return createPTreeFromHttpParameters(query, form ? form : parameters, true, false, (ipt_flags) flags);
    }
    bool isMappedToInputParameter()
    {
        if (isHttp())
        {
            aindex_t count = pathNodes.ordinality();
            if (count>2)
                for (aindex_t x = 2; x<count; ++x)
                    if (strncmp(pathNodes.item(x), "input(", 6)==0)
                        return true;
        }
        return false;
    }
    IPropertyTree *checkAddWrapperForAdaptiveInput(IPropertyTree *content, byte flags)
    {
        if (!isMappedToInputParameter())
            return content;
        if (!pathNodes.isItem(1))
            throw MakeStringException(THORHELPER_DATA_ERROR, "HTTP-GET Query not specified");
        StringBuffer query;
        appendDecodedURL(query, pathNodes.item(1));
        aindex_t count = pathNodes.ordinality();
        if (count>2)
            for (aindex_t x = 2; x<count; ++x)
                appendDecodedURL(query.append('/'), pathNodes.item(x));
        return createPTreeFromHttpPath(query, content, false, (ipt_flags) flags);
    }
    void getResultFilterAndTag(StringAttr &filter, StringAttr &tag)
    {
        if (!isHttp())
            return;
        aindex_t count = pathNodes.ordinality();
        if (count<=2)
            return;
        StringBuffer temp;
        for (aindex_t x = 2; x<count; ++x)
        {
            if (strncmp(pathNodes.item(x), "result(", 6)==0)
                checkParseUrlPathNodeValue(pathNodes.item(x), temp, filter);
            else if (strncmp(pathNodes.item(x), "tag(", 4)==0)
                checkParseUrlPathNodeValue(pathNodes.item(x), temp, tag);
        }
    }
};

//==============================================================================================================

//MORE: This should just contain the algorithm, and use a separate field for stable|spilling|parallel
//Should be implemented in a subsequent pull request, which also uses ALGORITHM('x') instead of requiring STABLE/UNSTABLE
typedef enum {
    heapSortAlgorithm,                  // heap sort
    insertionSortAlgorithm,             // insertion sort - purely for comparison (no longer supported)
    quickSortAlgorithm,                 // jlib implementation of quicksort
    stableQuickSortAlgorithm,           // jlib version of quick sort that uses an extra array indirect to ensure it is stable
    spillingQuickSortAlgorithm,         // quickSortAlgorithm with the ability to spill
    stableSpillingQuickSortAlgorithm,   // stableQuickSortAlgorithm with the ability to spill
    mergeSortAlgorithm,                 // stable merge sort
    spillingMergeSortAlgorithm,         // stable merge sort that can spill to disk
    parallelMergeSortAlgorithm,         // parallel version of stable merge sort
    spillingParallelMergeSortAlgorithm, // parallel version of stable merge sort that can spill to disk
    tbbQuickSortAlgorithm,              // (parallel) quick sort implemented by the TBB libraries
    tbbStableQuickSortAlgorithm,        // stable version of tbbQuickSortAlgorithm
    parallelQuickSortAlgorithm,         // parallel version of the internal quicksort implementation (for comparison)
    parallelStableQuickSortAlgorithm,   // stable version of parallelQuickSortAlgorithm
    unknownSortAlgorithm
} RoxieSortAlgorithm;

interface ISortAlgorithm : extends IInterface
{
    virtual void prepare(IEngineRowStream *input) = 0;
    virtual const void *next() = 0;
    virtual void reset() = 0;
    virtual void getSortedGroup(ConstPointerArray & result) = 0;
    virtual cycle_t getElapsedCycles(bool reset) = 0;
};

extern THORHELPER_API ISortAlgorithm *createQuickSortAlgorithm(ICompare *_compare);
extern THORHELPER_API ISortAlgorithm *createStableQuickSortAlgorithm(ICompare *_compare);
extern THORHELPER_API ISortAlgorithm *createParallelQuickSortAlgorithm(ICompare *_compare);
extern THORHELPER_API ISortAlgorithm *createParallelStableQuickSortAlgorithm(ICompare *_compare);
extern THORHELPER_API ISortAlgorithm *createHeapSortAlgorithm(ICompare *_compare);
extern THORHELPER_API ISortAlgorithm *createSpillingQuickSortAlgorithm(ICompare *_compare, roxiemem::IRowManager &_rowManager, IOutputMetaData * _rowMeta, ICodeContext *_ctx, const char *_tempDirectory, unsigned _activityId, bool _stable);
extern THORHELPER_API ISortAlgorithm *createMergeSortAlgorithm(ICompare *_compare);
extern THORHELPER_API ISortAlgorithm *createParallelMergeSortAlgorithm(ICompare *_compare);
extern THORHELPER_API ISortAlgorithm *createTbbQuickSortAlgorithm(ICompare *_compare);
extern THORHELPER_API ISortAlgorithm *createTbbStableQuickSortAlgorithm(ICompare *_compare);

extern THORHELPER_API ISortAlgorithm *createSortAlgorithm(RoxieSortAlgorithm algorithm, ICompare *_compare, roxiemem::IRowManager &_rowManager, IOutputMetaData * _rowMeta, ICodeContext *_ctx, const char *_tempDirectory, unsigned _activityId);

//=========================================================================================

interface IGroupedInput : extends IEngineRowStream  // MORE rename to IGroupedRowStream
{
};

extern THORHELPER_API IGroupedInput *createGroupedInputReader(IEngineRowStream *_input, const ICompare *_groupCompare);
extern THORHELPER_API IGroupedInput *createDegroupedInputReader(IEngineRowStream *_input);
extern THORHELPER_API IGroupedInput *createSortedInputReader(IEngineRowStream *_input, ISortAlgorithm *_sorter);
extern THORHELPER_API IGroupedInput *createSortedGroupedInputReader(IEngineRowStream *_input, const ICompare *_groupCompare, ISortAlgorithm *_sorter);

//=========================================================================================

interface SafeSocket : extends IInterface
{
    virtual ISocket *querySocket()  = 0;
    virtual size32_t write(const void *buf, size32_t size, bool takeOwnership=false) = 0;
    virtual bool readBlocktms(MemoryBuffer &ret, unsigned timeoutms, unsigned maxBlockSize) = 0;
    virtual bool readBlocktms(StringBuffer &ret, unsigned timeoutms, HttpHelper *pHttpHelper, bool &, bool &, unsigned maxBlockSize) = 0;
    virtual void checkSendHttpException(HttpHelper &httphelper, IException *E, const char *queryName) = 0;
    virtual void sendSoapException(IException *E, const char *queryName) = 0;
    virtual void sendJsonException(IException *E, const char *queryName) = 0;
    virtual void setHttpMode(const char *queryName, bool arrayMode, HttpHelper &httphelper) = 0;
    virtual void setHttpMode(bool mode) = 0;
    virtual void setHttpKeepAlive(bool val) = 0;
    virtual void setHeartBeat() = 0;
    virtual bool sendHeartBeat(const IContextLogger &logctx) = 0;
    virtual void flush() = 0;
    virtual unsigned bytesOut() const = 0;
    virtual bool checkConnection() const = 0;
    virtual void sendException(const char *source, unsigned code, const char *message, bool isBlocked, const IContextLogger &logctx) = 0;

    // To be removed and replaced with better mechanism when SafeSocket merged with the new output sequencer...
    // until then you may need to lock using this if you are making multiple calls and they need to stay together in the output
    virtual CriticalSection &queryCrit() = 0;

    virtual void setAdaptiveRoot(bool adaptive)=0;
    virtual bool getAdaptiveRoot()=0;
};

class THORHELPER_API CSafeSocket : implements SafeSocket, public CInterface
{
protected:
    Linked<ISocket> sock;
    bool httpMode;
    bool httpKeepAlive = false;
    bool heartbeat;
    bool adaptiveRoot = false;
    TextMarkupFormat mlResponseFmt = MarkupFmt_Unknown;
    HttpCompression respCompression = HttpCompression::NONE;
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
    bool readBlocktms(MemoryBuffer &ret, unsigned timeoutms, unsigned maxBlockSize);
    bool readBlocktms(StringBuffer &ret, unsigned timeoutms, HttpHelper *pHttpHelper, bool &, bool &, unsigned maxBlockSize);
    void setHttpMode(const char *queryName, bool arrayMode, HttpHelper &httphelper);
    void setHttpMode(bool mode) override {httpMode = mode;}
    virtual void setHttpKeepAlive(bool val) { httpKeepAlive = val; }
    void setAdaptiveRoot(bool adaptive){adaptiveRoot=adaptive;}
    bool getAdaptiveRoot(){return adaptiveRoot;}
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
    bool first = true;

    bool needsFlush(bool closing);
public:
    TextMarkupFormat mlFmt;      // controls whether xml/json elements are output
    bool isRaw;      // controls whether output as binary or ascii
    bool isBlocked;
    bool isHttp;
    bool isSoap;
    bool isEmpty;
    bool extend;
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
    virtual void flushXML(StringBuffer &current, bool isClosing)
    {
        flushXML(current, isClosing, nullptr);
    }
    void flushXML(StringBuffer &current, bool isClosing, const char *delim);
    virtual void flush(bool closing) ;
    virtual void addPayload(StringBuffer &s, unsigned int reserve=0);
    virtual void *getPayload(size32_t &length);
    virtual void startBlock();
    virtual void startDataset(const char *elementName, const char *resultName, unsigned sequence, bool _extend = false, const IProperties *xmlns=NULL, bool adaptive=false);
    virtual void startScalar(const char *resultName, unsigned sequence, bool simpleTag=false, const char *simplename=nullptr);
    virtual void setScalarInt(const char *resultName, unsigned sequence, __int64 value, unsigned size);
    virtual void setScalarUInt(const char *resultName, unsigned sequence, unsigned __int64 value, unsigned size);
    virtual void incrementRowCount();
    void setTail(const char *value){tail.set(value);}
    const char *queryResultName(){return name;}
};

class THORHELPER_API FlushingJsonBuffer : public FlushingStringBuffer
{
protected:
    bool extend;

public:
    FlushingJsonBuffer(SafeSocket *_sock, bool _isBlocked, bool _isHttp, const IContextLogger &_logctx, bool _extend = false) :
        FlushingStringBuffer(_sock, _isBlocked, MarkupFmt_JSON, false, _isHttp, _logctx), extend(_extend)
    {
    }

    void append(double data);
    void encodeString(const char *x, unsigned len, bool utf8=false);
    void encodeData(const void *data, unsigned len);
    void startDataset(const char *elementName, const char *resultName, unsigned sequence, bool _extend = false, const IProperties *xmlns=NULL, bool adaptive=false);
    void startScalar(const char *resultName, unsigned sequence, bool simpleTag, const char *simplename=nullptr);
    virtual void setScalarInt(const char *resultName, unsigned sequence, __int64 value, unsigned size, bool simpleTag = false, const char *simplename=nullptr);
    virtual void setScalarUInt(const char *resultName, unsigned sequence, unsigned __int64 value, unsigned size, bool simpleTag = false, const char *simplename=nullptr);
    virtual void flushXML(StringBuffer &current, bool isClosing)
    {
        FlushingStringBuffer::flushXML(current, isClosing, (extend) ? "," : nullptr);
    }
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
    void getPhysicalName(StringBuffer & name, const char * cluster) const;
    virtual void getTempFilename(StringAttr & out) const;

private:
    StringAttr logicalName;
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

THORHELPER_API ISectionTimer * queryNullSectionTimer();

#endif // ROXIEHELPER_HPP
