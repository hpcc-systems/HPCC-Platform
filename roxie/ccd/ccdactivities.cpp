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

#include "platform.h"
#include "jlib.hpp"

#include "ccd.hpp"
#include "ccdquery.hpp"
#include "ccdstate.hpp"
#include "ccdserver.hpp"
#include "ccdcontext.hpp"
#include "ccddebug.hpp"
#include "ccdactivities.hpp"
#include "ccdqueue.ipp"
#include "ccdsnmp.hpp"
#include "ccdfile.hpp"
#include "ccdkey.hpp"
#include "rtlkey.hpp"
#include "eclrtl_imp.hpp"
#include "rtlread_imp.hpp"
#include "rtlcommon.hpp"
#include "rtldynfield.hpp"

#include "jhtree.hpp"
#include "jlog.hpp"
#include "jmisc.hpp"
#include "udplib.hpp"
#include "csvsplitter.hpp"
#include "thorxmlread.hpp"
#include "thorcommon.ipp"
#include "thorstrand.hpp"
#include "jstats.h"

size32_t diskReadBufferSize = 0x10000;

using roxiemem::OwnedRoxieRow;
using roxiemem::OwnedConstRoxieRow;
using roxiemem::OwnedRoxieString;
using roxiemem::IRowManager;

#define maxContinuationSize 48000 // note - must fit in the 2-byte length field... but also needs to be possible to send back from Roxie server->slave in one packet

size32_t serializeRow(IOutputRowSerializer * serializer, IMessagePacker *output, const void *unserialized)
{
    CSizingSerializer sizer;
    serializer->serialize(sizer, (const byte *) unserialized);
    unsigned serializedLength = sizer.size();
    void *udpBuffer = output->getBuffer(serializedLength, true);
    CRawRowSerializer memSerializer(serializedLength, (byte *) udpBuffer);
    serializer->serialize(memSerializer, (const byte *) unserialized);
    assertex(memSerializer.size() == serializedLength);
    output->putBuffer(udpBuffer, serializedLength, true);
    return serializedLength;
}

inline void appendBuffer(IMessagePacker * output, size32_t size, const void * data, bool isVariable)
{
    void *recBuffer = output->getBuffer(size, isVariable);
    memcpy(recBuffer, data, size);
    output->putBuffer(recBuffer, size, isVariable);
}

extern void putStatsValue(IPropertyTree *node, const char *statName, const char *statType, unsigned __int64 val)
{
    if (val)
    {
        StringBuffer xpath;
        xpath.appendf("att[@name='%s']", statName);
        IPropertyTree *att = node->queryPropTree(xpath.str());
        if (!att)
        {
            att = node->addPropTree("att");
            att->setProp("@name", statName);
        }
        att->setProp("@type", statType);
        att->setPropInt64("@value", val);
    }
}

extern void putStatsValue(StringBuffer &reply, const char *statName, const char *statType, unsigned __int64 val)
{
    if (val)
    {
        reply.appendf("   <att name='%s' type='%s' value='%" I64F "d'/>\n", statName, statType, val);
    }
}

CActivityFactory::CActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode)
  : id(_id),
    subgraphId(_subgraphId),
    queryFactory(_queryFactory),
    helperFactory(_helperFactory),
    kind(_kind),
    mystats(allStatistics)  // We COULD cut down this list but it would complicate the structure, and we do actually track more in the factory than in the activity
{
    if (helperFactory)
    {
        Owned<IHThorArg> helper = helperFactory();
        meta.set(helper->queryOutputMeta());
    }
    const char *recordTranslationModeHintText = _graphNode.queryProp("hint[@name='layoutTranslation']/@value");
    if (recordTranslationModeHintText)
        recordTranslationModeHint = getTranslationMode(recordTranslationModeHintText);
}

void CActivityFactory::addChildQuery(unsigned id, ActivityArray *childQuery) 
{
    childQueries.append(*childQuery);
    childQueryIndexes.append(id);
}

ActivityArray *CActivityFactory::queryChildQuery(unsigned idx, unsigned &id)
{
    if (childQueries.isItem(idx))
    {
        id = childQueryIndexes.item(idx);
        return &childQueries.item(idx);
    }
    id = 0;
    return NULL;
}


class CSlaveActivityFactory : public CActivityFactory, implements ISlaveActivityFactory
{

public:
    IMPLEMENT_IINTERFACE

    CSlaveActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory) 
        : CActivityFactory(_graphNode.getPropInt("@id", 0), _subgraphId, _queryFactory, _helperFactory, getActivityKind(_graphNode), _graphNode)
    {
    }

    virtual IQueryFactory &queryQueryFactory() const
    {
        return CActivityFactory::queryQueryFactory();
    }

    void addChildQuery(unsigned id, ActivityArray *childQuery)
    {
        CActivityFactory::addChildQuery(id, childQuery);
    }

    StringBuffer &toString(StringBuffer &ret) const
    {
        return ret.appendf("%p", this);
    }

    const char *queryQueryName() const
    {
        return queryFactory.queryQueryName();
    }

    virtual ActivityArray *queryChildQuery(unsigned idx, unsigned &id)
    {
        return CActivityFactory::queryChildQuery(idx, id);
    }

    virtual void mergeStats(const CRuntimeStatisticCollection &from) const
    {
        CActivityFactory::mergeStats(from);
    }

    virtual unsigned queryId() const
    {
        return CActivityFactory::queryId();
    }

    virtual ThorActivityKind getKind() const 
    {
        return CActivityFactory::getKind();
    }

    virtual void getEdgeProgressInfo(unsigned idx, IPropertyTree &edge) const
    {
        CActivityFactory::getEdgeProgressInfo(idx, edge);
    }
    virtual void getNodeProgressInfo(IPropertyTree &node) const
    {
        CActivityFactory::getNodeProgressInfo(node);
    }
    virtual void resetNodeProgressInfo()
    {
        CActivityFactory::resetNodeProgressInfo();
    }
    virtual void getActivityMetrics(StringBuffer &reply) const
    {
        CActivityFactory::getActivityMetrics(reply);
    }
    IRoxieSlaveContext *createSlaveContext(const SlaveContextLogger &logctx, IRoxieQueryPacket *packet) const
    {
        return queryFactory.createSlaveContext(logctx, packet, childQueries.length()!=0);
    }
    virtual void getXrefInfo(IPropertyTree &reply, const IRoxieContextLogger &logctx) const
    {
        if (datafile)
            addXrefFileInfo(reply, datafile);
    }
    void createChildQueries(IRoxieSlaveContext *ctx, IArrayOf<IActivityGraph> &childGraphs, IHThorArg *colocalArg, IProbeManager *_probeManager, IRoxieSlaveContext *queryContext, const SlaveContextLogger &logctx) const
    {
        if (childQueries.length())
        {
            ForEachItemIn(idx, childQueries)
            {
                if (!_probeManager) // MORE - the probeAllRows is a hack!
                    _probeManager = queryContext->queryProbeManager();
                IActivityGraph *childGraph = createActivityGraph(ctx, NULL, childQueryIndexes.item(idx), childQueries.item(idx), NULL, _probeManager, logctx, 1); // MORE - the parent is wrong!
                childGraphs.append(*childGraph);
                queryContext->noteChildGraph(childQueryIndexes.item(idx), childGraph);
                childGraph->onCreate(colocalArg);             //NB: onCreate() on helper for activities in child graph are delayed, otherwise this would go wrong.
            }
        }
    }

    Owned<const IResolvedFile> datafile;

protected:

    static IPropertyTree *queryStatsNode(IPropertyTree *parent, const char *xpath)
    {
        StringBuffer levelx;
        const char *sep = strchr(xpath, '/');
        if (sep)
            levelx.append(sep-xpath, xpath);
        else
            levelx.append(xpath);
        IPropertyTree *child = parent->queryPropTree(levelx);
        if (!child)
        {
            const char *id = strchr(levelx, '[');
            if (!id)
            {
                child = parent->addPropTree(levelx);
            }
            else
            {
                StringBuffer elem;
                elem.append(id-levelx, levelx);
                child = parent->addPropTree(elem);
                for (;;)
                {
                    StringBuffer attr, val;
                    id++;
                    while (*id != '=')
                        attr.append(*id++);
                    id++;
                    char qu = *id++;
                    while (*id != qu)
                        val.append(*id++);
                    child->setProp(attr, val);
                    id++;
                    if (*id == ']')
                    {
                        if (id[1]!='[')
                            break;
                        id++;
                    }
                    else
                        throwUnexpected();
                }
            }
        }
        if (sep)
            return queryStatsNode(child, sep+1);
        else
            return child;
    }
};

//================================================================================================

class CRoxieSlaveActivity : implements CInterfaceOf<IRoxieSlaveActivity>, implements ICodeContext
{
protected:
    SlaveContextLogger &logctx;
    Linked<IRoxieQueryPacket> packet;
    mutable Owned<IRoxieSlaveContext> queryContext; // bit of a hack but easier than changing the ICodeContext callback interface to remove const
    const CSlaveActivityFactory *basefactory;
    IArrayOf<IActivityGraph> childGraphs;
    IHThorArg *basehelper;
    PartNoType lastPartNo;
    MemoryBuffer serializedCreate;
    MemoryBuffer resentInfo;
    CachedOutputMetaData meta;
    Owned<IOutputRowSerializer> serializer;
    Owned<IEngineRowAllocator> rowAllocator;
#ifdef _DEBUG
    Owned<IProbeManager> probeManager;
#endif
    bool aborted;
    bool resent;
    bool isOpt;
    bool variableFileName;
    RecordTranslationMode allowFieldTranslation;
    Owned<const IResolvedFile> varFileInfo;

    virtual void setPartNo(bool filechanged) = 0;

    inline void checkPartChanged(PartNoType &newPart)
    {
        if (newPart.partNo!=lastPartNo.partNo || newPart.fileNo!=lastPartNo.fileNo)
        {
            lastPartNo.partNo = newPart.partNo;
            bool filechanged = lastPartNo.fileNo != newPart.fileNo;
            lastPartNo.fileNo = newPart.fileNo;
            setPartNo(filechanged);
        }
    }

    virtual bool needsRowAllocator()
    {
        return meta.needsSerializeDisk() || meta.isVariableSize();
    }

    virtual void onCreate()
    {
        queryContext.setown(basefactory->createSlaveContext(logctx, packet));
#ifdef _DEBUG
        // MORE - need to consider debugging....
        if (probeAllRows)
            probeManager.setown(createProbeManager());
        basefactory->createChildQueries(queryContext, childGraphs, basehelper, probeManager, queryContext, logctx);
#else
        basefactory->createChildQueries(queryContext, childGraphs, basehelper, NULL, queryContext, logctx);
#endif
        if (meta.needsSerializeDisk())
            serializer.setown(meta.createDiskSerializer(queryContext->queryCodeContext(), basefactory->queryId()));
        if (needsRowAllocator())
            rowAllocator.setown(getRowAllocator(meta.queryOriginal(), basefactory->queryId()));
        unsigned parentExtractSize;
        serializedCreate.read(parentExtractSize);
        const byte * parentExtract = serializedCreate.readDirect(parentExtractSize);
        basehelper->onCreate(this, NULL, &serializedCreate);
        basehelper->onStart(parentExtract, &serializedCreate);
        deserializeExtra(serializedCreate);
        if (variableFileName) // note - in keyed join with dependent index case, kj itself won't have variableFileName but indexread might
        {
            CDateTime cacheDate(serializedCreate);
            unsigned checksum;
            serializedCreate.read(checksum);
            OwnedRoxieString fname(queryDynamicFileName());
            varFileInfo.setown(querySlaveDynamicFileCache()->lookupDynamicFile(logctx, fname, cacheDate, checksum, &packet->queryHeader(), isOpt, true));
            setVariableFileInfo();
        }
    }

    virtual void deserializeExtra(MemoryBuffer &out)
    {
    }

    CRoxieSlaveActivity(SlaveContextLogger &_logctx, IRoxieQueryPacket *_packet, HelperFactory *_hFactory, const CSlaveActivityFactory *_factory)
        : logctx(_logctx), packet(_packet), basefactory(_factory)
    {
        allowFieldTranslation = _factory->getEnableFieldTranslation();
        resent = packet->getContinuationLength() != 0;
        serializedCreate.setBuffer(packet->getContextLength(), (void *) packet->queryContextData(), false);
        if (resent)
            resentInfo.setBuffer(packet->getContinuationLength(), (void *) packet->queryContinuationData(), false);
        basehelper = _hFactory();
        lastPartNo.partNo = 0xffff;
        lastPartNo.fileNo = 0xffff;
        aborted = false;
        isOpt = false;
        variableFileName = false;
        meta.set(basehelper->queryOutputMeta());
    }

    ~CRoxieSlaveActivity()
    {
        ::Release(basehelper);
    }

    virtual void beforeDispose() override
    {
        CRuntimeStatisticCollection merged(allStatistics);
        logctx.gatherStats(merged);
        if (defaultCollectFactoryStatistics)
            basefactory->mergeStats(merged);
    }

public:
    IMPLEMENT_IINTERFACE_USING(CInterfaceOf<IRoxieSlaveActivity>)

    virtual const char *queryDynamicFileName() const = 0;
    virtual void setVariableFileInfo() = 0;
    virtual IIndexReadActivityInfo *queryIndexReadActivity() { throwUnexpected(); } // should only be called for index activity

    virtual unsigned queryId()
    {
        return basefactory->queryId();
    }

    virtual bool check()
    {
        Owned<IMessagePacker> output = ROQ->createOutputStream(packet->queryHeader(), false, logctx);
        doCheck(output);
        output->flush(true);
        return true;
    }

    virtual void doCheck(IMessagePacker *output) 
    {
        // MORE - unsophisticated default - if this approach seems fruitful then we can add something more thorough
        void *recBuffer = output->getBuffer(sizeof(bool), false);
        bool ret = false;
        memcpy(recBuffer, &ret, sizeof(bool));
    }

    virtual void abort() 
    {
        if (logctx.queryTraceLevel() > 2)
        {
            StringBuffer s;
            logctx.CTXLOG("Aborting running activity: %s", packet->queryHeader().toString(s).str());
        }
        aborted = true;
        logctx.abort();
        if (queryContext)
        {
            Owned<IException> E = MakeStringException(ROXIE_ABORT_ERROR, "Roxie server requested abort for running activity");
            queryContext->notifyAbort(E);
        }
    }

    virtual IRoxieQueryPacket *queryPacket() const
    {
        return packet;
    }

    void limitExceeded(bool keyed = false)
    {
        RoxiePacketHeader &header = packet->queryHeader();
        StringBuffer s;
        logctx.CTXLOG("%sLIMIT EXCEEDED: %s", keyed ? "KEYED " : "", header.toString(s).str());
        header.activityId = keyed ? ROXIE_KEYEDLIMIT_EXCEEDED : ROXIE_LIMIT_EXCEEDED;
        Owned<IMessagePacker> output = ROQ->createOutputStream(header, false, logctx);
        output->flush(true);
        aborted = true;
        logctx.abort();
    }

    virtual IThorChildGraph * resolveChildQuery(__int64 activityId, IHThorArg * colocal)
    {
        assertex(colocal == basehelper);
        return queryContext->queryCodeContext()->resolveChildQuery(activityId, colocal);
    }

    size32_t serializeRow(IMessagePacker *output, const void *unserialized) const
    {
        return ::serializeRow(serializer, output, unserialized);
    }

    virtual const char *loadResource(unsigned id) 
    { 
        return queryContext->queryCodeContext()->loadResource(id);
    }

    // Sets should not happen - they can only happen in the main Roxie server context

    virtual void setResultBool(const char *name, unsigned sequence, bool value) { throwUnexpected(); }
    virtual void setResultData(const char *name, unsigned sequence, int len, const void * data) { throwUnexpected(); }
    virtual void setResultDecimal(const char * stepname, unsigned sequence, int len, int precision, bool isSigned, const void *val) { throwUnexpected(); } 
    virtual void setResultInt(const char *name, unsigned sequence, __int64 value, unsigned size) { throwUnexpected(); }
    virtual void setResultRaw(const char *name, unsigned sequence, int len, const void * data) { throwUnexpected(); }
    virtual void setResultReal(const char * stepname, unsigned sequence, double value) { throwUnexpected(); }
    virtual void setResultSet(const char *name, unsigned sequence, bool isAll, size32_t len, const void * data, ISetToXmlTransformer * transformer) { throwUnexpected(); }
    virtual void setResultString(const char *name, unsigned sequence, int len, const char * str) { throwUnexpected(); }
    virtual void setResultUInt(const char *name, unsigned sequence, unsigned __int64 value, unsigned size) { throwUnexpected(); }
    virtual void setResultUnicode(const char *name, unsigned sequence, int len, UChar const * str) { throwUnexpected(); }
    virtual void setResultVarString(const char * name, unsigned sequence, const char * value) { throwUnexpected(); }
    virtual void setResultVarUnicode(const char * name, unsigned sequence, UChar const * value) { throwUnexpected(); }

    // Some gets are allowed though (e.g. for ONCE values)

    virtual bool getResultBool(const char * name, unsigned sequence)
    {
        return queryContext->queryCodeContext()->getResultBool(name, sequence);
    }
    virtual void getResultData(unsigned & tlen, void * & tgt, const char * name, unsigned sequence)
    {
        queryContext->queryCodeContext()->getResultData(tlen, tgt, name, sequence);
    }
    virtual void getResultDecimal(unsigned tlen, int precision, bool isSigned, void * tgt, const char * stepname, unsigned sequence)
    {
        queryContext->queryCodeContext()->getResultDecimal(tlen, precision, isSigned, tgt, stepname, sequence);
    }
    virtual void getResultRaw(unsigned & tlen, void * & tgt, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer)
    {
        queryContext->queryCodeContext()->getResultRaw(tlen, tgt, name, sequence, xmlTransformer, csvTransformer);
    }
    virtual void getResultSet(bool & isAll, size32_t & tlen, void * & tgt, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer)
    {
        queryContext->queryCodeContext()->getResultSet(isAll, tlen, tgt, name, sequence, xmlTransformer, csvTransformer);
    }
    virtual __int64 getResultInt(const char * name, unsigned sequence)
    {
        return queryContext->queryCodeContext()->getResultInt(name, sequence);
    }
    virtual double getResultReal(const char * name, unsigned sequence)
    {
        return queryContext->queryCodeContext()->getResultReal(name, sequence);
    }
    virtual void getResultString(unsigned & tlen, char * & tgt, const char * name, unsigned sequence)
    {
        queryContext->queryCodeContext()->getResultString(tlen, tgt, name, sequence);
    }
    virtual void getResultStringF(unsigned tlen, char * tgt, const char * name, unsigned sequence)
    {
        queryContext->queryCodeContext()->getResultStringF(tlen, tgt, name, sequence);
    }
    virtual void getResultUnicode(unsigned & tlen, UChar * & tgt, const char * name, unsigned sequence)
    {
        queryContext->queryCodeContext()->getResultUnicode(tlen, tgt, name, sequence);
    }
    virtual char *getResultVarString(const char * name, unsigned sequence)
    {
        return queryContext->queryCodeContext()->getResultVarString(name, sequence);
    }
    virtual UChar *getResultVarUnicode(const char * name, unsigned sequence)
    {
        return queryContext->queryCodeContext()->getResultVarUnicode(name, sequence);
    }
    virtual void getResultRowset(size32_t & tcount, const byte * * & tgt, const char * name, unsigned sequence, IEngineRowAllocator * _rowAllocator, bool isGrouped, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) override
    {
        return queryContext->queryCodeContext()->getResultRowset(tcount, tgt, name, sequence, _rowAllocator, isGrouped, xmlTransformer, csvTransformer);
    }
    virtual void getResultDictionary(size32_t & tcount, const byte * * & tgt, IEngineRowAllocator * _rowAllocator, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer, IHThorHashLookupInfo * hasher) override
    {
        return queryContext->queryCodeContext()->getResultDictionary(tcount, tgt, _rowAllocator, name, sequence, xmlTransformer, csvTransformer, hasher);
    }

    virtual unsigned getResultHash(const char * name, unsigned sequence) { throwUnexpected(); }
    virtual unsigned getExternalResultHash(const char * wuid, const char * name, unsigned sequence) { throwUnexpected(); }

    virtual ISectionTimer * registerTimer(unsigned activityId, const char * name)
    {
        return queryNullSectionTimer();
    }

    // Not yet thought about these....

    virtual char *getWuid() { throwUnexpected(); } // caller frees return string.
    virtual void getExternalResultRaw(unsigned & tlen, void * & tgt, const char * wuid, const char * stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) { throwUnexpected(); }  // shouldn't really be here, but it broke thor.
    virtual void executeGraph(const char * graphName, bool realThor, size32_t parentExtractSize, const void * parentExtract) { throwUnexpected(); }
    virtual unsigned __int64 getDatasetHash(const char * name, unsigned __int64 hash)   { throwUnexpected(); return 0; }

    virtual char * getExpandLogicalName(const char * logicalName) { throwUnexpected(); }
    virtual void addWuException(const char * text, unsigned code, unsigned severity, const char * source) { throwUnexpected(); }
    virtual void addWuAssertFailure(unsigned code, const char * text, const char * filename, unsigned lineno, unsigned column, bool isAbort) { throwUnexpected(); }
    virtual IUserDescriptor *queryUserDescriptor() { throwUnexpected(); }
    virtual unsigned getNodes() { throwUnexpected(); }
    virtual unsigned getNodeNum() { throwUnexpected(); }
    virtual char *getFilePart(const char *logicalPart, bool create=false) { throwUnexpected(); } // caller frees return string.
    virtual unsigned __int64 getFileOffset(const char *logicalPart) { throwUnexpected(); }
    virtual IDistributedFileTransaction *querySuperFileTransaction() { throwUnexpected(); }
    virtual char *getJobName() { throwUnexpected(); } // caller frees return string.
    virtual char *getJobOwner() { throwUnexpected(); } // caller frees return string.
    virtual char *getClusterName() { throwUnexpected(); } // caller frees return str.
    virtual char *getGroupName() { throwUnexpected(); } // caller frees return string.
    virtual char * queryIndexMetaData(char const * lfn, char const * xpath) { throwUnexpected(); }
    virtual char *getDaliServers() { return queryContext->queryCodeContext()->getDaliServers(); }

    // The below are called on Roxie server and passed in context
    virtual unsigned getPriority() const { throwUnexpected(); }
    virtual char *getPlatform() { throwUnexpected(); }
    virtual char *getEnv(const char *name, const char *defaultValue) const { throwUnexpected(); }
    virtual char *getOS() { throwUnexpected(); }

    virtual unsigned logString(const char *text) const
    {
        if (text && *text)
        {
            logctx.CTXLOG("USER: %s", text);
            return strlen(text);
        }
        else
            return 0;
    }
    virtual const IContextLogger &queryContextLogger() const
    {
        return logctx;
    }

    virtual IEngineRowAllocator * getRowAllocator(IOutputMetaData * meta, unsigned activityId) const
    {
        return queryContext->queryCodeContext()->getRowAllocator(meta, activityId);
    }
    virtual IEngineRowAllocator * getRowAllocatorEx(IOutputMetaData * meta, unsigned activityId, unsigned heapFlags) const
    {
        return queryContext->queryCodeContext()->getRowAllocatorEx(meta, activityId, heapFlags);
    }
    virtual const char *cloneVString(const char *str) const
    {
        return queryContext->queryCodeContext()->cloneVString(str);
    }
    virtual const char *cloneVString(size32_t len, const char *str) const
    {
        return queryContext->queryCodeContext()->cloneVString(len, str);
    }
    virtual void getRowXML(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags)
    {
        convertRowToXML(lenResult, result, info, row, flags);
    }
    virtual void getRowJSON(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags)
    {
        convertRowToJSON(lenResult, result, info, row, flags);
    }
    const void * fromXml(IEngineRowAllocator * rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace)
    {
        return createRowFromXml(rowAllocator, len, utf8, xmlTransformer, stripWhitespace);
    }
    const void * fromJson(IEngineRowAllocator * rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace)
    {
        return createRowFromJson(rowAllocator, len, utf8, xmlTransformer, stripWhitespace);
    }
    virtual IEngineContext *queryEngineContext() { return NULL; }
    virtual IWorkUnit *updateWorkUnit() const { throwUnexpected(); }
    virtual unsigned getGraphLoopCounter() const override { return queryContext->queryCodeContext()->getGraphLoopCounter(); }
    virtual IEclGraphResults * resolveLocalQuery(__int64 activityId) override { return NULL; } // Only want to do this on the server
    virtual IDebuggableContext *queryDebugContext() const override { return queryContext->queryCodeContext()->queryDebugContext(); }

};

//================================================================================================

class OptimizedRowBuilder : public ARowBuilder, public CInterface
{
public:
    OptimizedRowBuilder(IEngineRowAllocator * _rowAllocator, const CachedOutputMetaData & _meta, IMessagePacker * _output, IOutputRowSerializer * _serializer)
        : dynamicBuilder(_rowAllocator, false), meta(_meta), serializer(_serializer), output(_output)
    {
        useDynamic = serializer != NULL || meta.isVariableSize();
    }
    IMPLEMENT_IINTERFACE

    virtual IEngineRowAllocator *queryAllocator() const
    {
        return dynamicBuilder.queryAllocator();
    }
    virtual byte * createSelf()
    {
        if (useDynamic)
        {
            dynamicBuilder.ensureRow();
            self = dynamicBuilder.getSelf();
        }
        else
            self = static_cast<byte *>(output->getBuffer(meta.getFixedSize(), false));
        return self;
    }

    virtual byte * ensureCapacity(size32_t required, const char * fieldName)
    {
        if (useDynamic)
        {
            self = dynamicBuilder.ensureCapacity(required, fieldName);
            return static_cast<byte *>(self);
        }
        else
        {
            size32_t fixedLength = meta.getFixedSize();
            if (required <= fixedLength)
                return static_cast<byte *>(self);
            // This should never happen!
            rtlReportFieldOverflow(required, fixedLength, fieldName);
            return NULL;
        }
    }

    virtual void reportMissingRow() const
    {
        throw MakeStringException(MSGAUD_user, 1000, "OptimizedRowBuilder::row() is NULL");
    }

    inline void ensureRow()
    {
        if (!self)
            createSelf();
    }

    inline void clear()
    {
        if (useDynamic)
            dynamicBuilder.clear();
        self = NULL;
    }

    size_t writeToOutput(size32_t transformedSize, bool outputIfEmpty)
    {
        size32_t outputSize = transformedSize;
        if (transformedSize || outputIfEmpty)
        {
            if (useDynamic)
            {
                OwnedConstRoxieRow result = dynamicBuilder.finalizeRowClear(transformedSize);
                if (serializer)
                    outputSize = serializeRow(serializer, output, result);
                else
                {
                    self = static_cast<byte *>(output->getBuffer(transformedSize, true));
                    memcpy(self, result, transformedSize);
                    output->putBuffer(self, transformedSize, true);
                }
            }
            else
            {
                output->putBuffer(self, transformedSize, false);
            }
        }
        clear();
        return outputSize;
    }

private:
    RtlDynamicRowBuilder dynamicBuilder;
    const CachedOutputMetaData & meta;
    IMessagePacker * output;
    IOutputRowSerializer * serializer;
    bool useDynamic;
};

class OptimizedKJRowBuilder : public ARowBuilder, public CInterface
{
    // Rules are different enough that we can't easily derive from OptimizedRowBuilder
public:
    IMPLEMENT_IINTERFACE;
    OptimizedKJRowBuilder(IEngineRowAllocator * _rowAllocator, const CachedOutputMetaData & _meta, IMessagePacker * _output)
        : dynamicBuilder(_rowAllocator, false), meta(_meta), output(_output)
    {
        useDynamic = meta.isVariableSize();
    }
    virtual IEngineRowAllocator *queryAllocator() const
    {
        return dynamicBuilder.queryAllocator();
    }
    virtual byte * createSelf()
    {
        if (useDynamic)
        {
            dynamicBuilder.ensureRow();
            self = dynamicBuilder.getSelf();
            return self;
        }
        else
        {
            self = static_cast<byte *>(output->getBuffer(KEYEDJOIN_RECORD_SIZE(meta.getFixedSize()), true)) + KEYEDJOIN_RECORD_SIZE(0);
            return self ;
        }
    }

    virtual byte * ensureCapacity(size32_t required, const char * fieldName)
    {
        if (useDynamic)
        {
            self = dynamicBuilder.ensureCapacity(required, fieldName);
            return self;
        }
        else
        {
            size32_t fixedLength = meta.getFixedSize();
            if (required <= fixedLength)
                return self;
            rtlReportFieldOverflow(required, fixedLength, fieldName);
            return NULL;
        }
    }

    virtual void reportMissingRow() const
    {
        throw MakeStringException(MSGAUD_user, 1000, "OptimizedKJRowBuilder::row() is NULL");
    }

    inline void ensureRow()
    {
        if (!self)
            createSelf();
    }

    void writeToOutput(size32_t transformedSize, offset_t recptr, CJoinGroup *jg, unsigned short partNo)
    {
        KeyedJoinHeader *rec;
        if (useDynamic)
        {
            OwnedConstRoxieRow result = dynamicBuilder.finalizeRowClear(transformedSize);
            rec = (KeyedJoinHeader *) (output->getBuffer(KEYEDJOIN_RECORD_SIZE(transformedSize), true));
            memcpy(&rec->rhsdata, result, transformedSize);
        }
        else
        {
            rec = (KeyedJoinHeader *)(self - KEYEDJOIN_RECORD_SIZE(0));
        }
        rec->fpos = recptr;
        rec->thisGroup = jg;
        rec->partNo = partNo;
        output->putBuffer(rec, KEYEDJOIN_RECORD_SIZE(transformedSize), true);
        self = NULL;
    }

private:
    RtlDynamicRowBuilder dynamicBuilder;
    const CachedOutputMetaData & meta;
    IMessagePacker * output;
    bool useDynamic;
};

//================================================================================================

class CRoxieDiskReadBaseActivity : public CRoxieSlaveActivity, implements IIndexReadContext//, implements IDiskReadActivity
{
protected:
    IHThorDiskReadBaseArg *helper;

    unsigned processed;
    unsigned parallelPartNo;
    unsigned numParallel;

    bool isGrouped = false;
    bool forceUnkeyed;

    offset_t readPos;
    CachedOutputMetaData diskSize;
    ScoredRowFilter postFilter;
    Owned<IDirectReader> reader;
    Linked<IInMemoryIndexManager> manager;
    Linked<ITranslatorSet> translators;
    Owned<IInMemoryFileProcessor> processor;
    CriticalSection pcrit;

public:
    CRoxieDiskReadBaseActivity(SlaveContextLogger &_logctx, IRoxieQueryPacket *_packet, HelperFactory *_hFactory, const CSlaveActivityFactory *_aFactory,
        IInMemoryIndexManager *_manager,
        ITranslatorSet *_translators,
        unsigned _parallelPartNo, unsigned _numParallel, bool _forceUnkeyed)
        : CRoxieSlaveActivity(_logctx, _packet, _hFactory, _aFactory),
        manager(_manager),
        translators(_translators),
        parallelPartNo(_parallelPartNo),
        numParallel(_numParallel),
        forceUnkeyed(_forceUnkeyed)
    {
        helper = (IHThorDiskReadBaseArg *) basehelper;
        variableFileName = allFilesDynamic || basefactory->queryQueryFactory().isDynamic() || ((helper->getFlags() & (TDXvarfilename|TDXdynamicfilename)) != 0);
        isOpt = (helper->getFlags() & TDRoptional) != 0;
        diskSize.set(helper->queryProjectedDiskRecordSize()->querySerializedDiskMeta());
        isGrouped = diskSize.isGrouped();
        processed = 0;
        readPos = 0;
        if (resent)
        {
        }
    }

    virtual void onCreate()
    {
        CRoxieSlaveActivity::onCreate();
        helper->createSegmentMonitors(this);
        const IKeyTranslator *keyTranslator = translators->queryKeyTranslator(0);  // any part would do - in-memory requires all actuals to have same layout
        if (keyTranslator)
            keyTranslator->translate(postFilter);
        if (resent)
        {
            bool usedKey;
            resentInfo.read(processed);
            resentInfo.read(usedKey);
            if (usedKey)
                reader.setown(manager->selectKey(resentInfo, postFilter, translators));
            else
            {
                resentInfo.read(readPos);
                reader.setown(manager->createReader(postFilter, isGrouped, readPos, parallelPartNo, numParallel, translators));
            }
            assertex(resentInfo.remaining() == 0);
        }
        else
        {
            if (!forceUnkeyed && !isGrouped)
                reader.setown(manager->selectKey(postFilter, translators, logctx));
            if (!reader)
                reader.setown(manager->createReader(postFilter, isGrouped, readPos, parallelPartNo, numParallel, translators));
        }
    }

    virtual const char *queryDynamicFileName() const
    {
        return helper->getFileName();
    }

    virtual void setVariableFileInfo()
    {
        unsigned channel = packet->queryHeader().channel;
        unsigned projectedCrc = helper->getProjectedFormatCrc();
        unsigned expectedCrc = helper->getDiskFormatCrc();
        translators.setown(varFileInfo->getTranslators(projectedCrc, helper->queryProjectedDiskRecordSize(), expectedCrc, helper->queryDiskRecordSize(), basefactory->getEnableFieldTranslation(), false));
        manager.setown(varFileInfo->getIndexManager(isOpt, channel, translators->queryActualLayout(0), false));
    }

    inline bool queryKeyed() const
    {
        return reader->isKeyed();
    }

    void setParallel(unsigned _partno, unsigned _numParallel)
    {
        assertex(!processor);
        parallelPartNo = _partno;
        numParallel = _numParallel;
    }


    virtual StringBuffer &toString(StringBuffer &ret) const
    {
        return ret.appendf("DiskRead %u", packet->queryHeader().activityId);
    }

    virtual void append(IKeySegmentMonitor *segment)
    {
        segment->Release();
        throwUnexpected();
    }

    virtual void append(FFoption option, const IFieldFilter * filter)
    {
        if (filter->isWild())
            filter->Release();
        else
            postFilter.addFilter(*filter);
    }

    virtual void abort() 
    {
        CRoxieSlaveActivity::abort();
        CriticalBlock p(pcrit);
        if (processor)
            processor->abort();
    }

    virtual IMessagePacker *process()
    {
        MTIME_SECTION(queryActiveTimer(), "CRoxieDiskReadBaseActivity::process");
        diskReadStarted++;
        Owned<IMessagePacker> output = ROQ->createOutputStream(packet->queryHeader(), false, logctx);
        doProcess(output);
        helper->setCallback(NULL);
        if (aborted)
            return NULL;
        else
        {
            diskReadCompleted++;
            return output.getClear();
        }
    }

    virtual void doCheck(IMessagePacker *output)
    {
        // for in-memory diskread activities, not a lot to check. If it got this far the answer is 'true'...
        void *recBuffer = output->getBuffer(sizeof(bool), false);
        bool ret = true;
        memcpy(recBuffer, &ret, sizeof(bool));
    }

    virtual void doProcess(IMessagePacker * output) = 0;
        
    virtual void setPartNo(bool filechanged)
    {
        throwUnexpected();
    }

};

class CRoxieDiskBaseActivityFactory : public CSlaveActivityFactory
{
protected:
    Owned<ITranslatorSet> translators;
    Owned<IInMemoryIndexManager> manager;
public:
    CRoxieDiskBaseActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
        : CSlaveActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory)
    {
        Owned<IHThorDiskReadBaseArg> helper = (IHThorDiskReadBaseArg *) helperFactory();
        bool variableFileName = allFilesDynamic || queryFactory.isDynamic() || ((helper->getFlags() & (TDXvarfilename|TDXdynamicfilename)) != 0);
        if (!variableFileName)
        {
            bool isOpt = (helper->getFlags() & TDRoptional) != 0;
            OwnedRoxieString fileName(helper->getFileName());
            datafile.setown(_queryFactory.queryPackage().lookupFileName(fileName, isOpt, true, true, _queryFactory.queryWorkUnit(), true));
            if (datafile)
            {
                unsigned channel = queryFactory.queryChannel();
                unsigned projectedFormatCrc = helper->getProjectedFormatCrc();
                unsigned expectedFormatCrc = helper->getDiskFormatCrc();
                translators.setown(datafile->getTranslators(projectedFormatCrc, helper->queryProjectedDiskRecordSize(), expectedFormatCrc, helper->queryDiskRecordSize(), getEnableFieldTranslation(), false));
                manager.setown(datafile->getIndexManager(isOpt, channel, translators->queryActualLayout(0), _graphNode.getPropBool("att[@name=\"preload\"]/@value", false)));
                Owned<IPropertyTreeIterator> memKeyInfo = queryFactory.queryPackage().getInMemoryIndexInfo(_graphNode);
                Owned<IPropertyTree> memKeyHint;
                if (!memKeyInfo && _graphNode.hasProp("hint[@name=\"key\"]"))
                {
                    memKeyHint.setown(createPTreeFromXMLString(_graphNode.queryProp("hint[@name=\"key\"]/@value")));
                    memKeyInfo.setown(memKeyHint->getElements("MemIndex"));
                }
                if (memKeyInfo)
                {
                    // There's a small potential flaw that I can end up with indexes from a query that was unloaded active on some nodes but not others
                    // which could mess up continuation. I think I don't care.
                    ForEach(*memKeyInfo)
                    {
                        IPropertyTree &info = memKeyInfo->query();
                        manager->setKeyInfo(info);
                    }
                }
            }
            else
                manager.setown(createInMemoryIndexManager(helper->queryProjectedDiskRecordSize()->queryRecordAccessor(true), true, nullptr));
        }
    }

    ~CRoxieDiskBaseActivityFactory()
    {
    }
};


//================================================================================================

class CRoxieDiskReadActivity;
class CRoxieCsvReadActivity;
class CRoxieXmlReadActivity;
IInMemoryFileProcessor *createReadRecordProcessor(CRoxieDiskReadActivity &owner, bool isGrouped, IDirectReader *reader);
IInMemoryFileProcessor *createCsvRecordProcessor(CRoxieCsvReadActivity &owner, IDirectReader *reader, bool _skipHeader, const IResolvedFile *datafile, size32_t maxRowSize);
IInMemoryFileProcessor *createXmlRecordProcessor(CRoxieXmlReadActivity &owner, IDirectReader *reader);

class CRoxieDiskReadActivity : public CRoxieDiskReadBaseActivity
{
    friend class ReadRecordProcessor;

protected:
    IHThorDiskReadArg *helper;

public:
    CRoxieDiskReadActivity(SlaveContextLogger &_logctx, IRoxieQueryPacket *_packet, HelperFactory *_hFactory, const CSlaveActivityFactory *_aFactory,
        IInMemoryIndexManager *_manager, ITranslatorSet *_translators)
        : CRoxieDiskReadBaseActivity(_logctx, _packet, _hFactory, _aFactory, _manager, _translators, 0, 1, false)
    {
        onCreate();
        helper = (IHThorDiskReadArg *) basehelper;
    }

    virtual StringBuffer &toString(StringBuffer &ret) const
    {
        return ret.appendf("DiskRead %u", packet->queryHeader().activityId);
    }

    virtual void doProcess(IMessagePacker * output)
    {
        {
            CriticalBlock p(pcrit); // because of race with abort.
            processor.setown(createReadRecordProcessor(*this, isGrouped, reader));
        }
        unsigned __int64 rowLimit = helper->getRowLimit();
        unsigned __int64 stopAfter = helper->getChooseNLimit();
        processor->doQuery(output, processed, rowLimit, stopAfter);
    }

    size32_t doTransform(IMessagePacker * output, const void *src) const
    {
        if (likely(helper->canMatch(src)))
        {
            OptimizedRowBuilder rowBuilder(rowAllocator, meta, output, serializer);
            unsigned transformedSize = helper->transform(rowBuilder, src);
            return rowBuilder.writeToOutput(transformedSize, false);
        }
        else
            return 0;
    }

};

class CRoxieCsvReadActivity : public CRoxieDiskReadBaseActivity 
{
public:
    friend class CsvRecordProcessor;

protected:
    IHThorCsvReadArg *helper;
    const IResolvedFile *datafile;
    size32_t maxRowSize;

public:
    CRoxieCsvReadActivity(SlaveContextLogger &_logctx, IRoxieQueryPacket *_packet, HelperFactory *_hFactory, const CSlaveActivityFactory *_aFactory,
                          IInMemoryIndexManager *_manager, ITranslatorSet *_translators, const IResolvedFile *_datafile, size32_t _maxRowSize)
        : CRoxieDiskReadBaseActivity(_logctx, _packet, _hFactory, _aFactory, _manager, _translators, 0, 1, true), datafile(_datafile), maxRowSize(_maxRowSize)
    {
        onCreate();
        helper = (IHThorCsvReadArg *) basehelper;
    }

    virtual StringBuffer &toString(StringBuffer &ret) const
    {
        return ret.appendf("CsvRead %u", packet->queryHeader().activityId);
    }

    virtual void doProcess(IMessagePacker * output)
    {
        {
            CriticalBlock p(pcrit);
            processor.setown(
                    createCsvRecordProcessor(*this,
                                             manager->createReader(postFilter, false, readPos, parallelPartNo, numParallel, translators),
                                             packet->queryHeader().channel==1 && !resent,
                                             varFileInfo ? varFileInfo.get() : datafile, maxRowSize));
        }
        unsigned __int64 rowLimit = helper->getRowLimit();
        unsigned __int64 stopAfter = helper->getChooseNLimit();
        processor->doQuery(output, processed, rowLimit, stopAfter);
    }

    size32_t doTransform(IMessagePacker * output, unsigned *srcLen, const char **src) const
    {
        OptimizedRowBuilder rowBuilder(rowAllocator, meta, output, serializer);
        unsigned transformedSize = helper->transform(rowBuilder, srcLen, src);
        return rowBuilder.writeToOutput(transformedSize, false);
    }
};

class CRoxieXmlReadActivity : public CRoxieDiskReadBaseActivity 
{
public:
    friend class XmlRecordProcessor;

protected:
    IHThorXmlReadArg *helper;

public:
    CRoxieXmlReadActivity(SlaveContextLogger &_logctx, IRoxieQueryPacket *_packet, HelperFactory *_hFactory, const CSlaveActivityFactory *_aFactory,
        IInMemoryIndexManager *_manager, ITranslatorSet *_translators)
        : CRoxieDiskReadBaseActivity(_logctx, _packet, _hFactory, _aFactory, _manager, _translators, 0, 1, true)
    {
        onCreate();
        helper = (IHThorXmlReadArg *) basehelper;
    }

    virtual StringBuffer &toString(StringBuffer &ret) const
    {
        return ret.appendf("XmlRead %u", packet->queryHeader().activityId);
    }

    virtual void doProcess(IMessagePacker * output)
    {
        {
            CriticalBlock p(pcrit);
            processor.setown(createXmlRecordProcessor(*this, manager->createReader(postFilter, false, readPos, parallelPartNo, numParallel, translators)));
        }
        unsigned __int64 rowLimit = helper->getRowLimit();
        unsigned __int64 stopAfter = helper->getChooseNLimit();
        processor->doQuery(output, processed, rowLimit, stopAfter);
    }

    size32_t doTransform(IMessagePacker * output, IXmlToRowTransformer *rowTransformer, IColumnProvider *lastMatch, IThorDiskCallback *callback) const
    {
        OptimizedRowBuilder rowBuilder(rowAllocator, meta, output, serializer);
        unsigned transformedSize = rowTransformer->transform(rowBuilder, lastMatch, callback);
        return rowBuilder.writeToOutput(transformedSize, false);
    }
};

class CRoxieDiskReadActivityFactory : public CRoxieDiskBaseActivityFactory
{
public:
    CRoxieDiskReadActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
        : CRoxieDiskBaseActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory)
    {
    }

    virtual IRoxieSlaveActivity *createActivity(SlaveContextLogger &logctx, IRoxieQueryPacket *packet) const
    {
        return new CRoxieDiskReadActivity(logctx, packet, helperFactory, this, manager, translators);
    }

    virtual StringBuffer &toString(StringBuffer &s) const
    {
        return CSlaveActivityFactory::toString(s.append("DiskRead "));
    }

};

class CRoxieCsvReadActivityFactory : public CRoxieDiskBaseActivityFactory
{
    size32_t maxRowSize;

public:
    CRoxieCsvReadActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
        : CRoxieDiskBaseActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory)
    {
        maxRowSize = defaultMaxCsvRowSize * 1024 * 1024;
        IConstWorkUnit *workunit = _queryFactory.queryWorkUnit();
        if (workunit)
            maxRowSize = workunit->getDebugValueInt(OPT_MAXCSVROWSIZE, defaultMaxCsvRowSize) * 1024 * 1024;
    }

    virtual IRoxieSlaveActivity *createActivity(SlaveContextLogger &logctx, IRoxieQueryPacket *packet) const
    {
        return new CRoxieCsvReadActivity(logctx, packet, helperFactory, this, manager, translators, datafile, maxRowSize);
    }

    virtual StringBuffer &toString(StringBuffer &s) const
    {
        return CSlaveActivityFactory::toString(s.append("DiskRead "));
    }

};

class CRoxieXmlReadActivityFactory : public CRoxieDiskBaseActivityFactory
{
public:
    CRoxieXmlReadActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
        : CRoxieDiskBaseActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory)
    {
    }

    virtual IRoxieSlaveActivity *createActivity(SlaveContextLogger &logctx, IRoxieQueryPacket *packet) const
    {
        return new CRoxieXmlReadActivity(logctx, packet, helperFactory, this, manager, translators);
    }

    virtual StringBuffer &toString(StringBuffer &s) const
    {
        return CSlaveActivityFactory::toString(s.append("DiskRead "));
    }

};


//================================================================================================

class RecordProcessor : implements IInMemoryFileProcessor, public CInterface
{
protected:
    bool aborted = false;
    IDirectReader *reader;

public:
    IMPLEMENT_IINTERFACE
    RecordProcessor(IDirectReader *_reader) : reader(_reader) {}
    virtual void abort() 
    {
        aborted = true; 
    }
};

//================================================================================================

// Disk read activity

class ReadRecordProcessor : public RecordProcessor
{
public:
    ReadRecordProcessor(CRoxieDiskReadActivity &_owner, bool _isGrouped, IDirectReader *_reader)
        : RecordProcessor(_reader), owner(_owner), isGrouped(_isGrouped)
    {
        helper = _owner.helper;
    }

    virtual void doQuery(IMessagePacker *output, unsigned processed, unsigned __int64 rowLimit, unsigned __int64 stopAfter)
    {
        unsigned totalSizeSent = 0;
        helper->setCallback(reader);
        unsigned lastGroupProcessed = processed;
        bool someInGroup = false;
        while (!aborted)
        {
            // This loop is the inner loop for memory diskreads - so keep it efficient!
            const byte *nextRec = reader->nextRow();
            if (nextRec)
            {
                someInGroup = true;
                size32_t transformedSize = owner.doTransform(output, nextRec);
                reader->finishedRow();
                if (transformedSize)
                {
                    processed++;
                    if (processed > rowLimit)
                    {
                        owner.limitExceeded();
                        return;
                    }
                    if (processed == stopAfter)
                        return;
                    totalSizeSent += transformedSize;
                    if (totalSizeSent > indexReadChunkSize && !isGrouped)
                        break;
                }
            }
            else if (isGrouped)
            {
                if (processed>lastGroupProcessed)
                    break; // We return grouped data one whole group at a time
                else if (!someInGroup) // eof
                    return;
            }
            else
                return;
        }
        // Send continuation message indicating end of group or too much data
        if (!aborted)
        {
            MemoryBuffer si;
            unsigned short siLen = 0;
            si.append(siLen);
            si.append(processed);
            si.append(reader->isKeyed());
            reader->serializeCursorPos(si);
            siLen = si.length() - sizeof(siLen);
            si.writeDirect(0, sizeof(siLen), &siLen);
            output->sendMetaInfo(si.toByteArray(), si.length());
        }
    }

protected:
    CRoxieDiskReadActivity &owner;
    IHThorDiskReadArg *helper;
    bool isGrouped;
};

IInMemoryFileProcessor *createReadRecordProcessor(CRoxieDiskReadActivity &owner, bool isGrouped, IDirectReader *_reader)
{
    return new ReadRecordProcessor(owner, isGrouped, _reader);
}

//================================================================================================

// RecordProcessor used by CSV read activity. We don't try to index these ...

class CsvRecordProcessor : public RecordProcessor
{
protected:
    CRoxieCsvReadActivity &owner;
    IHThorCsvReadArg *helper;

    bool skipHeader;
    const IResolvedFile *datafile;
    size32_t maxRowSize;

public:
    CsvRecordProcessor(CRoxieCsvReadActivity &_owner, IDirectReader *_reader, bool _skipHeader, const IResolvedFile *_datafile, size32_t _maxRowSize)
      : RecordProcessor(_reader), owner(_owner), datafile(_datafile), maxRowSize(_maxRowSize)
    {
        // NOTE - postfilter not used at present - but may be in future
        helper = _owner.helper;
        skipHeader = _skipHeader;
        helper->setCallback(reader);
    }

    virtual void doQuery(IMessagePacker *output, unsigned processed, unsigned __int64 rowLimit, unsigned __int64 stopAfter)
    {
        unsigned totalSizeSent = 0;
        ICsvParameters * csvInfo = helper->queryCsvParameters();
        unsigned headerLines = skipHeader ? csvInfo->queryHeaderLen() : 0;
        const char *quotes = NULL;
        const char *separators = NULL;
        const char *terminators = NULL;
        const char *escapes = NULL;
        CSVSplitter csvSplitter;
        if (datafile)
        {
            const IPropertyTree *options = datafile->queryProperties();
            if (options)
            {
                quotes = options->queryProp("@csvQuote");
                separators = options->queryProp("@csvSeparate");
                terminators = options->queryProp("@csvTerminate");
                escapes = options->queryProp("@csvEscape");
            }
        }
        csvSplitter.init(helper->getMaxColumns(), csvInfo, quotes, separators, terminators, escapes);
        ISerialStream *stream = reader->queryDirectStreamReader();
        while (!aborted)
        {
            size32_t thisLineLength = csvSplitter.splitLine(stream, maxRowSize);
            if (0 == thisLineLength)
                break;
            if (headerLines)
            {
                headerLines--;
                stream->skip(thisLineLength);
            }
            else
            {
                unsigned transformedSize = owner.doTransform(output, csvSplitter.queryLengths(), (const char * *)csvSplitter.queryData());
                stream->skip(thisLineLength);
                if (transformedSize)
                {
                    processed++;
                    if (processed > rowLimit)
                    {
                        owner.limitExceeded(); 
                        return;
                    }
                    if (processed == stopAfter)
                        return;
                    totalSizeSent += transformedSize;
                    if (totalSizeSent > indexReadChunkSize)
                    {
                        MemoryBuffer si;
                        unsigned short siLen = 0;
                        si.append(siLen);
                        si.append(processed);
                        si.append(reader->isKeyed());
                        reader->serializeCursorPos(si);
                        siLen = si.length() - sizeof(siLen);
                        si.writeDirect(0, sizeof(siLen), &siLen);
                        output->sendMetaInfo(si.toByteArray(), si.length());
                        return;
                    }
                }
            }
        }
    }
};

//================================================================================================

// RecordProcessor used by XML read activity. We don't try to index these...

class XmlRecordProcessor : public RecordProcessor, implements IXMLSelect, implements IThorDiskCallback
{
public:
    IMPLEMENT_IINTERFACE;
    XmlRecordProcessor(CRoxieXmlReadActivity &_owner, IDirectReader *_reader)
        : RecordProcessor(_reader), owner(_owner), streamReader(_reader->queryDirectStreamReader()), fileposition(0)
    {
        helper = _owner.helper;
        helper->setCallback(this);
    }

    //interface IThorDiskCallback
    virtual unsigned __int64 getFilePosition(const void * row)
    {
        return fileposition;
    }
    virtual unsigned __int64 getLocalFilePosition(const void * row)
    {
        return streamReader->makeFilePositionLocal(fileposition);
    }
    virtual const char * queryLogicalFilename(const void * row)
    {
        return reader->queryLogicalFilename(row);
    }
    virtual const byte * lookupBlob(unsigned __int64 id) { throwUnexpected(); }

    virtual void match(IColumnProvider &entry, offset_t startOffset, offset_t endOffset)
    {
        fileposition = startOffset;
        lastMatch.set(&entry);
    }

    virtual void doQuery(IMessagePacker *output, unsigned processed, unsigned __int64 rowLimit, unsigned __int64 stopAfter)
    {
        // Note: xml read does not support continuation record stuff as too hard to serialize state of xml parser
        Linked<IXmlToRowTransformer> rowTransformer = helper->queryTransformer();
        OwnedRoxieString xmlIterator(helper->getXmlIteratorPath());
        Owned<IXMLParse> xmlParser;
        if (owner.basefactory->getKind() == TAKjsonread)
            xmlParser.setown(createJSONParse(*streamReader, xmlIterator, *this, (0 != (TDRxmlnoroot & helper->getFlags()))?ptr_noRoot:ptr_none, (helper->getFlags() & TDRusexmlcontents) != 0));
        else
            xmlParser.setown(createXMLParse(*streamReader, xmlIterator, *this, (0 != (TDRxmlnoroot & helper->getFlags()))?ptr_noRoot:ptr_none, (helper->getFlags() & TDRusexmlcontents) != 0));
        while (!aborted)
        {
            //call to next() will callback on the IXmlSelect interface
            bool gotNext = false;
            gotNext = xmlParser->next();
            if(!gotNext)
                break;
            else if (lastMatch)
            {
                unsigned transformedSize = owner.doTransform(output, rowTransformer, lastMatch, this);
                lastMatch.clear();
                if (transformedSize)
                {
                    processed++;
                    if (processed > rowLimit)
                    {
                        owner.limitExceeded(); 
                        return;
                    }
                    if (processed == stopAfter)
                        return;
                }
            }
        }
    }

protected:
    CRoxieXmlReadActivity &owner;
    IHThorXmlReadArg *helper;

    Owned<IColumnProvider> lastMatch;
    IDirectStreamReader *streamReader;
    unsigned __int64 fileposition;
};

IInMemoryFileProcessor *createCsvRecordProcessor(CRoxieCsvReadActivity &owner, IDirectReader *_reader, bool _skipHeader, const IResolvedFile *datafile, size32_t maxRowSize)
{
    return new CsvRecordProcessor(owner, _reader, _skipHeader, datafile, maxRowSize);
}

IInMemoryFileProcessor *createXmlRecordProcessor(CRoxieXmlReadActivity &owner, IDirectReader *_reader)
{
    return new XmlRecordProcessor(owner, _reader);
}

ISlaveActivityFactory *createRoxieCsvReadActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
{
    return new CRoxieCsvReadActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory);
}

ISlaveActivityFactory *createRoxieXmlReadActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
{
    return new CRoxieXmlReadActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory);
}

ISlaveActivityFactory *createRoxieDiskReadActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
{
    return new CRoxieDiskReadActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory);
}


//================================================================================================

class CRoxieDiskNormalizeActivity;
IInMemoryFileProcessor *createNormalizeRecordProcessor(CRoxieDiskNormalizeActivity &owner, IDirectReader *_reader);

class CRoxieDiskNormalizeActivity : public CRoxieDiskReadBaseActivity
{
    friend class NormalizeRecordProcessor;

protected:
    IHThorDiskNormalizeArg *helper;

public:
    CRoxieDiskNormalizeActivity(SlaveContextLogger &_logctx, IRoxieQueryPacket *_packet, HelperFactory *_hFactory, const CSlaveActivityFactory *_aFactory,
        IInMemoryIndexManager *_manager, ITranslatorSet *_translators)
        : CRoxieDiskReadBaseActivity(_logctx, _packet, _hFactory, _aFactory, _manager, _translators, 0, 1, false)
    {
        onCreate();
        helper = (IHThorDiskNormalizeArg *) basehelper;
    }

    virtual StringBuffer &toString(StringBuffer &ret) const
    {
        return ret.appendf("DiskNormalize %u", packet->queryHeader().activityId);
    }

    virtual void doProcess(IMessagePacker * output)
    {
        {
            CriticalBlock p(pcrit);
            processor.setown(createNormalizeRecordProcessor(*this, reader));
        }
        unsigned __int64 rowLimit = helper->getRowLimit();
        unsigned __int64 stopAfter = helper->getChooseNLimit();
        processor->doQuery(output, processed, rowLimit, stopAfter);
    }

    size32_t doNormalizeTransform(IMessagePacker * output) const
    {
        OptimizedRowBuilder rowBuilder(rowAllocator, meta, output, serializer);
        unsigned transformedSize = helper->transform(rowBuilder);
        return rowBuilder.writeToOutput(transformedSize, false);
    }

};

class CRoxieDiskNormalizeActivityFactory : public CRoxieDiskBaseActivityFactory
{
public:
    CRoxieDiskNormalizeActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
        : CRoxieDiskBaseActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory)
    {
    }

    virtual IRoxieSlaveActivity *createActivity(SlaveContextLogger &logctx, IRoxieQueryPacket *packet) const
    {
        return new CRoxieDiskNormalizeActivity(logctx, packet, helperFactory, this, manager, translators);
    }

    virtual StringBuffer &toString(StringBuffer &s) const
    {
        return CSlaveActivityFactory::toString(s.append("DiskNormalize "));
    }

};

ISlaveActivityFactory *createRoxieDiskNormalizeActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
{
    return new CRoxieDiskNormalizeActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory);
}


//================================================================================================

// RecordProcessors used by Disk Normalize activity.

class NormalizeRecordProcessor : public RecordProcessor
{
public:
    NormalizeRecordProcessor(CRoxieDiskNormalizeActivity &_owner, IDirectReader *_reader)
    : RecordProcessor(_reader), owner(_owner)
    {
        helper = _owner.helper;
    }

    virtual void doQuery(IMessagePacker *output, unsigned processed, unsigned __int64 rowLimit, unsigned __int64 stopAfter)
    {
        unsigned totalSizeSent = 0;
        helper->setCallback(reader);
        while (!aborted)
        {
            const byte *nextRec = reader->nextRow();
            if (!nextRec)
                break;
            if (helper->first(nextRec))
            {
                do
                {
                    size32_t transformedSize = owner.doNormalizeTransform(output);
                    if (transformedSize)
                    {
                        processed++;
                        if (processed > rowLimit)
                        {
                            owner.limitExceeded();
                            return;
                        }
                        totalSizeSent += transformedSize;
                        if (processed == stopAfter)
                            return;
                    }
                } while (helper->next());
            }
            reader->finishedRow();
            if (totalSizeSent > indexReadChunkSize)
            {
                MemoryBuffer si;
                unsigned short siLen = 0;
                si.append(siLen);
                si.append(processed);
                si.append(reader->isKeyed());
                reader->serializeCursorPos(si);
                siLen = si.length() - sizeof(siLen);
                si.writeDirect(0, sizeof(siLen), &siLen);
                output->sendMetaInfo(si.toByteArray(), si.length());
                return;
            }
        }
    }

protected:
    CRoxieDiskNormalizeActivity &owner;
    IHThorDiskNormalizeArg *helper;
};

IInMemoryFileProcessor *createNormalizeRecordProcessor(CRoxieDiskNormalizeActivity &owner, IDirectReader *_reader)
{
    return new NormalizeRecordProcessor(owner, _reader);
}


//================================================================================================

class CRoxieDiskCountActivity;
IInMemoryFileProcessor *createCountRecordProcessor(CRoxieDiskCountActivity &owner, IDirectReader *reader);

class CRoxieDiskCountActivity : public CRoxieDiskReadBaseActivity
{
    friend class CountRecordProcessor;

protected:
    IHThorDiskCountArg *helper;

public:
    CRoxieDiskCountActivity(SlaveContextLogger &_logctx, IRoxieQueryPacket *_packet, HelperFactory *_hFactory, const CSlaveActivityFactory *_aFactory,
        IInMemoryIndexManager *_manager, ITranslatorSet *_translators)
        : CRoxieDiskReadBaseActivity(_logctx, _packet, _hFactory, _aFactory, _manager, _translators, 0, 1, false)
    {
        onCreate();
        helper = (IHThorDiskCountArg *) basehelper;
    }

    virtual StringBuffer &toString(StringBuffer &ret) const
    {
        return ret.appendf("DiskCount %u", packet->queryHeader().activityId);
    }

    virtual void doProcess(IMessagePacker * output)
    {
        {
            CriticalBlock p(pcrit);
            processor.setown(createCountRecordProcessor(*this, reader));
        }
        unsigned __int64 stopAfter = helper->getChooseNLimit();
        processor->doQuery(output, processed, (unsigned __int64) -1, stopAfter);
    }
};

class CRoxieDiskCountActivityFactory : public CRoxieDiskBaseActivityFactory
{
public:
    CRoxieDiskCountActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
        : CRoxieDiskBaseActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory)
    {
    }

    virtual IRoxieSlaveActivity *createActivity(SlaveContextLogger &logctx, IRoxieQueryPacket *packet) const
    {
        return new CRoxieDiskCountActivity(logctx, packet, helperFactory, this, manager, translators);
    }

    virtual StringBuffer &toString(StringBuffer &s) const
    {
        return CSlaveActivityFactory::toString(s.append("DiskRead "));
    }

};


//================================================================================================

// RecordProcessors used by Disk Count activity.

class CountRecordProcessor : public RecordProcessor
{
public:
    CountRecordProcessor(CRoxieDiskCountActivity &_owner, IDirectReader *_reader)
    : RecordProcessor(_reader), owner(_owner)
    {
        helper = _owner.helper;
    }

    virtual void doQuery(IMessagePacker *output, unsigned processed, unsigned __int64 rowLimit, unsigned __int64 stopAfter)
    {
        unsigned outputRecordSize = owner.meta.getFixedSize();
        void *recBuffer = output->getBuffer(outputRecordSize, false);
        helper->setCallback(reader);
        unsigned __int64 totalCount  = 0;
        while (!aborted)
        {
            const byte *nextRec = reader->nextRow();
            if (!nextRec)
                break;
            totalCount += helper->numValid(nextRec);
            if (totalCount >= stopAfter)
            {
                totalCount = stopAfter;
                break;
            }
            reader->finishedRow();
        }
        if (!aborted)
        {
            assert(!owner.serializer); // A count can never need serializing, surely!
            if (outputRecordSize == 1)
                *(byte *)recBuffer = (byte)totalCount;
            else
            {
                assertex(outputRecordSize == sizeof(unsigned __int64));
                *(unsigned __int64 *)recBuffer = totalCount;
            }
            output->putBuffer(recBuffer, outputRecordSize, false);
        }
    }
protected:
    CRoxieDiskCountActivity &owner;
    IHThorDiskCountArg *helper;
};

IInMemoryFileProcessor *createCountRecordProcessor(CRoxieDiskCountActivity &owner, IDirectReader *reader)
{
    return new CountRecordProcessor(owner, reader);
}

ISlaveActivityFactory *createRoxieDiskCountActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
{
    return new CRoxieDiskCountActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory);
}


//================================================================================================

class CRoxieDiskAggregateActivity;
IInMemoryFileProcessor *createAggregateRecordProcessor(CRoxieDiskAggregateActivity &owner, IDirectReader *_reader);

class CRoxieDiskAggregateActivity : public CRoxieDiskReadBaseActivity
{
    friend class AggregateRecordProcessor;

protected:
    IHThorDiskAggregateArg *helper;

public:
    CRoxieDiskAggregateActivity(SlaveContextLogger &_logctx, IRoxieQueryPacket *_packet, HelperFactory *_hFactory, const CSlaveActivityFactory *_aFactory,
        IInMemoryIndexManager *_manager,
        ITranslatorSet *_translators,
        unsigned _parallelPartNo, unsigned _numParallel, bool _forceUnkeyed)
        : CRoxieDiskReadBaseActivity(_logctx, _packet, _hFactory, _aFactory, _manager, _translators, _parallelPartNo, _numParallel, _forceUnkeyed)
    {
        onCreate();
        helper = (IHThorDiskAggregateArg *) basehelper;
    }

    virtual bool needsRowAllocator()
    {
        return true;
    }

    virtual StringBuffer &toString(StringBuffer &ret) const
    {
        return ret.appendf("DiskAggregate %u", packet->queryHeader().activityId);
    }

    virtual void doProcess(IMessagePacker * output)
    {
        {
            CriticalBlock p(pcrit);
            processor.setown(createAggregateRecordProcessor(*this, reader));
        }
        processor->doQuery(output, 0, 0, 0);
    }
};

//================================================================================================

class CParallelRoxieActivity : public CRoxieSlaveActivity
{
protected:
    IBasedArrayOf<CRoxieDiskReadBaseActivity, IRoxieSlaveActivity> parts;
    unsigned numParallel;
    CriticalSection parCrit;
    Owned<IOutputRowDeserializer> deserializer;

public:
    CParallelRoxieActivity(SlaveContextLogger &_logctx, IRoxieQueryPacket *_packet, HelperFactory *_hFactory, const CSlaveActivityFactory *_factory, unsigned _numParallel)
        : CRoxieSlaveActivity(_logctx, _packet, _hFactory, _factory), numParallel(_numParallel)
    {
        assertex(numParallel > 1);
    }

    virtual void abort()
    {
        ForEachItemIn(idx, parts)
        {
            parts.item(idx).abort();
        }
    }

    virtual void setPartNo(bool filechanged) { throwUnexpected(); }
    virtual StringBuffer &toString(StringBuffer &ret) const
    {
        return parts.item(0).toString(ret);
    }

    virtual const char *queryDynamicFileName() const
    {
        throwUnexpected();
    }

    virtual void setVariableFileInfo()
    {
        throwUnexpected();
    }

    virtual void doProcess(IMessagePacker * output) = 0;
    virtual void processRow(CDummyMessagePacker &output) = 0;

    virtual IMessagePacker *process()
    {
        if (numParallel == 1)
        {
            return parts.item(0).process();
        }
        else
        {
            MTIME_SECTION(queryActiveTimer(), "CParallelRoxieActivity::process");
            diskReadStarted++;
            Owned<IMessagePacker> output = ROQ->createOutputStream(packet->queryHeader(), false, logctx);
            class casyncfor: public CAsyncFor
            {
                IBasedArrayOf<CRoxieDiskReadBaseActivity, IRoxieSlaveActivity> &parts;
                CParallelRoxieActivity &parent;
            public:
                casyncfor(IBasedArrayOf<CRoxieDiskReadBaseActivity, IRoxieSlaveActivity> &_parts, CParallelRoxieActivity &_parent)
                    : parts(_parts), parent(_parent)
                {
                }

                void Do(unsigned i)
                {
                    try
                    {
                        CDummyMessagePacker d;
                        parts.item(i).doProcess(&d);
                        d.flush(true);
                        parent.processRow(d);
                    }
                    catch (IException *)
                    {
                        // if one throws exception, may as well abort the rest
                        parent.abort();
                        throw;
                    }
                }
            } afor(parts, *this);
            afor.For(numParallel, numParallel);
            //for (unsigned i = 0; i < numParallel; i++) afor.Do(i); // use this instead of line above to make them serial - handy for debugging!
            if (aborted)
                return NULL;
            else
            {
                doProcess(output);
                diskReadCompleted++;
                return output.getClear();
            }
        }
    }
};


class CParallelRoxieDiskAggregateActivity : public CParallelRoxieActivity
{
protected:
    IHThorDiskAggregateArg *helper;
    OwnedConstRoxieRow finalRow;
public:
    CParallelRoxieDiskAggregateActivity(SlaveContextLogger &_logctx, IRoxieQueryPacket *_packet, HelperFactory *_hFactory, const CSlaveActivityFactory *_aFactory,
        IInMemoryIndexManager *_manager, ITranslatorSet *_translators, unsigned _numParallel) :
        CParallelRoxieActivity(_logctx, _packet, _hFactory, _aFactory, _numParallel)
    {
        helper = (IHThorDiskAggregateArg *) basehelper;
        onCreate();
        if (meta.needsSerializeDisk())
        {
            // MORE - avoiding serializing to dummy would be more efficient...
            deserializer.setown(meta.createDiskDeserializer(queryContext->queryCodeContext(), basefactory->queryId()));
        }
        CRoxieDiskAggregateActivity *part0 = new CRoxieDiskAggregateActivity(_logctx, _packet, _hFactory, _aFactory, _manager, _translators, 0, numParallel, false);
        parts.append(*part0);
        if (part0->queryKeyed())
        {
            numParallel = 1;
            part0->setParallel(0, 1);
        }
        else
        {
            for (unsigned i = 1; i < numParallel; i++)
                parts.append(*new CRoxieDiskAggregateActivity(_logctx, _packet, _hFactory, _aFactory, _manager, _translators, i, numParallel, true));
        }
    }

    ~CParallelRoxieDiskAggregateActivity()
    {
        finalRow.clear();
    }

    virtual bool needsRowAllocator()
    {
        return true;
    }

    virtual void doProcess(IMessagePacker *output)
    {
        if (!aborted)
        {
            if (!finalRow)
            {
                RtlDynamicRowBuilder rowBuilder(rowAllocator);
                size32_t size = helper->clearAggregate(rowBuilder);
                finalRow.setown(rowBuilder.finalizeRowClear(size));
            }
            //GH-RKC: This can probably be cleaner and more efficient using the OptimizedRowBuilder class introduced since I fixed this code
            if (serializer)
                serializeRow(output, finalRow);
            else
            {
                size32_t size = meta.getRecordSize(finalRow);
                appendBuffer(output, size, finalRow, meta.isVariableSize());
            }
        }
        finalRow.clear();
        helper->setCallback(NULL);
    }

    virtual void processRow(CDummyMessagePacker &d) 
    {
        CriticalBlock c(parCrit);
        MemoryBuffer &m = d.data;

        RtlDynamicRowBuilder finalBuilder(rowAllocator, false);
        if (deserializer)
        {
            Owned<ISerialStream> stream = createMemoryBufferSerialStream(m);
            CThorStreamDeserializerSource rowSource(stream);

            while (m.remaining())
            {
                RecordLengthType *rowLen = (RecordLengthType *) m.readDirect(sizeof(RecordLengthType));
                if (!*rowLen)
                    break; 
                RecordLengthType len = *rowLen;
                RtlDynamicRowBuilder rowBuilder(rowAllocator);
                size_t outsize = deserializer->deserialize(rowBuilder, rowSource);
                if (!finalBuilder.exists())
                    finalBuilder.swapWith(rowBuilder);
                else
                {
                    const void * deserialized = rowBuilder.finalizeRowClear(outsize);
                    helper->mergeAggregate(finalBuilder, deserialized);
                    ReleaseRoxieRow(deserialized);
                }
            }
        }
        else
        {
            RecordLengthType len = meta.getFixedSize();
            while (m.remaining())
            {
                const void *row;
                if (!meta.isFixedSize())
                {
                    RecordLengthType *rowLen = (RecordLengthType *) m.readDirect(sizeof(RecordLengthType));
                    if (!*rowLen)
                        break; 
                    len = *rowLen;
                }
                row = m.readDirect(len);
                if (!finalBuilder.exists())
                    cloneRow(finalBuilder, row, meta);
                else
                    helper->mergeAggregate(finalBuilder, row);
            }
        }
        if (finalBuilder.exists())
        {
            size32_t finalSize = meta.getRecordSize(finalBuilder.getSelf());  // MORE - can probably track it above...
            finalRow.setown(finalBuilder.finalizeRowClear(finalSize));
        }
    }

};

class CRoxieDiskAggregateActivityFactory : public CRoxieDiskBaseActivityFactory
{
public:
    CRoxieDiskAggregateActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
        : CRoxieDiskBaseActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory)
    {
    }

    virtual IRoxieSlaveActivity *createActivity(SlaveContextLogger &logctx, IRoxieQueryPacket *packet) const
    {
        if (parallelAggregate > 1)
            return new CParallelRoxieDiskAggregateActivity(logctx, packet, helperFactory, this, manager, translators, parallelAggregate);
        else
            return new CRoxieDiskAggregateActivity(logctx, packet, helperFactory, this, manager, translators, 0, 1, false);
    }

    virtual StringBuffer &toString(StringBuffer &s) const
    {
        return CSlaveActivityFactory::toString(s.append("DiskRead "));
    }

};


//================================================================================================

// RecordProcessor used by Disk Aggregate activity.

class AggregateRecordProcessor : public RecordProcessor
{
public:
    AggregateRecordProcessor(CRoxieDiskAggregateActivity &_owner, IDirectReader *_reader)
    : RecordProcessor(_reader), owner(_owner)
    {
        helper = _owner.helper;
        helper->setCallback(reader);
    }

    virtual void doQuery(IMessagePacker *output, unsigned processed, unsigned __int64 rowLimit, unsigned __int64 stopAfter)
    {
        OptimizedRowBuilder rowBuilder(owner.rowAllocator, owner.meta, output, owner.serializer);
        helper->clearAggregate(rowBuilder);
        while (!aborted)
        {
            const byte *nextRec = reader->nextRow();
            if (!nextRec)
                break;
            helper->processRow(rowBuilder, nextRec);
            reader->finishedRow();
        }
        if (!aborted)
        {
            if (helper->processedAnyRows())
            {
                size32_t finalSize = owner.meta.getRecordSize(rowBuilder.getSelf());
                rowBuilder.writeToOutput(finalSize, true);
            }
        }
    }
protected:
    CRoxieDiskAggregateActivity &owner;
    IHThorDiskAggregateArg *helper;
};

IInMemoryFileProcessor *createAggregateRecordProcessor(CRoxieDiskAggregateActivity &_owner, IDirectReader *_reader)
{
    return new AggregateRecordProcessor(_owner, _reader);
}

ISlaveActivityFactory *createRoxieDiskAggregateActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
{
    return new CRoxieDiskAggregateActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory);
}


//================================================================================================

class CRoxieDiskGroupAggregateActivity : public CRoxieDiskReadBaseActivity
{
protected:
    IHThorDiskGroupAggregateArg *helper;
    RowAggregator results;

    void outputResults(IMessagePacker *output)
    {
        if (!aborted)
        {
            for (;;)
            {
                Owned<AggregateRowBuilder> next = results.nextResult();
                if (!next)
                    break;
                unsigned rowSize = next->querySize();
                OwnedConstRoxieRow row(next->finalizeRowClear());
                if (serializer)
                {
                    serializeRow(output, row);
                }
                else
                {
                    void *recBuffer = output->getBuffer(rowSize, meta.isVariableSize());
                    memcpy(recBuffer, row, rowSize);
                    output->putBuffer(recBuffer, rowSize, meta.isVariableSize());
                }
            }
        }
    }

public:
    CRoxieDiskGroupAggregateActivity(SlaveContextLogger &_logctx, IRoxieQueryPacket *_packet, HelperFactory *_hFactory, const CSlaveActivityFactory *_aFactory,
        IInMemoryIndexManager *_manager,
        ITranslatorSet *_translators,
        unsigned partNo, unsigned numParts, bool _forceUnkeyed)
        : CRoxieDiskReadBaseActivity(_logctx, _packet, _hFactory, _aFactory, _manager, _translators, partNo, numParts, _forceUnkeyed),
          helper((IHThorDiskGroupAggregateArg *) basehelper),
          results(*helper, *helper)
    {
        onCreate();
        results.start(rowAllocator, queryContext->queryCodeContext(), basefactory->queryId());
    }

    virtual bool needsRowAllocator()
    {
        return true;
    }

    virtual StringBuffer &toString(StringBuffer &ret) const
    {
        return ret.appendf("DiskGroupAggregate %u", packet->queryHeader().activityId);
    }

    virtual void doProcess(IMessagePacker * output)
    {
        {
            CriticalBlock p(pcrit);
            processor.setown(createGroupAggregateRecordProcessor(results, *helper, reader));
        }
        processor->doQuery(output, 0, 0, 0);
        if (!aborted)
            outputResults(output);
        results.reset();
    }

};

class CParallelRoxieDiskGroupAggregateActivity : public CParallelRoxieActivity
{
protected:
    IHThorDiskGroupAggregateArg *helper;
    RowAggregator resultAggregator;
    Owned<IRowManager> rowManager;

public:
    CParallelRoxieDiskGroupAggregateActivity(SlaveContextLogger &_logctx, IRoxieQueryPacket *_packet, HelperFactory *_hFactory, const CSlaveActivityFactory *_aFactory,
        IInMemoryIndexManager *_manager, ITranslatorSet *_translators, unsigned _numParallel) :
        CParallelRoxieActivity(_logctx, _packet, _hFactory, _aFactory, _numParallel),
        helper((IHThorDiskGroupAggregateArg *) basehelper),
        resultAggregator(*helper, *helper)
    {
        onCreate();
        resultAggregator.start(rowAllocator, queryContext->queryCodeContext(), basefactory->queryId());
        if (meta.needsSerializeDisk())
        {
            // MORE - avoiding serializing to dummy would be more efficient...
            deserializer.setown(meta.createDiskDeserializer(queryContext->queryCodeContext(), basefactory->queryId()));
        }
        CRoxieDiskGroupAggregateActivity *part0 = new CRoxieDiskGroupAggregateActivity(_logctx, _packet, _hFactory, _aFactory, _manager, _translators, 0, numParallel, false);
        parts.append(*part0);
        if (part0->queryKeyed())
        {
            numParallel = 1;
            part0->setParallel(0, 1);
        }
        else
        {
            for (unsigned i = 1; i < numParallel; i++)
                parts.append(*new CRoxieDiskGroupAggregateActivity(_logctx, _packet, _hFactory, _aFactory, _manager, _translators, i, numParallel, true));
        }
    }

    virtual bool needsRowAllocator()
    {
        return true;
    }

    virtual void doProcess(IMessagePacker *output)
    {
        if (!aborted)
        {
            for (;;)
            {
                Owned<AggregateRowBuilder> next = resultAggregator.nextResult();
                if (!next)
                    break;
                unsigned rowSize = next->querySize();
                OwnedConstRoxieRow row(next->finalizeRowClear());
                if (serializer)
                {
                    serializeRow(output, row);
                }
                else
                {
                    void *recBuffer = output->getBuffer(rowSize, meta.isVariableSize());
                    memcpy(recBuffer, row, rowSize);
                    output->putBuffer(recBuffer, rowSize, meta.isVariableSize());
                }
            }
        }
        resultAggregator.reset();
        helper->setCallback(NULL);
    }

    void processRow(CDummyMessagePacker &d)
    {
        CriticalBlock b(parCrit); // MORE - use a spinlock
        MemoryBuffer &m = d.data;
        Owned<ISerialStream> stream = createMemoryBufferSerialStream(m);
        CThorStreamDeserializerSource rowSource(stream);
        while (m.remaining())
        {
            const void *row;
            if (meta.isFixedSize() && !deserializer)
            {
                row = m.readDirect(meta.getFixedSize());
                resultAggregator.mergeElement(row);
            }
            else
            {
                RecordLengthType *rowLen = (RecordLengthType *) m.readDirect(sizeof(RecordLengthType));
                if (!*rowLen)
                    break; 
                RecordLengthType len = *rowLen;
                if (deserializer)
                {
                    RtlDynamicRowBuilder rowBuilder(rowAllocator);
                    size_t outsize = deserializer->deserialize(rowBuilder, rowSource);
                    OwnedConstRoxieRow deserialized = rowBuilder.finalizeRowClear(outsize);
                    resultAggregator.mergeElement(deserialized);
                }
                else
                {
                    row = m.readDirect(len);
                    resultAggregator.mergeElement(row);
                }
            }
        }
    }

};

class CRoxieDiskGroupAggregateActivityFactory : public CRoxieDiskBaseActivityFactory
{
public:
    CRoxieDiskGroupAggregateActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
        : CRoxieDiskBaseActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory)
    {
    }

    virtual IRoxieSlaveActivity *createActivity(SlaveContextLogger &logctx, IRoxieQueryPacket *packet) const
    {
        if (parallelAggregate > 1)
            return new CParallelRoxieDiskGroupAggregateActivity(logctx, packet, helperFactory, this, manager, translators, parallelAggregate);
        else
            return new CRoxieDiskGroupAggregateActivity(logctx, packet, helperFactory, this, manager, translators, 0, 1, false);
    }

    virtual StringBuffer &toString(StringBuffer &s) const
    {
        return CSlaveActivityFactory::toString(s.append("DiskRead "));
    }
};

//================================================================================================

// RecordProcessors used by Disk Group Aggregate activity.

class GroupAggregateRecordProcessor : public RecordProcessor, implements IHThorGroupAggregateCallback
{
public:
    IMPLEMENT_IINTERFACE;
    GroupAggregateRecordProcessor(RowAggregator &_results, IHThorDiskGroupAggregateArg &_helper, IDirectReader *_reader)
    : RecordProcessor(_reader), results(_results), helper(_helper)
    {
    }

    virtual void processRow(const void * next)
    {
        results.addRow(next);
    }

    virtual void doQuery(IMessagePacker *output, unsigned processed, unsigned __int64 rowLimit, unsigned __int64 stopAfter)
    {
        helper.setCallback(reader);
        while (!aborted)
        {
            const byte *nextRec = reader->nextRow();
            if (!nextRec)
                break;
            helper.processRow(nextRec, this);
            reader->finishedRow();
        }
    }

protected:
    RowAggregator &results;
    IHThorDiskGroupAggregateArg &helper;
};

IInMemoryFileProcessor *createGroupAggregateRecordProcessor(RowAggregator &results, IHThorDiskGroupAggregateArg &helper, IDirectReader *reader)
{
    return new GroupAggregateRecordProcessor(results, helper, reader);
}

ISlaveActivityFactory *createRoxieDiskGroupAggregateActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
{
    return new CRoxieDiskGroupAggregateActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory);
}


//================================================================================================

class CRoxieKeyedActivityFactory : public CSlaveActivityFactory
{
    friend class CRoxieKeyedActivity;
protected:
    Owned<IKeyArray> keyArray;
    Owned<ITranslatorSet> translators;
    unsigned projectedCrc = 0;
    unsigned expectedCrc = 0;
    IOutputMetaData *projectedMeta = nullptr;
    IOutputMetaData *expectedMeta = nullptr;

    CRoxieKeyedActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
        : CSlaveActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory)
    {
    }
};

class CRoxieIndexActivityFactory : public CRoxieKeyedActivityFactory
{
public:
    CRoxieIndexActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
        : CRoxieKeyedActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory)
    {
    }

    void init(IHThorIndexReadBaseArg * helper, IPropertyTree &graphNode)
    {
        projectedCrc = helper->getProjectedFormatCrc();
        expectedCrc = helper->getDiskFormatCrc();
        projectedMeta = helper->queryProjectedDiskRecordSize();
        expectedMeta = helper->queryDiskRecordSize();
        bool variableFileName = allFilesDynamic || queryFactory.isDynamic() || ((helper->getFlags() & (TIRvarfilename|TIRdynamicfilename)) != 0);
        if (!variableFileName)
        {
            bool isOpt = (helper->getFlags() & TIRoptional) != 0;
            OwnedRoxieString indexName(helper->getFileName());
            datafile.setown(queryFactory.queryPackage().lookupFileName(indexName, isOpt, true, true, queryFactory.queryWorkUnit(), true));
            if (datafile)
            {
                translators.setown(datafile->getTranslators(projectedCrc, projectedMeta, expectedCrc, expectedMeta, queryFactory.queryOptions().enableFieldTranslation, true));
                keyArray.setown(datafile->getKeyArray(isOpt, queryFactory.queryChannel()));
            }
        }
    }
};

class CRoxieKeyedActivity : public CRoxieSlaveActivity
{
    // Common base class for all activities that deal with keys - keyed join or indexread and its allies
protected:
    Owned<IKeyManager> tlk;
    Linked<ITranslatorSet> translators;
    Linked<IKeyArray> keyArray;
    const RtlRecord *keyRecInfo = nullptr;
    bool createSegmentMonitorsPending;
    virtual bool hasNewSegmentMonitors() = 0;

    virtual void createSegmentMonitors() = 0;
    virtual void setPartNo(bool filechanged)
    {
        if (!lastPartNo.partNo)  // Check for ,LOCAL indexes
        {
            assertex(filechanged);
            Owned<IKeyIndexSet> allKeys = createKeyIndexSet();
            for (unsigned subpart = 0; subpart < keyArray->length(); subpart++)
            {
                IKeyIndexBase *kib = keyArray->queryKeyPart(subpart);
                if (kib)
                {
                    IKeyIndex *k = kib->queryPart(lastPartNo.fileNo);
                    if (k)
                    {
                        assertex(!k->isTopLevelKey());
                        allKeys->addIndex(LINK(k));
                    }
                }
            }
            if (allKeys->numParts())
            {
                tlk.setown(createKeyMerger(*keyRecInfo, allKeys, 0, &logctx, hasNewSegmentMonitors()));
                createSegmentMonitorsPending = true;
            }
            else
                tlk.clear();
        }
        else
        {
            IKeyIndexBase *kib = keyArray->queryKeyPart(lastPartNo.partNo);
            assertex(kib != NULL);
            IKeyIndex *k = kib->queryPart(lastPartNo.fileNo);
            if (filechanged)
            {
                tlk.setown(createLocalKeyManager(*keyRecInfo, k, &logctx, hasNewSegmentMonitors()));
                createSegmentMonitorsPending = true;
            }
            else
                tlk->setKey(k);
        }
    }

    virtual void setVariableFileInfo()
    {
        const CRoxieKeyedActivityFactory &aFactory = *static_cast<const CRoxieKeyedActivityFactory *>(basefactory);
        translators.setown(varFileInfo->getTranslators(aFactory.projectedCrc, aFactory.projectedMeta, aFactory.expectedCrc, aFactory.expectedMeta, allowFieldTranslation, true));
        keyArray.setown(varFileInfo->getKeyArray(isOpt, packet->queryHeader().channel));
    }

    void noteStats(unsigned accepted, unsigned rejected)
    {
        // Note that the key-level statistics (seeks, scans, skips etc) are handled inside jhtree
        logctx.noteStatistic(StNumIndexAccepted, accepted);
        logctx.noteStatistic(StNumIndexRejected, rejected);
        logctx.noteStatistic(StNumIndexRowsRead, accepted+rejected);
    }

    CRoxieKeyedActivity(SlaveContextLogger &_logctx, IRoxieQueryPacket *_packet, HelperFactory *_hFactory, const CRoxieKeyedActivityFactory *_aFactory)
        : CRoxieSlaveActivity(_logctx, _packet, _hFactory, _aFactory), 
        keyArray(_aFactory->keyArray),
        translators(_aFactory->translators),
        createSegmentMonitorsPending(true)
    {
    }

};

class CRoxieIndexActivity : public CRoxieKeyedActivity
{
    // Common base class for indexread, indexcount and related activities

protected:
    PartNoType *inputData;  // list of channels
    IHThorIndexReadBaseArg * indexHelper;
    unsigned inputCount;
    unsigned inputsDone;
    unsigned processed;
    unsigned keyprocessed;
    unsigned steppingOffset;
    unsigned steppingLength;
    unsigned short numSkipFields;
    unsigned numSeeks;
    bool seeksAreEof;
    bool lastRowCompleteMatch;
    CIndexTransformCallback callback;

    SmartStepExtra stepExtra; // just used for flags - a little unnecessary...
    const byte *steppingRow;

    virtual bool hasNewSegmentMonitors() { return indexHelper->hasNewSegmentMonitors(); }

    bool checkLimit(unsigned __int64 limit)
    {
        assertex(!resent);
        unsigned __int64 result = 0;
        unsigned inputsDone = 0;
        bool ret = true;
        unsigned saveStepping = steppingOffset;  // Avoid using a stepping keymerger for the checkCount - they don't support it (and would be inefficient)
        steppingOffset = 0;
        while (!aborted && inputsDone < inputCount)
        {
            checkPartChanged(inputData[inputsDone]);
            if (tlk)
            {
                createSegmentMonitors();
                result += tlk->checkCount(limit-result);
                if (result > limit)
                {
                    ret = false;
                    break;
                }
            }
            inputsDone++;
        }
        if (saveStepping)
        {
            steppingOffset = saveStepping;
            lastPartNo.partNo = 0xffff;
            lastPartNo.fileNo = 0xffff;
            tlk.clear();
        }
        return ret;
    }

public:
    CRoxieIndexActivity(SlaveContextLogger &_logctx, IRoxieQueryPacket *_packet, HelperFactory *_hFactory, const CRoxieIndexActivityFactory *_aFactory, unsigned _steppingOffset)
        : CRoxieKeyedActivity(_logctx, _packet, _hFactory, _aFactory),
        steppingOffset(_steppingOffset),
        stepExtra(SSEFreadAhead, NULL)
    {
        indexHelper = (IHThorIndexReadBaseArg *) basehelper;
        keyRecInfo = &indexHelper->queryDiskRecordSize()->queryRecordAccessor(true);
        variableFileName = allFilesDynamic || basefactory->queryQueryFactory().isDynamic() || ((indexHelper->getFlags() & (TIRvarfilename|TIRdynamicfilename)) != 0);
        isOpt = (indexHelper->getFlags() & TDRoptional) != 0;
        inputData = NULL;
        inputCount = 0;
        inputsDone = 0;
        processed = 0;
        keyprocessed = 0;
        numSkipFields = 0;
        lastRowCompleteMatch = true; // default is we only return complete matches....
        seeksAreEof = false;
        steppingLength = 0;
        steppingRow = NULL;
        numSeeks = 0;
        if (packet->getSmartStepInfoLength())
        {
            const byte *smartStepInfoValue = packet->querySmartStepInfoData();
            numSkipFields = * (unsigned short *) smartStepInfoValue;
            smartStepInfoValue += sizeof(unsigned short);
            steppingLength = * (unsigned short *) smartStepInfoValue;
            smartStepInfoValue += sizeof(unsigned short);
            unsigned flags = * (unsigned short *) smartStepInfoValue;
            smartStepInfoValue += sizeof(unsigned short);
            seeksAreEof = * (bool *) smartStepInfoValue;
            smartStepInfoValue += sizeof(bool);
            numSeeks = * (unsigned *) smartStepInfoValue;
            smartStepInfoValue += sizeof(unsigned);
            assertex(numSeeks); // Given that we put the first seek in here to there should always be at least one!
            steppingRow = smartStepInfoValue; // the first of them...
            stepExtra.set(flags, NULL);
            if (logctx.queryTraceLevel() > 10)
            {
                logctx.CTXLOG("%d seek rows provided. mismatch(%d) readahead(%d) onlyfirst(%d)", numSeeks,
                       (int)stepExtra.returnMismatches(), (int)stepExtra.readAheadManyResults(), (int)stepExtra.onlyReturnFirstSeekMatch());

                if (logctx.queryTraceLevel() > 15)
                {
                    for (unsigned i = 0; i < numSeeks; i++)
                    {
                        StringBuffer b;
                        for (unsigned j = 0; j < steppingLength; j++)
                            b.appendf("%02x ", steppingRow[i*steppingLength + j]);
                        logctx.CTXLOG("Seek row %d: %s", i+1, b.str());
                    }
                }
            }
        }
        else
        {
            if (logctx.queryTraceLevel() > 10)
                logctx.CTXLOG("0 seek rows provided.");
        }
    }

    virtual void onCreate()
    {
        CRoxieKeyedActivity::onCreate();
        inputData = (PartNoType *) serializedCreate.readDirect(0);
        inputCount = (serializedCreate.length() - serializedCreate.getPos()) / sizeof(*inputData);
        indexHelper->setCallback(&callback);
    }

    virtual const char *queryDynamicFileName() const
    {
        return indexHelper->getFileName();
    }

    virtual void createSegmentMonitors()
    {
        if (createSegmentMonitorsPending)
        {
            createSegmentMonitorsPending = false;
            tlk->setLayoutTranslator(translators->queryTranslator(lastPartNo.fileNo));
            indexHelper->createSegmentMonitors(tlk);
            tlk->finishSegmentMonitors();
        }
    }

    bool sendContinuation(IMessagePacker * output)
    {
        MemoryBuffer si;
        unsigned short siLen = 0;
        si.append(siLen);
        si.append(lastRowCompleteMatch);
        si.append(inputsDone);
        si.append(processed);
        si.append(keyprocessed);
        si.append(lastPartNo.partNo);
        si.append(lastPartNo.fileNo);
        tlk->serializeCursorPos(si);
        if (si.length() <= maxContinuationSize)
        {
            siLen = si.length() - sizeof(siLen);
            si.writeDirect(0, sizeof(siLen), &siLen);
            output->sendMetaInfo(si.toByteArray(), si.length());
            return true;
        }
        else
            return false;
    }

    void readContinuationInfo()
    {
        resentInfo.read(lastRowCompleteMatch);
        resentInfo.read(inputsDone);
        resentInfo.read(processed);
        resentInfo.read(keyprocessed);
        resentInfo.read(lastPartNo.partNo);
        resentInfo.read(lastPartNo.fileNo);
        setPartNo(true);  
        tlk->deserializeCursorPos(resentInfo);
        assertex(resentInfo.remaining() == 0);
    }

    virtual void setPartNo(bool fileChanged)
    {
        // NOTE - may be used by both indexread and normalize...
        if (steppingOffset) // MORE - may be other cases too - eg want output sorted and there are multiple subfiles...
        {
            unsigned i = 0;
            Owned<IKeyIndexSet> allKeys = createKeyIndexSet();
            while (!aborted && i < inputCount)
            {
                PartNoType &part = inputData[i];
                lastPartNo.partNo = part.partNo;
                lastPartNo.fileNo = part.fileNo; // This is a hack so that the translator can be retrieved. We don't support record translation properly when doing keymerging...)
                // MORE - this code looks like it could be commoned up with code in CRoxieKeyedActivity::setPartNo
                if (!lastPartNo.partNo)
                {
                    for (unsigned subpart = 0; subpart < keyArray->length(); subpart++)
                    {
                        IKeyIndexBase *kib = keyArray->queryKeyPart(subpart);
                        if (kib)
                        {
                            IKeyIndex *k = kib->queryPart(lastPartNo.fileNo);
                            if (k)
                            {
                                assertex(!k->isTopLevelKey());
                                allKeys->addIndex(LINK(k));
                            }
                        }
                    }
                }
                else
                {
                    IKeyIndexBase *kib = keyArray->queryKeyPart(part.partNo);
                    assertex(kib != NULL);
                    IKeyIndex *k = kib->queryPart(part.fileNo);
                    allKeys->addIndex(LINK(k));
                }
                i++;
            }
            if (allKeys->numParts())
                tlk.setown(::createKeyMerger(*keyRecInfo, allKeys, steppingOffset, &logctx, hasNewSegmentMonitors()));
            else
                tlk.clear();
            createSegmentMonitorsPending = true;
        }
        else
            CRoxieKeyedActivity::setPartNo(fileChanged);
    }
};

//================================================================================================

class CRoxieIndexReadActivity : public CRoxieIndexActivity, implements IIndexReadActivityInfo
{
protected:
    IHThorIndexReadArg * readHelper;

public:
    CRoxieIndexReadActivity(SlaveContextLogger &_logctx, IRoxieQueryPacket *_packet, HelperFactory *_hFactory, const CRoxieIndexActivityFactory *_aFactory, unsigned _steppingOffset)
        : CRoxieIndexActivity(_logctx, _packet, _hFactory, _aFactory, _steppingOffset)
    {
        onCreate();
        readHelper = (IHThorIndexReadArg *) basehelper;
        if (resent)
            readContinuationInfo();
    }

    virtual StringBuffer &toString(StringBuffer &ret) const
    {
        return ret.appendf("IndexRead %u", packet->queryHeader().activityId);
    }

/* Notes on Global smart stepping implementation:

  When smart stepping, I get from the Roxie server:
   1 or more seek positions 
   some flags
  
  I can read from the index in an order that matches the seek positions order (because of index merger), and which match my hard filter (they may not match my postfilter)
  I can skip forwards in the index to the first record GE a skip position
  I am not going to try to implement a (possible future) flag to return mismatches after any but the last of the seek positions (yet)

  I want to return M matching (keyed matches, seek field matches, and postfilter matches) rows for all provided seek positions
      where M is 1 if SSEFonlyReturnFirstSeekMatch flag set, otherwise all...

  THEN once we are beyond the last provided seek:
    if returnMismatches flag set,                 
       current row = (next row matching keyed filter)
       if someNewAsYetUnnamedAndUnimplementedFlag flag set 
           //if the post filter matches then there may be scope for returning all records which match the seek fields of that first match
           // But not very likely to help unless terms correlated, and may well hinder
           return current row and all following rows matching keyed filter, seek data from current row, and postfilter
       return next row matching keyed filter (even if doesn't match postfilter)
    else
       return next N rows matching keyed filter and postfilter (where N depends on readAheadManyResults flag - 1 if not set)
*/
    inline void advanceToNextSeek()
    {
        assertex(numSeeks != 0);
        if (--numSeeks)
            steppingRow += steppingLength;
        else
            steppingRow = NULL;
    }

    virtual IMessagePacker *process()
    {
        MTIME_SECTION(queryActiveTimer(), "CRoxieIndexReadActivity ::process");
        unsigned __int64 keyedLimit = readHelper->getKeyedLimit();
        unsigned __int64 limit = readHelper->getRowLimit();

        if (!resent && (keyedLimit != (unsigned __int64) -1) && ((keyedLimit > preabortIndexReadsThreshold) || (indexHelper->getFlags() & TIRcountkeyedlimit) != 0)) // Don't recheck the limit every time!
        {
            if (!checkLimit(keyedLimit))
            {
                limitExceeded(true); 
                return NULL;
            }

        }
        unsigned __int64 stopAfter = readHelper->getChooseNLimit();

        Owned<IMessagePacker> output = ROQ->createOutputStream(packet->queryHeader(), false, logctx);
        OptimizedRowBuilder rowBuilder(rowAllocator, meta, output, serializer);

        unsigned totalSizeSent = 0;
        unsigned skipped = 0;

        unsigned processedBefore = processed;
        unsigned keyprocessedBefore = keyprocessed;
        bool continuationFailed = false;
        const byte *rawSeek = NULL;
        if (steppingRow)
            rawSeek = steppingRow;
        bool continuationNeeded = false;
        ScopedAtomic<unsigned> indexRecordsRead(::indexRecordsRead);
        ScopedAtomic<unsigned> postFiltered(::postFiltered);
        while (!aborted && inputsDone < inputCount)
        {
            if (!resent || !steppingOffset)     // Bit of a hack... In the resent case, we have already set up the tlk, and all keys are processed at once in the steppingOffset case (which makes checkPartChanged gives a false positive in this case)
                checkPartChanged(inputData[inputsDone]);
            if (tlk)
            {
                createSegmentMonitors();
                tlk->reset(resent);
                resent = false;
                {
                    TransformCallbackAssociation associate(callback, tlk); // want to destroy this before we advance to next key...
                    while (!aborted && (rawSeek ? tlk->lookupSkip(rawSeek, steppingOffset, steppingLength) : tlk->lookup(true)))
                    {
                        rawSeek = NULL;  // only want to do the seek first time we look for a particular seek value
                        keyprocessed++;
                        if ((keyedLimit != (unsigned __int64) -1) && keyprocessed > keyedLimit)
                        {
                            noteStats(keyprocessed-keyprocessedBefore, skipped);
                            limitExceeded(true);
                            break;
                        }

                        indexRecordsRead++;
                        size32_t transformedSize;
                        const byte * keyRow = tlk->queryKeyBuffer();
                        int diff = 0;
                        if (steppingRow)
                        {
                            diff = memcmp(keyRow+steppingOffset, steppingRow, steppingLength);
                            assertex(diff >= 0);
                        }
                        while (diff > 0)
                        {
                            advanceToNextSeek();
                            if (!steppingRow)
                                break;
                            diff = memcmp(keyRow+steppingOffset, steppingRow, steppingLength);
                            if (diff < 0)
                            {
                                rawSeek = steppingRow;
                                break;
                            }
                        }
                        if (diff >= 0)
                        {
                            if (diff > 0 && seeksAreEof)
                            {
                                assertex(!steppingRow);
                                break;
                            }

                            rowBuilder.ensureRow();
                            transformedSize = 0;
                            if (likely(readHelper->canMatch(keyRow)))
                                transformedSize = readHelper->transform(rowBuilder, keyRow);

                            callback.finishedRow();
                            if (transformedSize)
                            {
                                if (logctx.queryTraceLevel() > 15)
                                {
                                    StringBuffer b;
                                    for (unsigned j = 0; j < (steppingLength ? steppingLength : 6); j++)
                                        b.appendf("%02x ", keyRow[steppingOffset + j]);
                                    logctx.CTXLOG("Returning seek row %s", b.str());
                                }

                                // Did get a match
                                processed++;
                                if (limit && processed > limit)
                                {
                                    noteStats(keyprocessed-keyprocessedBefore, skipped);
                                    limitExceeded(false); 
                                    break;
                                }
                                if (processed > stopAfter)
                                {
                                    noteStats(keyprocessed-keyprocessedBefore, skipped);
                                    return output.getClear();
                                }
                                rowBuilder.writeToOutput(transformedSize, true);

                                totalSizeSent += transformedSize;
                                if (totalSizeSent > indexReadChunkSize || (steppingOffset && !steppingRow && !stepExtra.readAheadManyResults()))
                                    continuationNeeded = true;
                                lastRowCompleteMatch = true;
                                if (steppingRow && stepExtra.onlyReturnFirstSeekMatch())
                                    advanceToNextSeek();
                            }
                            else
                            {
                                // Didn't get a match
                                if (steppingOffset && !steppingRow && stepExtra.returnMismatches())
                                {
                                    transformedSize = readHelper->unfilteredTransform(rowBuilder, keyRow);
                                    if (transformedSize) // will only be zero in odd situations where codegen can't work out how to transform (eg because of a skip)
                                    {
                                        callback.finishedRow();
                                        rowBuilder.writeToOutput(transformedSize, true);

                                        totalSizeSent += transformedSize;
                                        continuationNeeded = true;
                                        lastRowCompleteMatch = false;
                                    }
                                }                           
                                else
                                {
                                    postFiltered++;
                                    skipped++;
                                }
                            }
                        }
                        if (continuationNeeded && !continuationFailed)
                        {
                            if (logctx.queryTraceLevel() > 10)
                                logctx.CTXLOG("Indexread returning partial result set %d rows from %d seeks, %d scans, %d skips", processed-processedBefore, tlk->querySeeks(), tlk->queryScans(), tlk->querySkips());
                            if (sendContinuation(output))
                            {
                                noteStats(keyprocessed-keyprocessedBefore, skipped);
                                return output.getClear();
                            }
                            else
                            {
                                // This is actually pretty fatal for smart-stepping case
                                if (logctx.queryTraceLevel())
                                    logctx.CTXLOG("Indexread unable to return partial result set");
                                continuationFailed = true;
                            }
                        }
                        rowBuilder.clear();
                    }
                }
            }
            if (steppingOffset)
                inputsDone = inputCount;
            else
                inputsDone++;
        }
        if (tlk) // a very early abort can mean it is NULL.... MORE is this the right place to put it or should it be inside the loop??
        {
            if (logctx.queryTraceLevel() > 10 && !aborted)
            {
                logctx.CTXLOG("Indexread returning result set %d rows from %d seeks, %d scans, %d skips", processed-processedBefore, tlk->querySeeks(), tlk->queryScans(), tlk->querySkips());
                if (steppingOffset)
                    logctx.CTXLOG("Indexread return: steppingOffset %d, steppingRow %p, stepExtra.returnMismatches() %d",steppingOffset, steppingRow, (int) stepExtra.returnMismatches());
            }
            noteStats(keyprocessed-keyprocessedBefore, skipped);
        }
        if (aborted)
            return NULL;
        else
            return output.getClear();
    }

    virtual IIndexReadActivityInfo *queryIndexReadActivity() 
    {
        return this;
    }

    virtual IKeyArray *getKeySet() const 
    {
        return keyArray.getLink();
    }
    virtual const IResolvedFile *getVarFileInfo() const 
    {
        return varFileInfo.getLink(); 
    }
    virtual ITranslatorSet *getTranslators() const override
    {
        return translators.getLink();
    }
    virtual void mergeSegmentMonitors(IIndexReadContext *irc) const 
    {
        indexHelper->createSegmentMonitors(irc); // NOTE: they will merge; 
    }
    virtual IRoxieServerActivity *queryActivity() { throwUnexpected(); }
    virtual const RemoteActivityId &queryRemoteId() const { throwUnexpected(); }
};

//================================================================================================

class CRoxieIndexReadActivityFactory : public CRoxieIndexActivityFactory
{
    unsigned steppingOffset;

public:
    CRoxieIndexReadActivityFactory(IPropertyTree &graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
        : CRoxieIndexActivityFactory(graphNode, _subgraphId, _queryFactory, _helperFactory)
    {
        Owned<IHThorIndexReadArg> helper = (IHThorIndexReadArg *) helperFactory();
        init(helper, graphNode);
        ISteppingMeta *rawMeta = helper->queryRawSteppingMeta();
        if (rawMeta)
        {
            // MORE - should check all keys in maxFields list can actually be keyed.
            const CFieldOffsetSize * fields = rawMeta->queryFields();
            steppingOffset = fields[0].offset;
        }
        else
        {
            steppingOffset = 0;
        }
    }
    
    virtual IRoxieSlaveActivity *createActivity(SlaveContextLogger &logctx, IRoxieQueryPacket *packet) const
    {
        return new CRoxieIndexReadActivity(logctx, packet, helperFactory, this, steppingOffset);
    }

    virtual StringBuffer &toString(StringBuffer &s) const
    {
        return CSlaveActivityFactory::toString(s.append("INDEXREAD "));
    }
};

ISlaveActivityFactory *createRoxieIndexReadActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
{
    return new CRoxieIndexReadActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory);
}

//================================================================================================

//MORE: Very similar to the indexRead code, but I'm not sure it's worth commoning up....
class CRoxieIndexNormalizeActivity : public CRoxieIndexActivity
{
protected:
    IHThorCompoundNormalizeExtra * normalizeHelper;

public:
    CRoxieIndexNormalizeActivity(SlaveContextLogger &_logctx, IRoxieQueryPacket *_packet, HelperFactory *_hFactory, const CRoxieIndexActivityFactory *_aFactory)
        : CRoxieIndexActivity(_logctx, _packet, _hFactory, _aFactory, 0) //MORE - stepping?
    {
        onCreate();
        normalizeHelper = (IHThorIndexNormalizeArg *) basehelper;
        if (resent)
            readContinuationInfo();
    }

    virtual StringBuffer &toString(StringBuffer &ret) const
    {
        return ret.appendf("IndexNormalize %u", packet->queryHeader().activityId);
    }

    virtual IMessagePacker *process()
    {
        MTIME_SECTION(queryActiveTimer(), "CRoxieIndexNormalizeActivity ::process");
        unsigned __int64 keyedLimit = normalizeHelper->getKeyedLimit(); 
        unsigned __int64 rowLimit = normalizeHelper->getRowLimit();

        if (!resent && (keyedLimit != (unsigned __int64) -1) && (indexHelper->getFlags() & TIRcountkeyedlimit) != 0) // Don't recheck the limit every time!
        {
            if (!checkLimit(keyedLimit))
            {
                limitExceeded(true); 
                return NULL;
            }

        }
        unsigned __int64 stopAfter = normalizeHelper->getChooseNLimit();

        Owned<IMessagePacker> output = ROQ->createOutputStream(packet->queryHeader(), false, logctx);
        OptimizedRowBuilder rowBuilder(rowAllocator, meta, output, serializer);
        unsigned totalSizeSent = 0;
        unsigned skipped = 0;

        unsigned processedBefore = processed;
        ScopedAtomic<unsigned> indexRecordsRead(::indexRecordsRead);
        ScopedAtomic<unsigned> postFiltered(::postFiltered);
        bool continuationFailed = false;
        while (!aborted && inputsDone < inputCount)
        {
            checkPartChanged(inputData[inputsDone]);
            if (tlk)
            {
                createSegmentMonitors();
                tlk->reset(resent);
                resent = false;

                TransformCallbackAssociation associate(callback, tlk);
                while (!aborted && tlk->lookup(true))
                {
                    keyprocessed++;
                    if (keyedLimit && processed > keyedLimit)
                    {
                        noteStats(processed-processedBefore, skipped);
                        limitExceeded(true);
                        break;
                    }

                    indexRecordsRead++;
                    if (normalizeHelper->first(tlk->queryKeyBuffer()))
                    {
                        do
                        {
                            rowBuilder.ensureRow();
                            size32_t transformedSize = normalizeHelper->transform(rowBuilder);
                            if (transformedSize)
                            {
                                processed++;
                                if (processed > rowLimit)
                                {
                                    limitExceeded(false); 
                                    break;
                                }
                                if (processed > stopAfter)
                                {
                                    noteStats(processed-processedBefore, skipped);
                                    return output.getClear();
                                }

                                totalSizeSent += rowBuilder.writeToOutput(transformedSize, true);
                            }
                        } while (normalizeHelper->next());
                        callback.finishedRow();

                        if (totalSizeSent > indexReadChunkSize && !continuationFailed)
                        {
                            if (sendContinuation(output))
                            {
                                noteStats(processed-processedBefore, skipped);
                                return output.getClear();
                            }
                            else
                                continuationFailed = true;
                        }
                    }
                    else
                    {
                        postFiltered++;
                        skipped++;
                    }
                }
            }
            inputsDone++;
        }
        noteStats(processed-processedBefore, skipped);
        if (aborted)
            return NULL;
        else
            return output.getClear();
    }
};

//================================================================================================

class CRoxieIndexNormalizeActivityFactory : public CRoxieIndexActivityFactory
{
public:
    CRoxieIndexNormalizeActivityFactory(IPropertyTree &graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
        : CRoxieIndexActivityFactory(graphNode, _subgraphId, _queryFactory, _helperFactory)
    {
        Owned<IHThorIndexNormalizeArg> helper = (IHThorIndexNormalizeArg *) helperFactory();
        init(helper, graphNode);
    }

    virtual IRoxieSlaveActivity *createActivity(SlaveContextLogger &logctx, IRoxieQueryPacket *packet) const
    {
        return new CRoxieIndexNormalizeActivity(logctx, packet, helperFactory, this);
    }

    virtual StringBuffer &toString(StringBuffer &s) const
    {
        return CSlaveActivityFactory::toString(s.append("IndexNormalize "));
    }
};

ISlaveActivityFactory *createRoxieIndexNormalizeActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
{
    return new CRoxieIndexNormalizeActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory);
}

//================================================================================================

class CRoxieIndexCountActivity : public CRoxieIndexActivity
{
protected:
    IHThorIndexCountArg * countHelper;
    unsigned __int64 choosenLimit;
    unsigned __int64 rowLimit;
    unsigned __int64 keyedLimit;

public:
    CRoxieIndexCountActivity(SlaveContextLogger &_logctx, IRoxieQueryPacket *_packet, HelperFactory *_hFactory, const CRoxieIndexActivityFactory *_aFactory)
        : CRoxieIndexActivity(_logctx, _packet, _hFactory, _aFactory, 0)
    {
        onCreate();
        countHelper = (IHThorIndexCountArg *) basehelper;
        assertex(!resent);
        choosenLimit = countHelper->getChooseNLimit();
        rowLimit = countHelper->getRowLimit();
        keyedLimit = countHelper->getKeyedLimit();
    }

    virtual StringBuffer &toString(StringBuffer &ret) const
    {
        return ret.appendf("IndexCount %u", packet->queryHeader().activityId);
    }

    virtual IMessagePacker *process()
    {
        MTIME_SECTION(queryActiveTimer(), "CRoxieIndexCountActivity ::process");
        Owned<IMessagePacker> output = ROQ->createOutputStream(packet->queryHeader(), false, logctx);
        unsigned skipped = 0;

        unsigned processedBefore = processed;
        unsigned __int64 count = 0;
        ScopedAtomic<unsigned> indexRecordsRead(::indexRecordsRead);
        while (!aborted && inputsDone < inputCount && count < choosenLimit)
        {
            checkPartChanged(inputData[inputsDone]);
            if (tlk)
            {
                createSegmentMonitors();
                tlk->reset(false);
                if (countHelper->hasFilter())
                {
                    callback.setManager(tlk);
                    while (!aborted && (count < choosenLimit) && tlk->lookup(true))
                    {
                        keyprocessed++;
                        indexRecordsRead++;
                        count += countHelper->numValid(tlk->queryKeyBuffer());
                        if (count > rowLimit)
                            limitExceeded(false);
                        else if (count > keyedLimit)
                            limitExceeded(true);
                        callback.finishedRow();
                    }
                    callback.setManager(NULL);
                }
                else
                {
                    //MORE: GH->RKC There should be value in providing a choosenLimit to getCount()
                    //MORE: note that tlk->checkCount() is NOT suitable as it makes assumptions about the segmonitors (only checks leading ones)
                    count += tlk->getCount();
                    if (count > rowLimit)
                        limitExceeded(false);
                    else if (count > keyedLimit)
                        limitExceeded(true); // MORE - is this right?
                }
            }
            inputsDone++;
        }

        if (!aborted && count)
        {
            if (count > choosenLimit)
                count = choosenLimit;

            processed++;
            assertex(!serializer);
            void *recBuffer = output->getBuffer(meta.getFixedSize(), false);
            if (meta.getFixedSize() == 1)
                *(byte *)recBuffer = (byte)count;
            else
            {
                assertex(meta.getFixedSize() == sizeof(unsigned __int64));
                *(unsigned __int64 *)recBuffer = count;
            }
            output->putBuffer(recBuffer, meta.getFixedSize(), false);
        }
        noteStats(processed-processedBefore, skipped);
        if (aborted)
            return NULL;
        else
            return output.getClear();
    }
};

//================================================================================================

class CRoxieIndexCountActivityFactory : public CRoxieIndexActivityFactory
{
public:
    CRoxieIndexCountActivityFactory(IPropertyTree &graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
        : CRoxieIndexActivityFactory(graphNode, _subgraphId, _queryFactory, _helperFactory)
    {
        Owned<IHThorIndexCountArg> helper = (IHThorIndexCountArg *) helperFactory();
        init(helper, graphNode);
    }

    virtual IRoxieSlaveActivity *createActivity(SlaveContextLogger &logctx, IRoxieQueryPacket *packet) const
    {
        return new CRoxieIndexCountActivity(logctx, packet, helperFactory, this);
    }

    virtual StringBuffer &toString(StringBuffer &s) const
    {
        return CSlaveActivityFactory::toString(s.append("INDEXCOUNT "));
    }
};

ISlaveActivityFactory *createRoxieIndexCountActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
{
    return new CRoxieIndexCountActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory);
}

//================================================================================================

class CRoxieIndexAggregateActivity : public CRoxieIndexActivity
{
protected:
    IHThorCompoundAggregateExtra * aggregateHelper;

public:
    CRoxieIndexAggregateActivity(SlaveContextLogger &_logctx, IRoxieQueryPacket *_packet, HelperFactory *_hFactory, const CRoxieIndexActivityFactory *_aFactory)
        : CRoxieIndexActivity(_logctx, _packet, _hFactory, _aFactory, 0)
    {
        onCreate();
        aggregateHelper = (IHThorIndexAggregateArg *) basehelper;
        assertex(!resent);
    }
    virtual StringBuffer &toString(StringBuffer &ret) const
    {
        return ret.appendf("IndexAggregate %u", packet->queryHeader().activityId);
    }

    virtual IMessagePacker *process()
    {
        MTIME_SECTION(queryActiveTimer(), "CRoxieIndexAggregateActivity ::process");
        Owned<IMessagePacker> output = ROQ->createOutputStream(packet->queryHeader(), false, logctx);

        OptimizedRowBuilder rowBuilder(rowAllocator, meta, output, serializer);

        rowBuilder.ensureRow();
        aggregateHelper->clearAggregate(rowBuilder);
        unsigned skipped = 0;

        unsigned processedBefore = processed;
        ScopedAtomic<unsigned> indexRecordsRead(::indexRecordsRead);
        while (!aborted && inputsDone < inputCount)
        {
            checkPartChanged(inputData[inputsDone]);
            if (tlk)
            {
                createSegmentMonitors();
                tlk->reset(false);
                callback.setManager(tlk);
                while (!aborted && tlk->lookup(true))
                {
                    keyprocessed++;
                    indexRecordsRead++;
                    aggregateHelper->processRow(rowBuilder, tlk->queryKeyBuffer());
                    callback.finishedRow();
                }
                callback.setManager(NULL);
            }
            inputsDone++;
        }

        if (!aborted && aggregateHelper->processedAnyRows())
        {
            processed++;
            size32_t transformedSize = meta.getRecordSize(rowBuilder.getSelf());
            rowBuilder.writeToOutput(transformedSize, true);
        }
        noteStats(processed-processedBefore, skipped);
        if (aborted)
            return NULL;
        else
            return output.getClear();
    }
};

//================================================================================================

class CRoxieIndexAggregateActivityFactory : public CRoxieIndexActivityFactory
{
public:
    CRoxieIndexAggregateActivityFactory(IPropertyTree &graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
        : CRoxieIndexActivityFactory(graphNode, _subgraphId, _queryFactory, _helperFactory)
    {
        Owned<IHThorIndexAggregateArg> helper = (IHThorIndexAggregateArg *) helperFactory();
        init(helper, graphNode);
    }

    virtual IRoxieSlaveActivity *createActivity(SlaveContextLogger &logctx, IRoxieQueryPacket *packet) const
    {
        return new CRoxieIndexAggregateActivity(logctx, packet, helperFactory, this);
    }

    virtual StringBuffer &toString(StringBuffer &s) const
    {
        return CSlaveActivityFactory::toString(s.append("INDEXAGGREGATE "));
    }
};

ISlaveActivityFactory *createRoxieIndexAggregateActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
{
    return new CRoxieIndexAggregateActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory);
}

//================================================================================================

class CRoxieIndexGroupAggregateActivity : public CRoxieIndexActivity, implements IHThorGroupAggregateCallback
{
protected:
    IHThorCompoundGroupAggregateExtra * aggregateHelper;
    RowAggregator results;
    unsigned groupSegCount;
    ThorActivityKind kind;

public:
    IMPLEMENT_IINTERFACE_USING(CRoxieIndexActivity)

    CRoxieIndexGroupAggregateActivity(SlaveContextLogger &_logctx, IRoxieQueryPacket *_packet, HelperFactory *_hFactory, const CRoxieIndexActivityFactory *_aFactory, ThorActivityKind _kind)
        : CRoxieIndexActivity(_logctx, _packet, _hFactory, _aFactory, 0),
          aggregateHelper((IHThorIndexGroupAggregateArg *) basehelper),
          results(*aggregateHelper, *aggregateHelper), kind(_kind)
    {
        onCreate();
        results.start(rowAllocator, queryContext->queryCodeContext(), basefactory->queryId());
        assertex(!resent);
        groupSegCount = 0;
    }

    virtual bool needsRowAllocator()
    {
        return true;
    }

    virtual StringBuffer &toString(StringBuffer &ret) const
    {
        return ret.appendf("IndexGroupAggregate %u", packet->queryHeader().activityId);
    }

    virtual void processRow(const void * next)
    {
        results.addRow(next);
    }

    virtual void createSegmentMonitors()
    {
        if (createSegmentMonitorsPending)
        {
            CRoxieIndexActivity::createSegmentMonitors();
            if ((kind==TAKindexgroupcount || kind==TAKindexgroupexists))
                groupSegCount = aggregateHelper->getGroupingMaxField();
            else
                groupSegCount = 0;
        }
    }

    virtual IMessagePacker *process()
    {
        MTIME_SECTION(queryActiveTimer(), "CRoxieIndexGroupAggregateActivity ::process");
        Owned<IRowManager> rowManager = roxiemem::createRowManager(0, NULL, logctx, NULL, true); // MORE - should not really use default limits
        Owned<IMessagePacker> output = ROQ->createOutputStream(packet->queryHeader(), false, logctx);

        unsigned processedBefore = processed;
        ScopedAtomic<unsigned> indexRecordsRead(::indexRecordsRead);
        try
        {
            while (!aborted && inputsDone < inputCount)
            {
                checkPartChanged(inputData[inputsDone]);
                if (tlk)
                {
                    createSegmentMonitors();
                    tlk->reset(false);
                    callback.setManager(tlk);
                    while (!aborted && tlk->lookup(true))
                    {
                        if (groupSegCount && !translators->queryTranslator(lastPartNo.fileNo))
                        {
                            AggregateRowBuilder &rowBuilder = results.addRow(tlk->queryKeyBuffer());
                            callback.finishedRow();
                            if (kind == TAKindexgroupcount)
                            {
                                unsigned __int64 count = tlk->getCurrentRangeCount(groupSegCount);
                                aggregateHelper->processCountGrouping(rowBuilder, count-1);
                            }
                            if (!tlk->nextRange(groupSegCount))
                                break;
                        }
                        else
                        {
                            keyprocessed++;
                            indexRecordsRead++;
                            aggregateHelper->processRow(tlk->queryKeyBuffer(), this);
                            callback.finishedRow();
                        }
                    }
                    callback.setManager(NULL);
                }
                inputsDone++;
            }

            if (!aborted)
            {
                for (;;)
                {
                    Owned<AggregateRowBuilder> next = results.nextResult();
                    if (!next)
                        break;
                    unsigned rowSize = next->querySize();
                    OwnedConstRoxieRow row(next->finalizeRowClear());
                    if (serializer)
                    {
                        serializeRow(output, row);
                    }
                    else
                    {
                        void *recBuffer = output->getBuffer(rowSize, meta.isVariableSize());
                        memcpy(recBuffer, row, rowSize);
                        output->putBuffer(recBuffer, rowSize, meta.isVariableSize());
                    }
                }
            }
        }
        catch (...)
        {
            results.reset();            // kill entries before the rowManager dies.
            throw;
        }

        results.reset();
        noteStats(processed-processedBefore, 0);
        if (aborted)
            return NULL;
        else
            return output.getClear();
    }
};

//================================================================================================

class CRoxieIndexGroupAggregateActivityFactory : public CRoxieIndexActivityFactory
{
    ThorActivityKind kind;
public:
    CRoxieIndexGroupAggregateActivityFactory(IPropertyTree &graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieIndexActivityFactory(graphNode, _subgraphId, _queryFactory, _helperFactory), kind(_kind)
    {
        Owned<IHThorIndexGroupAggregateArg> helper = (IHThorIndexGroupAggregateArg *) helperFactory();
        init(helper, graphNode);
    }

    virtual IRoxieSlaveActivity *createActivity(SlaveContextLogger &logctx, IRoxieQueryPacket *packet) const
    {
        return new CRoxieIndexGroupAggregateActivity(logctx, packet, helperFactory, this, kind);
    }

    virtual StringBuffer &toString(StringBuffer &s) const
    {
        return CSlaveActivityFactory::toString(s.append("INDEXGROUPAGGREGATE "));
    }
};

ISlaveActivityFactory *createRoxieIndexGroupAggregateActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieIndexGroupAggregateActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//================================================================================================

class CRoxieFetchActivityFactory : public CSlaveActivityFactory
{
public:
    Owned<ITranslatorSet> translators;
    Owned<IFileIOArray> fileArray;

    CRoxieFetchActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
        : CSlaveActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory)
    {
        Owned<IHThorFetchBaseArg> helper = (IHThorFetchBaseArg *) helperFactory();
        bool variableFileName = allFilesDynamic || queryFactory.isDynamic() || ((helper->getFetchFlags() & (FFvarfilename|FFdynamicfilename)) != 0);
        if (!variableFileName)
        {
            bool isOpt = (helper->getFetchFlags() & FFdatafileoptional) != 0;
            OwnedRoxieString fname(helper->getFileName());
            datafile.setown(_queryFactory.queryPackage().lookupFileName(fname, isOpt, true, true, _queryFactory.queryWorkUnit(), true));
            if (datafile)
            {
                unsigned expectedCrc = helper->getDiskFormatCrc();
                unsigned projectedCrc = helper->getProjectedFormatCrc();
                translators.setown(datafile->getTranslators(projectedCrc, helper->queryProjectedDiskRecordSize(), expectedCrc, helper->queryDiskRecordSize(), getEnableFieldTranslation(), false));
                fileArray.setown(datafile->getIFileIOArray(isOpt, queryFactory.queryChannel()));
            }
        }
    }

    virtual IRoxieSlaveActivity *createActivity(SlaveContextLogger &logctx, IRoxieQueryPacket *packet) const;

    virtual StringBuffer &toString(StringBuffer &s) const
    {
        return CSlaveActivityFactory::toString(s.append("FETCH "));
    }
};

class CRoxieFetchActivityBase : public CRoxieSlaveActivity
{
protected:
    IHThorFetchBaseArg *helper;
    const CRoxieFetchActivityFactory *factory;
    Owned<IFileIO> rawFile;
    Owned<ISerialStream> rawStream;
    offset_t base;
    char *inputData;
    char *inputLimit;
    Linked<ITranslatorSet> translators;
    Linked<IFileIOArray> files;
    const IDynamicTransform *translator = nullptr;
    bool needsRHS;

    virtual size32_t doFetch(ARowBuilder & rowBuilder, offset_t pos, offset_t rawpos, void *inputData) = 0;

public:
    CRoxieFetchActivityBase(SlaveContextLogger &_logctx, IRoxieQueryPacket *_packet,  HelperFactory *_hFactory,
                            const CRoxieFetchActivityFactory *_aFactory,
                            ITranslatorSet *_translators,
                            IFileIOArray *_files)
        : CRoxieSlaveActivity(_logctx, _packet, _hFactory, _aFactory), factory(_aFactory), translators(_translators), files(_files)
    {
        helper = (IHThorFetchBaseArg *) basehelper;
        base = 0;
        variableFileName = allFilesDynamic || basefactory->queryQueryFactory().isDynamic() || ((helper->getFetchFlags() & (FFvarfilename|FFdynamicfilename)) != 0);
        isOpt = (helper->getFetchFlags() & FFdatafileoptional) != 0;
        onCreate();
        inputData = (char *) serializedCreate.readDirect(0);
        inputLimit = inputData + (serializedCreate.length() - serializedCreate.getPos());
        needsRHS = helper->transformNeedsRhs();
    }

    virtual const char *queryDynamicFileName() const
    {
        return helper->getFileName();
    }

    virtual void setVariableFileInfo()
    {
        unsigned expectedCrc = helper->getDiskFormatCrc();
        unsigned projectedCrc = helper->getProjectedFormatCrc();
        translators.setown(varFileInfo->getTranslators(projectedCrc, helper->queryProjectedDiskRecordSize(), expectedCrc, helper->queryDiskRecordSize(), basefactory->getEnableFieldTranslation(), false));
        files.setown(varFileInfo->getIFileIOArray(isOpt, packet->queryHeader().channel));
    }

    virtual IMessagePacker *process();
    virtual StringBuffer &toString(StringBuffer &ret) const
    {
        return ret.appendf("Fetch %u", packet->queryHeader().activityId);
    }
    virtual void setPartNo(bool filechanged);
};

IMessagePacker *CRoxieFetchActivityBase::process()
{
    MTIME_SECTION(queryActiveTimer(), "CRoxieFetchActivityBase::process");
    Owned<IMessagePacker> output = ROQ->createOutputStream(packet->queryHeader(), false, logctx);
    unsigned accepted = 0;
    unsigned rejected = 0;
    unsigned __int64 rowLimit = helper->getRowLimit();
    OptimizedRowBuilder rowBuilder(rowAllocator, meta, output, serializer);
    while (!aborted && inputData < inputLimit)
    {
        checkPartChanged(*(PartNoType *) inputData);
        inputData += sizeof(PartNoType);
        offset_t rp = *(offset_t *)inputData;
        inputData += sizeof(offset_t);
        unsigned rhsSize;
        if (needsRHS)
        {
            rhsSize = *(unsigned *)inputData;
            inputData += sizeof(unsigned);
        }
        else
            rhsSize = 0;
        offset_t pos;
        if (isLocalFpos(rp))
            pos = getLocalFposOffset(rp);
        else
            pos = rp-base;

        unsigned thisSize = doFetch(rowBuilder, pos, rp, inputData);
        inputData += rhsSize;
        if (thisSize)
        {
            rowBuilder.writeToOutput(thisSize, true);

            accepted++;
            if (accepted > rowLimit)
            {
                logctx.noteStatistic(StNumDiskSeeks, accepted+rejected);
                logctx.noteStatistic(StNumDiskRowsRead, accepted+rejected);
                logctx.noteStatistic(StNumDiskAccepted, accepted);
                logctx.noteStatistic(StNumDiskRejected, rejected);
                limitExceeded();
                return NULL;
            }
        }
        else
            rejected++;
    }
    logctx.noteStatistic(StNumDiskSeeks, accepted+rejected);
    logctx.noteStatistic(StNumDiskRowsRead, accepted+rejected);
    logctx.noteStatistic(StNumDiskAccepted, accepted);
    logctx.noteStatistic(StNumDiskRejected, rejected);
    if (aborted)
        return NULL;
    else
        return output.getClear();
}

class CRoxieFetchActivity : public CRoxieFetchActivityBase
{
    Owned<IEngineRowAllocator> diskAllocator;
    CThorContiguousRowBuffer prefetchSource;
    Owned<ISourceRowPrefetcher> rowPrefetcher;

public:
    CRoxieFetchActivity(SlaveContextLogger &_logctx, IRoxieQueryPacket *_packet, HelperFactory *_hFactory,
                        const CRoxieFetchActivityFactory *_aFactory,
                        ITranslatorSet *_translators,
                        IFileIOArray *_files)
        : CRoxieFetchActivityBase(_logctx, _packet, _hFactory, _aFactory, _translators, _files)
    {
        // If we ever supported superfiles this would need to move to setPartNo, and pass proper subfile idx in
        rowPrefetcher.setown(translators->getPrefetcher(0));

        IOutputMetaData *diskMeta = helper->queryProjectedDiskRecordSize();
        diskAllocator.setown(getRowAllocator(diskMeta, basefactory->queryId()));
    }

    virtual size32_t doFetch(ARowBuilder & rowBuilder, offset_t pos, offset_t rawpos, void *inputData)
    {
        prefetchSource.reset(pos);
        rowPrefetcher->readAhead(prefetchSource);
        const byte *diskRow = prefetchSource.queryRow();
        if (translator)
        {
            MemoryBuffer buf;
            MemoryBufferBuilder aBuilder(buf, 0);
            FetchVirtualFieldCallback fieldCallback(rawpos);
            translator->translate(aBuilder, fieldCallback, diskRow);
            //  note the swapped parameters - left and right map to input and raw differently for JOIN vs FETCH
            IHThorFetchArg *h = (IHThorFetchArg *) helper;
            return h->transform(rowBuilder, buf.toByteArray(), inputData, rawpos);
        }
        else
        {
            //  note the swapped parameters - left and right map to input and raw differently for JOIN vs FETCH
            IHThorFetchArg *h = (IHThorFetchArg *) helper;
            return h->transform(rowBuilder, diskRow, inputData, rawpos);
        }
    }

    virtual void setPartNo(bool filechanged)
    {
        CRoxieFetchActivityBase::setPartNo(filechanged);
        prefetchSource.setStream(rawStream);
    }
};

IRoxieSlaveActivity *CRoxieFetchActivityFactory::createActivity(SlaveContextLogger &logctx, IRoxieQueryPacket *packet) const
{
    return new CRoxieFetchActivity(logctx, packet, helperFactory, this, translators, fileArray);
}

//------------------------------------------------------------------------------------

class CRoxieCSVFetchActivity : public CRoxieFetchActivityBase
{
    CSVSplitter csvSplitter;
    size32_t maxRowSize;
    CThorStreamDeserializerSource deserializeSource;

public:
    CRoxieCSVFetchActivity(SlaveContextLogger &_logctx, IRoxieQueryPacket *_packet, HelperFactory *_hFactory,
                           const CRoxieFetchActivityFactory *_aFactory,
                           ITranslatorSet *_translators,
                           IFileIOArray *_files,
                           unsigned _maxColumns, size32_t _maxRowSize)
        : CRoxieFetchActivityBase(_logctx, _packet, _hFactory, _aFactory, _translators, _files), maxRowSize(_maxRowSize)
    {
        const char * quotes = NULL;
        const char * separators = NULL;
        const char * terminators = NULL;
        const char * escapes = NULL;

        const IResolvedFile *fileInfo = varFileInfo ? varFileInfo : factory->datafile;
        if (fileInfo)
        {
            const IPropertyTree *options = fileInfo->queryProperties();
            if (options)
            {
                quotes = options->queryProp("@csvQuote");
                separators = options->queryProp("@csvSeparate");
                terminators = options->queryProp("@csvTerminate");
                escapes = options->queryProp("@csvEscape");
            }
        }

        IHThorCsvFetchArg *h = (IHThorCsvFetchArg *) helper;
        ICsvParameters *csvInfo = h->queryCsvParameters();
        csvSplitter.init(_maxColumns, csvInfo, quotes, separators, terminators, escapes);
        if (translator)
            UNIMPLEMENTED;  // It's not obvious how we would support this.
    }

    virtual size32_t doFetch(ARowBuilder & rowBuilder, offset_t pos, offset_t rawpos, void *inputData)
    {
        IHThorCsvFetchArg *h = (IHThorCsvFetchArg *) helper;
        rawStream->reset(pos);
        size32_t rowSize = 4096; // MORE - make configurable
        for (;;)
        {
            size32_t avail;
            const void *peek = rawStream->peek(rowSize, avail);
            if (csvSplitter.splitLine(avail, (const byte *)peek) < rowSize || avail < rowSize)
                break;
            if (rowSize == maxRowSize)
                throw MakeStringException(0, "File contained a line of length greater than %d bytes.", maxRowSize);
            if (rowSize >= maxRowSize/2)
                rowSize = maxRowSize;
            else
                rowSize += rowSize;
        }
        return h->transform(rowBuilder, csvSplitter.queryLengths(), (const char * *)csvSplitter.queryData(), inputData, rawpos);
    }

    virtual void setPartNo(bool filechanged)
    {
        CRoxieFetchActivityBase::setPartNo(filechanged);
        deserializeSource.setStream(rawStream);
    }
};

class CRoxieXMLFetchActivity : public CRoxieFetchActivityBase, implements IXMLSelect
{
    Owned<IXMLParse> parser;
    Owned<IColumnProvider> lastMatch;
    Owned<IFileIOStream> rawStreamX;
    unsigned streamBufferSize;
    CThorStreamDeserializerSource deserializeSource;

public:
    IMPLEMENT_IINTERFACE_USING(CRoxieFetchActivityBase)

    CRoxieXMLFetchActivity(SlaveContextLogger &_logctx, IRoxieQueryPacket *_packet, HelperFactory *_hFactory,
                           const CRoxieFetchActivityFactory *_aFactory,
                           ITranslatorSet *_translators,
                           IFileIOArray *_files,
                           unsigned _streamBufferSize)
        : CRoxieFetchActivityBase(_logctx, _packet, _hFactory, _aFactory, _translators, _files),
          streamBufferSize(_streamBufferSize)
    {
        if (translator)
            UNIMPLEMENTED;
    }

    virtual size32_t doFetch(ARowBuilder & rowBuilder, offset_t pos, offset_t rawpos, void *inputData)
    {
        rawStreamX->seek(pos, IFSbegin);
        try
        {
            while(!lastMatch)
                if(!parser->next())
                    throw MakeStringException(ROXIE_RECORD_FETCH_ERROR, "XML parse error at position %" I64F "d", pos);
            IHThorXmlFetchArg *h = (IHThorXmlFetchArg *) helper;
            unsigned thisSize = h->transform(rowBuilder, lastMatch, inputData, rawpos);
            lastMatch.clear();
            parser->reset();
            return thisSize;
        }
        catch (IException *E)
        {
            ::Release(E);
            throw MakeStringException(ROXIE_RECORD_FETCH_ERROR, "XML parse error at position %" I64F "d", pos);
        }
    }

    virtual void match(IColumnProvider & entry, offset_t startOffset, offset_t endOffset)
    {
        lastMatch.set(&entry);
    }

    virtual void setPartNo(bool filechanged)
    {
        CRoxieFetchActivityBase::setPartNo(filechanged);
        deserializeSource.setStream(rawStream);
        rawStreamX.setown(createBufferedIOStream(rawFile, streamBufferSize));
        parser.setown((factory->getKind()==TAKjsonfetch) ? createJSONParse(*rawStreamX, "/", *this) : createXMLParse(*rawStreamX, "/", *this));
    }
};


void CRoxieFetchActivityBase::setPartNo(bool filechanged)
{
    rawFile.setown(files->getFilePart(lastPartNo.partNo, base)); // MORE - superfiles
    translator = translators->queryTranslator(0);                // MORE - superfiles
    assertex(rawFile != NULL);
    rawStream.setown(createFileSerialStream(rawFile, 0, -1, 0));
}

class CRoxieCSVFetchActivityFactory : public CRoxieFetchActivityFactory
{
    unsigned maxColumns;
    size32_t maxRowSize;

public:
    CRoxieCSVFetchActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
        : CRoxieFetchActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory)
    {
        Owned<IHThorCsvFetchArg> helper = (IHThorCsvFetchArg*) helperFactory();
        maxColumns = helper->getMaxColumns();
        ICsvParameters *csvInfo = helper->queryCsvParameters();
        assertex(!csvInfo->queryEBCDIC());
        maxRowSize = defaultMaxCsvRowSize * 1024 * 1024;
        IConstWorkUnit *workunit = _queryFactory.queryWorkUnit();
        if (workunit)
            maxRowSize = workunit->getDebugValueInt(OPT_MAXCSVROWSIZE, defaultMaxCsvRowSize) * 1024 * 1024;
    }

    virtual IRoxieSlaveActivity *createActivity(SlaveContextLogger &logctx, IRoxieQueryPacket *packet) const
    {
        return new CRoxieCSVFetchActivity(logctx, packet, helperFactory, this, translators, fileArray, maxColumns, maxRowSize);
    }
};

class CRoxieXMLFetchActivityFactory : public CRoxieFetchActivityFactory
{
public:
    CRoxieXMLFetchActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
        : CRoxieFetchActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory)
    {
    }

    virtual IRoxieSlaveActivity *createActivity(SlaveContextLogger &logctx, IRoxieQueryPacket *packet) const
    {
        return new CRoxieXMLFetchActivity(logctx, packet, helperFactory, this, translators, fileArray, 4096);
    }
};


ISlaveActivityFactory *createRoxieFetchActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
{
    return new CRoxieFetchActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory);
}

ISlaveActivityFactory *createRoxieCSVFetchActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
{
    return new CRoxieCSVFetchActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory);
}

ISlaveActivityFactory *createRoxieXMLFetchActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
{
    return new CRoxieXMLFetchActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory);
}


//================================================================================================

class CRoxieKeyedJoinIndexActivityFactory : public CRoxieKeyedActivityFactory
{
public:
    CRoxieKeyedJoinIndexActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
        : CRoxieKeyedActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory)
    {
        Owned<IHThorKeyedJoinArg> helper = (IHThorKeyedJoinArg *) helperFactory();
        bool variableFileName = allFilesDynamic || queryFactory.isDynamic() || ((helper->getJoinFlags() & (JFvarindexfilename|JFdynamicindexfilename|JFindexfromactivity)) != 0);
        expectedCrc = helper->getIndexFormatCrc();
        projectedCrc = helper->getProjectedIndexFormatCrc();
        projectedMeta = helper->queryProjectedIndexRecordSize();
        expectedMeta = helper->queryIndexRecordSize();
        if (!variableFileName)
        {
            bool isOpt = (helper->getJoinFlags() & JFindexoptional) != 0;
            OwnedRoxieString indexFileName(helper->getIndexFileName());
            datafile.setown(_queryFactory.queryPackage().lookupFileName(indexFileName, isOpt, true, true, _queryFactory.queryWorkUnit(), true));
            if (datafile)
            {
                translators.setown(datafile->getTranslators(projectedCrc, projectedMeta, expectedCrc, expectedMeta, queryFactory.queryOptions().enableFieldTranslation, true));
                keyArray.setown(datafile->getKeyArray(isOpt, queryFactory.queryChannel()));
            }
        }
    }

    virtual IRoxieSlaveActivity *createActivity(SlaveContextLogger &logctx, IRoxieQueryPacket *packet) const;

    virtual StringBuffer &toString(StringBuffer &s) const
    {
        return CSlaveActivityFactory::toString(s.append("KEYEDJOIN INDEX "));
    }

};

class CRoxieKeyedJoinIndexActivity : public CRoxieKeyedActivity
{
    IHThorKeyedJoinArg *helper;

    const CRoxieKeyedJoinIndexActivityFactory *factory;

    unsigned inputLength;
    char *inputData;
    Owned<IRoxieSlaveActivity> rootIndexActivity;
    IIndexReadActivityInfo *rootIndex;

    unsigned processed;
    unsigned candidateCount;
    unsigned keepCount;
    unsigned inputDone;

public:
    CRoxieKeyedJoinIndexActivity(SlaveContextLogger &_logctx, IRoxieQueryPacket *_packet, HelperFactory *_hFactory, const CRoxieKeyedJoinIndexActivityFactory *_aFactory)
        : factory(_aFactory), CRoxieKeyedActivity(_logctx, _packet, _hFactory, _aFactory)
    {
        helper = (IHThorKeyedJoinArg *) basehelper;
        keyRecInfo = &helper->queryIndexRecordSize()->queryRecordAccessor(true);
        variableFileName = allFilesDynamic || basefactory->queryQueryFactory().isDynamic() || ((helper->getJoinFlags() & (JFvarindexfilename|JFdynamicindexfilename|JFindexfromactivity)) != 0);
        inputDone = 0;
        processed = 0;
        candidateCount = 0;
        keepCount = 0;
        rootIndex = NULL;
        onCreate();
        inputData = (char *) serializedCreate.readDirect(0);
        inputLength = (serializedCreate.length() - serializedCreate.getPos());
        if (resent)
        {
            resentInfo.read(inputDone);
            inputData += inputDone;
            resentInfo.read(processed);
            resentInfo.read(candidateCount);
            resentInfo.read(keepCount);
            resentInfo.read(lastPartNo.partNo);
            resentInfo.read(lastPartNo.fileNo);
            setPartNo(true);
            tlk->deserializeCursorPos(resentInfo);
            assertex(resentInfo.remaining() == 0);
        }
    }

    virtual bool hasNewSegmentMonitors() { return helper->hasNewSegmentMonitors(); }

    ~CRoxieKeyedJoinIndexActivity()
    {
    }

    virtual void deserializeExtra(MemoryBuffer &buff)
    {
        if (helper->getJoinFlags() & JFindexfromactivity)
        {
            RemoteActivityId indexActivityId(buff);
            assertex(indexActivityId.activityId);

            unsigned indexCtxLen;
            buff.read(indexCtxLen);
            const void *indexCtx = buff.readDirect(indexCtxLen);

            //We create a packet for the index activity to use. Header, trace info and parentextract are clone of mine, context info is copied from buff
            MemoryBuffer indexPacketData;
            indexPacketData.append(sizeof(RoxiePacketHeader), &packet->queryHeader());
            indexPacketData.append(packet->getTraceLength(), packet->queryTraceInfo());
            const byte *parentExtract = (const byte *) packet->queryContextData();
            unsigned parentExtractLen = *(unsigned*) parentExtract;
            indexPacketData.append(parentExtractLen);
            indexPacketData.append(parentExtractLen, parentExtract + sizeof(unsigned));
            indexPacketData.append(indexCtxLen, indexCtx);

            RoxiePacketHeader *newHeader = (RoxiePacketHeader *) indexPacketData.toByteArray();
            newHeader->continueSequence = 0;
            newHeader->activityId = indexActivityId.activityId;
            newHeader->queryHash = indexActivityId.queryHash;

            Owned<IRoxieQueryPacket> indexPacket = createRoxiePacket(indexPacketData);
            Owned<ISlaveActivityFactory> indexActivityFactory = factory->queryQueryFactory().getSlaveActivityFactory(indexActivityId.activityId);
            assertex(indexActivityFactory != NULL);
            rootIndexActivity.setown(indexActivityFactory->createActivity(logctx, indexPacket));
            rootIndex = rootIndexActivity->queryIndexReadActivity();
    
            varFileInfo.setown(rootIndex->getVarFileInfo());
            translators.setown(rootIndex->getTranslators());
            keyArray.setown(rootIndex->getKeySet());
        }
    }

    virtual const char *queryDynamicFileName() const
    {
        return helper->getIndexFileName();
    }

    virtual IMessagePacker *process();
    virtual StringBuffer &toString(StringBuffer &ret) const
    {
        return ret.appendf("KeyedJoinIndex %u", packet->queryHeader().activityId);
    }

    virtual void createSegmentMonitors()
    {
        // This is called to create the segmonitors that apply to ALL rows - not the per-row ones
        // At present there are none. However we should still set up the layout translation.
        if (createSegmentMonitorsPending)
        {
            createSegmentMonitorsPending = false;
            tlk->setLayoutTranslator(translators->queryTranslator(lastPartNo.fileNo));
        }
    }
};

IRoxieSlaveActivity *CRoxieKeyedJoinIndexActivityFactory::createActivity(SlaveContextLogger &logctx, IRoxieQueryPacket *packet) const
{
    return new CRoxieKeyedJoinIndexActivity(logctx, packet, helperFactory, this);
}

IMessagePacker *CRoxieKeyedJoinIndexActivity::process()
{
    MTIME_SECTION(queryActiveTimer(), "CRoxieKeyedJoinIndexActivity::process");
    Owned<IMessagePacker> output = ROQ->createOutputStream(packet->queryHeader(), false, logctx);
    CachedOutputMetaData joinFieldsMeta(helper->queryJoinFieldsRecordSize());
    Owned<IEngineRowAllocator> joinFieldsAllocator = getRowAllocator(joinFieldsMeta, basefactory->queryId());
    OptimizedKJRowBuilder rowBuilder(joinFieldsAllocator, joinFieldsMeta, output);

    unsigned __int64 rowLimit = helper->getRowLimit();
    unsigned atmost = helper->getJoinLimit();
    if (!atmost) atmost = (unsigned) -1;
    unsigned abortLimit = helper->getMatchAbortLimit();
    if (!abortLimit) abortLimit = (unsigned) -1;
    if (abortLimit < atmost)
        atmost = abortLimit;

    unsigned keepLimit = helper->getKeepLimit();
    unsigned joinFlags = helper->getJoinFlags();
    if (joinFlags & (JFtransformMaySkip | JFfetchMayFilter)) 
        keepLimit = 0;
    if ((joinFlags & (JFexclude|JFleftouter)) == (JFexclude|JFleftouter) && (!(joinFlags & JFfetchMayFilter)))  // For left-only joins, all we care about is existence of a match. Return as soon as we know that there is one
        keepLimit = 1;

    unsigned processedBefore = processed;
    unsigned rejected = 0;
    CachedOutputMetaData inputFields(helper->queryIndexReadInputRecordSize());
    unsigned inputSize = inputFields.getFixedSize();
    unsigned totalSizeSent = 0;
    // Now go fetch the records

    bool continuationFailed = false;
    while (!aborted && inputDone < inputLength)
    {
        checkPartChanged(*(PartNoType *) inputData);
        CJoinGroup *jg = *(CJoinGroup **) (inputData + sizeof(PartNoType)); // NOTE - this is a pointer in Roxie server's address space - don't go following it!
        char *inputRow = inputData+sizeof(PartNoType)+sizeof(const void *);
        if (inputFields.isVariableSize())
        {
            inputSize = *(unsigned *)inputRow;
            inputRow += sizeof(unsigned);
        }
        if (tlk)
        {
            createSegmentMonitors();

            helper->createSegmentMonitors(tlk, inputRow);
            if (rootIndex)
                rootIndex->mergeSegmentMonitors(tlk);
            tlk->finishSegmentMonitors();

            if (!resent && (atmost != (unsigned) -1) && ((atmost > preabortKeyedJoinsThreshold) || (joinFlags & JFcountmatchabortlimit) || (keepLimit != 0)))  
            {
                unsigned __int64 precount = tlk->checkCount(atmost);
                if (precount > atmost)
                {
                    candidateCount = atmost+1;
                    if (logctx.queryTraceLevel() > 5)
                        logctx.CTXLOG("Pre-aborting since candidate count is at least %" I64F "d", precount);
                }
                else
                {
                    if (logctx.queryTraceLevel() > 10)
                        logctx.CTXLOG("NOT Pre-aborting since candidate count is %" I64F "d", precount);
                    tlk->reset(false);
                }
            }
            else
                tlk->reset(resent);
            resent = false;
            ScopedAtomic<unsigned> indexRecordsRead(::indexRecordsRead);
            ScopedAtomic<unsigned> postFiltered(::postFiltered);
            while (candidateCount <= atmost)
            {
                if (tlk->lookup(true))
                {
                    candidateCount++;
                    indexRecordsRead++;
                    KLBlobProviderAdapter adapter(tlk);
                    const byte *indexRow = tlk->queryKeyBuffer();
                    size_t fposOffset = tlk->queryRowSize() - sizeof(offset_t);
                    offset_t fpos = rtlReadBigUInt8(indexRow + fposOffset);
                    if (helper->indexReadMatch(inputRow, indexRow, &adapter))
                    {
                        processed++;
                        if (keepLimit)
                        {
                            keepCount++;
                            if (keepCount > keepLimit)
                                break;
                        }
                        if (processed > rowLimit)
                        {
                            if (logctx.queryTraceLevel() > 1)
                            {
                                StringBuffer s;
                                logctx.CTXLOG("limit exceeded for %s", packet->queryHeader().toString(s).str());
                            }
                            noteStats(processed-processedBefore, rejected);
                            limitExceeded();
                            return NULL;
                        }
                        unsigned totalSize = 0;
                        if (helper->diskAccessRequired())
                        {
                            const void *self = output->getBuffer(KEYEDJOIN_RECORD_SIZE(0), true);
                            KeyedJoinHeader *rec = (KeyedJoinHeader *) self;
                            rec->fpos = fpos;
                            rec->thisGroup = jg;
                            rec->partNo = lastPartNo.partNo;
                            output->putBuffer(self, KEYEDJOIN_RECORD_SIZE(0), true);
                        }
                        else
                        {
                            KLBlobProviderAdapter adapter(tlk);
                            totalSize = helper->extractJoinFields(rowBuilder, indexRow, &adapter);
                            rowBuilder.writeToOutput(totalSize, fpos, jg, lastPartNo.partNo);
                        }
                        totalSizeSent += KEYEDJOIN_RECORD_SIZE(totalSize);
                        if (totalSizeSent > indexReadChunkSize && !continuationFailed)
                        {
                            MemoryBuffer si;
                            unsigned short siLen = 0;
                            si.append(siLen);
                            si.append(inputDone);
                            si.append(processed);
                            si.append(candidateCount);
                            si.append(keepCount);
                            si.append(lastPartNo.partNo);
                            si.append(lastPartNo.fileNo);
                            tlk->serializeCursorPos(si);
                            if (si.length() <= maxContinuationSize)
                            {
                                siLen = si.length() - sizeof(siLen);
                                si.writeDirect(0, sizeof(siLen), &siLen);
                                output->sendMetaInfo(si.toByteArray(), si.length());
                                noteStats(processed-processedBefore, rejected);
                                return output.getClear();
                            }
                            else
                                continuationFailed = true;
                        }
                    }
                    else
                    {
                        rejected++;
                        postFiltered++;
                    }
                }
                else
                    break;
            }
            tlk->releaseSegmentMonitors();
        }
        // output an end marker for the matches to this group
        KeyedJoinHeader *rec = (KeyedJoinHeader *) output->getBuffer(KEYEDJOIN_RECORD_SIZE(0), true);
        rec->fpos = candidateCount;
        rec->thisGroup = jg;
        rec->partNo = (unsigned short) -1;
        output->putBuffer(rec, KEYEDJOIN_RECORD_SIZE(0), true);
        totalSizeSent += KEYEDJOIN_RECORD_SIZE(0); // note - don't interrupt here though - too complicated.

        candidateCount = 0;
        keepCount = 0;

        inputData = inputRow + inputSize;
        inputDone += sizeof(PartNoType) + sizeof(const void *);
        if (inputFields.isVariableSize())
            inputDone += sizeof(unsigned);
        inputDone += inputSize;
    }
    noteStats(processed-processedBefore, rejected);
    if (aborted)
        return NULL;
    else
        return output.getClear();
}

//================================================================================================

ISlaveActivityFactory *createRoxieKeyedJoinIndexActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
{
    return new CRoxieKeyedJoinIndexActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory);
}

//================================================================================================

class CRoxieKeyedJoinFetchActivityFactory : public CSlaveActivityFactory
{
public:
    Owned<ITranslatorSet> translators;
    Owned<IFileIOArray> files;

    CRoxieKeyedJoinFetchActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
        : CSlaveActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory)
    {
        Owned<IHThorKeyedJoinArg> helper = (IHThorKeyedJoinArg *) helperFactory();
        assertex(helper->diskAccessRequired());
        bool variableFileName = allFilesDynamic || queryFactory.isDynamic() || ((helper->getFetchFlags() & (FFvarfilename|FFdynamicfilename)) != 0);
        if (!variableFileName)
        {
            bool isOpt = (helper->getFetchFlags() & FFdatafileoptional) != 0;
            OwnedRoxieString fileName(helper->getFileName());
            datafile.setown(_queryFactory.queryPackage().lookupFileName(fileName, isOpt, true, true, _queryFactory.queryWorkUnit(), true));
            if (datafile)
            {
                unsigned expectedCrc = helper->getDiskFormatCrc();
                unsigned projectedCrc = helper->getProjectedFormatCrc();
                translators.setown(datafile->getTranslators(projectedCrc, helper->queryProjectedDiskRecordSize(), expectedCrc, helper->queryDiskRecordSize(), getEnableFieldTranslation(), false));
                files.setown(datafile->getIFileIOArray(isOpt, queryFactory.queryChannel()));
            }
        }

    }

    virtual IRoxieSlaveActivity *createActivity(SlaveContextLogger &logctx, IRoxieQueryPacket *packet) const;

    virtual StringBuffer &toString(StringBuffer &s) const
    {
        return CSlaveActivityFactory::toString(s.append("KEYEDJOIN FETCH "));
    }
};

class CRoxieKeyedJoinFetchActivity : public CRoxieSlaveActivity
{
    IHThorKeyedJoinArg *helper;
    Owned<IFileIO> rawFile;
    offset_t base;
    const char *inputLimit;
    const char *inputData;
    Linked<ITranslatorSet> translators;
    Linked<IFileIOArray> files;
    Owned<ISerialStream> rawStream;
    CThorContiguousRowBuffer prefetchSource;
    Owned<ISourceRowPrefetcher> prefetcher;
    const IDynamicTransform *translator = nullptr;

    virtual void setPartNo(bool filechanged)
    {
        rawFile.setown(files->getFilePart(lastPartNo.partNo, base)); // MORE - superfiles
        translator = translators->queryTranslator(0);                // MORE - superfiles
        rawStream.setown(createFileSerialStream(rawFile, 0, -1, 0));
        prefetcher.setown(translators->getPrefetcher(0));
        prefetchSource.setStream(rawStream);
    }

public:
    CRoxieKeyedJoinFetchActivity(SlaveContextLogger &_logctx, IRoxieQueryPacket *_packet, HelperFactory *_hFactory, const CRoxieKeyedJoinFetchActivityFactory *_aFactory,
                                 IFileIOArray *_files, ITranslatorSet *_translators)
        : files(_files),
          translators(_translators),
          CRoxieSlaveActivity(_logctx, _packet, _hFactory, _aFactory)
    {
        // MORE - no continuation row support?
        base = 0;
        helper = (IHThorKeyedJoinArg *) basehelper;
        variableFileName = allFilesDynamic || basefactory->queryQueryFactory().isDynamic() || ((helper->getFetchFlags() & (FFvarfilename|FFdynamicfilename)) != 0);
        onCreate();
        inputData = (const char *) serializedCreate.readDirect(0);
        inputLimit = inputData + (serializedCreate.length() - serializedCreate.getPos());
    }

    ~CRoxieKeyedJoinFetchActivity()
    {
    }

    virtual const char *queryDynamicFileName() const
    {
        return helper->getFileName();
    }

    virtual void setVariableFileInfo()
    {
        unsigned expectedCrc = helper->getDiskFormatCrc();
        unsigned projectedCrc = helper->getProjectedFormatCrc();
        translators.setown(varFileInfo->getTranslators(projectedCrc, helper->queryProjectedDiskRecordSize(), expectedCrc, helper->queryDiskRecordSize(), basefactory->getEnableFieldTranslation(), false));
        files.setown(varFileInfo->getIFileIOArray(isOpt, packet->queryHeader().channel));
    }

    virtual IMessagePacker *process();

    virtual StringBuffer &toString(StringBuffer &ret) const
    {
        return ret.appendf("KeyedJoinFetch %u", packet->queryHeader().activityId);
    }
};

IMessagePacker *CRoxieKeyedJoinFetchActivity::process()
{
    MTIME_SECTION(queryActiveTimer(), "CRoxieKeyedJoinFetchActivity::process");
    // MORE - where we are returning everything there is an optimization or two to be had
    Owned<IMessagePacker> output = ROQ->createOutputStream(packet->queryHeader(), false, logctx);
    unsigned processed = 0;
    unsigned skipped = 0;
    unsigned __int64 rowLimit = helper->getRowLimit();
    unsigned totalSizeSent = 0;

    CachedOutputMetaData joinFieldsMeta(helper->queryJoinFieldsRecordSize());
    Owned<IEngineRowAllocator> joinFieldsAllocator = getRowAllocator(joinFieldsMeta, basefactory->queryId());
    OptimizedKJRowBuilder jfRowBuilder(joinFieldsAllocator, joinFieldsMeta, output);
    CachedOutputMetaData inputFields(helper->queryFetchInputRecordSize());
    size32_t inputSize = inputFields.getFixedSize();
    while (!aborted && inputData < inputLimit)
    {
        checkPartChanged(*(PartNoType *) inputData);
        inputData += sizeof(PartNoType);

        offset_t rp;
        memcpy(&rp, inputData, sizeof(rp));
        offset_t pos;
        if (isLocalFpos(rp))
            pos = getLocalFposOffset(rp);
        else
            pos = rp-base;

        prefetchSource.reset(pos);
        prefetcher->readAhead(prefetchSource);
        const byte *rawRHS = prefetchSource.queryRow();
        const KeyedJoinHeader *headerPtr = (KeyedJoinHeader *) inputData;

        MemoryBuffer buf;
        if (translator)
        {
            MemoryBufferBuilder aBuilder(buf, 0);
            FetchVirtualFieldCallback fieldCallback(headerPtr->fpos);
            translator->translate(aBuilder, fieldCallback, rawRHS);
            rawRHS = (const byte *) buf.toByteArray();
        }

        inputData = &headerPtr->rhsdata[0];
        if (inputFields.isVariableSize())
        {
            memcpy(&inputSize, inputData, sizeof(inputSize));
            inputData += sizeof(inputSize);
        }
        if (helper->fetchMatch(inputData, rawRHS))
        {
            unsigned thisSize = helper->extractJoinFields(jfRowBuilder, rawRHS, (IBlobProvider*)NULL);
            jfRowBuilder.writeToOutput(thisSize, headerPtr->fpos, headerPtr->thisGroup, headerPtr->partNo);
            totalSizeSent += KEYEDJOIN_RECORD_SIZE(thisSize);
            processed++;
            if (processed > rowLimit)
            {
                logctx.noteStatistic(StNumDiskSeeks, processed+skipped);
                logctx.noteStatistic(StNumDiskRowsRead, processed+skipped);
                logctx.noteStatistic(StNumDiskAccepted, processed);
                logctx.noteStatistic(StNumDiskRejected, skipped);
                limitExceeded();
                return NULL;
            }
        }
        else
        {
            skipped++;
            KeyedJoinHeader *out = (KeyedJoinHeader *) output->getBuffer(KEYEDJOIN_RECORD_SIZE(0), true);
            out->fpos = 0;
            out->thisGroup = headerPtr->thisGroup;
            out->partNo = (unsigned short) -1;
            output->putBuffer(out, KEYEDJOIN_RECORD_SIZE(0), true);
            totalSizeSent += KEYEDJOIN_RECORD_SIZE(0);
        }
        inputData += inputSize;
    }
    logctx.noteStatistic(StNumDiskSeeks, processed+skipped);
    logctx.noteStatistic(StNumDiskRowsRead, processed+skipped);
    logctx.noteStatistic(StNumDiskAccepted, processed);
    logctx.noteStatistic(StNumDiskRejected, skipped);
    if (aborted)
        return NULL;
    else
        return output.getClear();
}

IRoxieSlaveActivity *CRoxieKeyedJoinFetchActivityFactory::createActivity(SlaveContextLogger &logctx, IRoxieQueryPacket *packet) const
{
    return new CRoxieKeyedJoinFetchActivity(logctx, packet, helperFactory, this, files, translators);
}

ISlaveActivityFactory *createRoxieKeyedJoinFetchActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory)
{
    return new CRoxieKeyedJoinFetchActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory);
}


//================================================================================================

class CRoxieRemoteActivity : public CRoxieSlaveActivity
{
protected:
    IHThorRemoteArg * remoteHelper;
    unsigned processed;
    unsigned remoteId;

public:
    CRoxieRemoteActivity(SlaveContextLogger &_logctx, IRoxieQueryPacket *_packet, HelperFactory *_hFactory, const CSlaveActivityFactory *_aFactory, unsigned _remoteId)
        : CRoxieSlaveActivity(_logctx, _packet, _hFactory, _aFactory), 
        remoteId(_remoteId)
    {
        remoteHelper = (IHThorRemoteArg *) basehelper;
        processed = 0;
        onCreate();
    }

    virtual StringBuffer &toString(StringBuffer &ret) const
    {
        return ret.appendf("Remote %u", packet->queryHeader().activityId);
    }

    virtual const char *queryDynamicFileName() const
    {
        throwUnexpected();
    }

    virtual void setVariableFileInfo()
    {
        throwUnexpected();
    }

    virtual IMessagePacker *process()
    {
        MTIME_SECTION(queryActiveTimer(), "CRoxieRemoteActivity ::process");

        Owned<IMessagePacker> output = ROQ->createOutputStream(packet->queryHeader(), false, logctx);
        unsigned __int64 rowLimit = remoteHelper->getRowLimit();

        rtlRowBuilder remoteExtractBuilder;
        remoteHelper->createParentExtract(remoteExtractBuilder);

        Linked<IActivityGraph> remoteQuery = queryContext->queryChildGraph(remoteId);
        Linked<IRoxieServerChildGraph> remoteGraph = remoteQuery->queryLoopGraph();

        try
        {
            remoteGraph->beforeExecute();
            Owned<IFinalRoxieInput> input = remoteGraph->startOutput(0, remoteExtractBuilder.size(), remoteExtractBuilder.getbytes(), false);
            Owned<IStrandJunction> junction;
            IEngineRowStream *stream = connectSingleStream(queryContext, input, 0, junction, 0);

            while (!aborted)
            {
                const void * next = stream->ungroupedNextRow();
                if (!next)
                    break;

                size32_t nextSize = meta.getRecordSize(next);
                //MORE - what about grouping?
                processed++;
                if (processed > rowLimit)
                {
                    ReleaseRoxieRow(next);
                    limitExceeded(); 
                    break;
                }

                if (serializer)
                    serializeRow(output, next);
                else
                {
                    void * recBuffer = output->getBuffer(nextSize, meta.isVariableSize());
                    memcpy(recBuffer, next, nextSize);
                    output->putBuffer(recBuffer, nextSize, meta.isVariableSize());
                }
                ReleaseRoxieRow(next);
            }

            remoteGraph->afterExecute();
        }
        catch (IException *E)
        {
            remoteGraph->afterExecute();
            if (aborted)
                ::Release(E);
            else
                throw;
        }
        catch (...)
        {
            remoteGraph->afterExecute();
            throw;
        }

        if (aborted)
            return NULL;
        else
            return output.getClear();
    }

    virtual void setPartNo(bool filechanged)
    {
        throwUnexpected();
    }

};

//================================================================================================

class CRoxieRemoteActivityFactory : public CSlaveActivityFactory
{
    unsigned remoteId;

public:
    CRoxieRemoteActivityFactory(IPropertyTree &graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, unsigned _remoteId)
        : CSlaveActivityFactory(graphNode, _subgraphId, _queryFactory, _helperFactory), remoteId(_remoteId)
    {
    }

    virtual IRoxieSlaveActivity *createActivity(SlaveContextLogger &logctx, IRoxieQueryPacket *packet) const
    {
        return new CRoxieRemoteActivity(logctx, packet, helperFactory, this, remoteId);
    }

    virtual StringBuffer &toString(StringBuffer &s) const
    {
        return CSlaveActivityFactory::toString(s.append("Remote "));
    }
};

ISlaveActivityFactory *createRoxieRemoteActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, unsigned _remoteId)
{
    return new CRoxieRemoteActivityFactory(_graphNode, _subgraphId, _queryFactory, _helperFactory, _remoteId);
}


//================================================================================================

class CRoxieDummyActivityFactory : public CSlaveActivityFactory  // not a real activity - just used to properly link files
{
protected:
    Owned<const IResolvedFile> indexfile;
    Owned<IKeyArray> keyArray;
    Owned<IFileIOArray> fileArray;

public:
    CRoxieDummyActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, bool isLoadDataOnly)
        : CSlaveActivityFactory(_graphNode, _subgraphId, _queryFactory, NULL)
    {
        if (_graphNode.getPropBool("att[@name='_isSpill']/@value", false) || _graphNode.getPropBool("att[@name='_isSpillGlobal']/@value", false))
            return;  // ignore 'spills'

        try  // operations does not want any missing file errors to be fatal, or throw traps - just log it
        {
            ThorActivityKind kind = getActivityKind(_graphNode);
            if (kind != TAKdiskwrite && kind != TAKspillwrite && kind != TAKindexwrite && kind != TAKpiperead && kind != TAKpipewrite)
            {
                const char *fileName = queryNodeFileName(_graphNode, kind);
                const char *indexName = queryNodeIndexName(_graphNode, kind);
                if (indexName && !allFilesDynamic && !queryFactory.isDynamic())
                {
                    bool isOpt = pretendAllOpt || _graphNode.getPropBool("att[@name='_isIndexOpt']/@value");
                    indexfile.setown(_queryFactory.queryPackage().lookupFileName(indexName, isOpt, true, true, _queryFactory.queryWorkUnit(), true));
                    if (indexfile)
                        keyArray.setown(indexfile->getKeyArray(isOpt, queryFactory.queryChannel()));
                }
                if (fileName && !allFilesDynamic && !queryFactory.isDynamic())
                {
                    bool isOpt = pretendAllOpt || _graphNode.getPropBool("att[@name='_isOpt']/@value");
                    datafile.setown(_queryFactory.queryPackage().lookupFileName(fileName, isOpt, true, true, _queryFactory.queryWorkUnit(), true));
                    if (datafile)
                    {
                        fileArray.setown(datafile->getIFileIOArray(isOpt, queryFactory.queryChannel()));
                    }
                }
            }
        }
        catch(IException *E)
        {
            StringBuffer errors;
            E->errorMessage(errors);
            OERRLOG("%s File error = %s", (isLoadDataOnly) ? "LOADDATAONLY" : "SUSPENDED QUERY", errors.str());
            E->Release();
        }
    }

    virtual IRoxieSlaveActivity *createActivity(SlaveContextLogger &logctx, IRoxieQueryPacket *packet) const
    {
        throwUnexpected();  // don't actually want to create an activity
    }

};
//================================================================================================

ISlaveActivityFactory *createRoxieDummyActivityFactory(IPropertyTree &_graphNode, unsigned _subgraphId, IQueryFactory &_queryFactory, bool isLoadDataOnly)
{
    // MORE - bool isLoadDataOnly may need to be an enum if more than just LOADDATAONLY and suspended queries use this
    return new CRoxieDummyActivityFactory(_graphNode, _subgraphId, _queryFactory, isLoadDataOnly);
}
