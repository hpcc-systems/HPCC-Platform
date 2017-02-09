/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
##############################################################################
 */

#ifndef ECLHELPER_BASE_HPP
#define ECLHELPER_BASE_HPP

//Don't #include any files here - because sometimes included inside a namespace, and that generates confusion.

/*
This file contains base class definitions for the different helper classes.  Any common methods are implemented here.

Doesn't include jlib.hpp yet, so different implementation of CInterface (RtlCInterface) in eclrtl.hpp

*/

//---------------------------------------------------------------------------
// Record size handling.

class CGlobalHelperClass : public RtlCInterface
{
public:
    inline CGlobalHelperClass(unsigned _activityId) { activityId = _activityId; ctx = NULL; }

    inline void onCreate(ICodeContext * _ctx) { ctx = _ctx; }

protected:
    ICodeContext * ctx;
    unsigned activityId;

};

class COutputRowSerializer : implements IOutputRowSerializer, public CGlobalHelperClass
{
public:
    inline COutputRowSerializer(unsigned _activityId) : CGlobalHelperClass(_activityId) { }
    RTLIMPLEMENT_IINTERFACE

    virtual void serialize(IRowSerializerTarget & out, const byte * self) = 0;
};


class COutputRowDeserializer : implements IOutputRowDeserializer, public RtlCInterface
{
public:
    inline COutputRowDeserializer(unsigned _activityId) { activityId = _activityId; ctx = NULL; }
    RTLIMPLEMENT_IINTERFACE

    inline void onCreate(ICodeContext * _ctx) { ctx = _ctx; }

    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in) = 0;

protected:
    ICodeContext * ctx;
    unsigned activityId;
};


class CSourceRowPrefetcher : implements ISourceRowPrefetcher, public RtlCInterface
{
public:
    inline CSourceRowPrefetcher(unsigned _activityId) { activityId = _activityId; ctx = NULL; }
    RTLIMPLEMENT_IINTERFACE

    inline void onCreate(ICodeContext * _ctx) { ctx = _ctx; }

    virtual void readAhead(IRowDeserializerSource & in) = 0;

protected:
    ICodeContext * ctx;
    unsigned activityId;
};


class CFixedOutputRowSerializer : public COutputRowSerializer
{
public:
    inline CFixedOutputRowSerializer(unsigned _activityId, unsigned _fixedSize) : COutputRowSerializer(_activityId) { fixedSize = _fixedSize; }

    virtual void serialize(IRowSerializerTarget & out, const byte * self) { out.put(fixedSize, self); }

protected:
    size32_t fixedSize;
};

class CFixedOutputRowDeserializer : public COutputRowDeserializer
{
public:
    inline CFixedOutputRowDeserializer(unsigned _activityId, unsigned _fixedSize) : COutputRowDeserializer(_activityId) { fixedSize = _fixedSize; }

    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in) { in.read(fixedSize, rowBuilder.getSelf()); return fixedSize; }

protected:
    size32_t fixedSize;
};

class CFixedSourceRowPrefetcher : public CSourceRowPrefetcher
{
public:
    inline CFixedSourceRowPrefetcher(unsigned _activityId, unsigned _fixedSize) : CSourceRowPrefetcher(_activityId) { fixedSize = _fixedSize; }

    virtual void readAhead(IRowDeserializerSource & in) { in.skip(fixedSize); }

protected:
    size32_t fixedSize;
};

class CVariableOutputRowSerializer : public COutputRowSerializer
{
public:
    inline CVariableOutputRowSerializer(unsigned _activityId, IOutputMetaData * _meta) : COutputRowSerializer(_activityId) { meta = _meta; }

    virtual void serialize(IRowSerializerTarget & out, const byte * self)
    {
        unsigned size = meta->getRecordSize(self);
        out.put(size, self); 
    }

protected:
    IOutputMetaData * meta;
};

class COutputMetaData : implements IOutputMetaData, public RtlCInterface
{
public:
    RTLIMPLEMENT_IINTERFACE

    virtual void toXML(const byte * self, IXmlWriter & out) { 
                                                                const RtlTypeInfo * type = queryTypeInfo();
                                                                if (type)
                                                                {
                                                                    RtlFieldStrInfo dummyField("",NULL,type);
                                                                    type->toXML(self, self, &dummyField, out);
                                                                }
                                                            }
    virtual unsigned getVersion() const                     { return OUTPUTMETADATA_VERSION; }
    virtual unsigned getMetaFlags()                         { return MDFhasserialize|MDFhasxml; }

    virtual void destruct(byte * self)                      {}
    virtual IOutputMetaData * querySerializedDiskMeta()    { return this; }
    virtual IOutputRowSerializer * createDiskSerializer(ICodeContext * ctx, unsigned activityId)
    {
        return new CVariableOutputRowSerializer(activityId, this);
    }
    virtual ISourceRowPrefetcher * createDiskPrefetcher(ICodeContext * ctx, unsigned activityId)
    {
        ISourceRowPrefetcher * fetcher = defaultCreateDiskPrefetcher(ctx, activityId);
        if (fetcher)
            return fetcher;
        //Worse case implementation using a deserialize
        return new CSimpleSourceRowPrefetcher(*this, ctx, activityId);
    }
    //Default internal serializers are the same as the disk versions
    virtual IOutputRowSerializer * createInternalSerializer(ICodeContext * ctx, unsigned activityId)
    {
        return createDiskSerializer(ctx, activityId);
    }
    virtual IOutputRowDeserializer * createInternalDeserializer(ICodeContext * ctx, unsigned activityId)
    {
        return createDiskDeserializer(ctx, activityId);
    }
    virtual void walkIndirectMembers(const byte * self, IIndirectMemberVisitor & visitor) { }
    virtual IOutputMetaData * queryChildMeta(unsigned i) { return NULL; }

protected:
    //This is the prefetch function that is actually generated by the code generator
    virtual CSourceRowPrefetcher * doCreateDiskPrefetcher(unsigned activityId) { return NULL; }

    inline ISourceRowPrefetcher * defaultCreateDiskPrefetcher(ICodeContext * ctx, unsigned activityId)
    {
        if (getMetaFlags() & MDFneedserializedisk)
            return querySerializedDiskMeta()->createDiskPrefetcher(ctx, activityId);
        CSourceRowPrefetcher * fetcher = doCreateDiskPrefetcher(activityId);
        if (fetcher)
        {
            fetcher->onCreate(ctx);
            return fetcher;
        }
        return NULL;
    }
};

class CFixedOutputMetaData : public COutputMetaData
{
public:
    CFixedOutputMetaData(size32_t _fixedSize)               { fixedSize = _fixedSize; }

    virtual size32_t getRecordSize(const void *rec)         { return fixedSize; }
    virtual size32_t getMinRecordSize() const               { return fixedSize; }
    virtual size32_t getFixedSize() const                   { return fixedSize; }

    virtual IOutputRowSerializer * createDiskSerializer(ICodeContext * ctx, unsigned activityId)
    {
        return new CFixedOutputRowSerializer(activityId, fixedSize);
    }
    virtual IOutputRowDeserializer * createDiskDeserializer(ICodeContext * ctx, unsigned activityId)
    {
        return new CFixedOutputRowDeserializer(activityId, fixedSize);
    }
    virtual ISourceRowPrefetcher * createDiskPrefetcher(ICodeContext * ctx, unsigned activityId)
    {
        ISourceRowPrefetcher * fetcher = defaultCreateDiskPrefetcher(ctx, activityId);
        if (fetcher)
            return fetcher;
        return new CFixedSourceRowPrefetcher(activityId, fixedSize);
    }

protected:
    size32_t fixedSize;
};

class CVariableOutputMetaData : public COutputMetaData
{
public:
    CVariableOutputMetaData(size32_t _minSize) : minSize(_minSize) { }

    virtual size32_t getMinRecordSize() const               { return minSize; }
    virtual size32_t getFixedSize() const                   { return 0; }  // is variable

protected:
    size32_t minSize;
};

class CActionOutputMetaData : public COutputMetaData
{
public:
    virtual size32_t getRecordSize(const void *)            { return 0; }
    virtual size32_t getMinRecordSize() const               { return 0; }
    virtual size32_t getFixedSize() const                   { return 0; }  // is pseudo-variable
    virtual void toXML(const byte * self, IXmlWriter & out) { }
    virtual IOutputRowSerializer * createDiskSerializer(ICodeContext * ctx, unsigned activityId)
    {
        return new CFixedOutputRowSerializer(activityId, 0);
    }
    virtual IOutputRowDeserializer * createDiskDeserializer(ICodeContext * ctx, unsigned activityId)
    {
        return new CFixedOutputRowDeserializer(activityId, 0);
    }
    virtual ISourceRowPrefetcher * createDiskPrefetcher(ICodeContext * ctx, unsigned activityId)
    {
        return new CFixedSourceRowPrefetcher(activityId, 0);
    }
};

//---------------------------------------------------------------------------

class CXmlToRowTransformer : implements IXmlToRowTransformer, public CGlobalHelperClass
{
public:
    inline CXmlToRowTransformer(unsigned _activityId) : CGlobalHelperClass(_activityId) {}
    RTLIMPLEMENT_IINTERFACE

};

//---------------------------------------------------------------------------
// Record size handling.

class ThorHelper : public RtlCInterface
{
public:
    ThorHelper()                                            { ctx = 0; }

protected:
    ICodeContext * ctx;
};


class CNormalizeChildIterator : implements INormalizeChildIterator, public RtlCInterface
{
public:
    CNormalizeChildIterator(IOutputMetaData & _recordSize) : iter(0, NULL, _recordSize) {}
    RTLIMPLEMENT_IINTERFACE

    virtual byte * first(const void * parentRecord)         { init(parentRecord); return (byte *)iter.first(); }
    virtual byte * next()                                   { return (byte *)iter.next(); }
    virtual void init(const void * parentRecord) = 0;

    inline void setDataset(size32_t len, const void * data) { iter.setDataset(len, data); }

protected:
    RtlVariableDatasetCursor    iter;
};
    
class CNormalizeLinkedChildIterator : implements INormalizeChildIterator, public RtlCInterface
{
public:
    CNormalizeLinkedChildIterator() : iter(0, NULL) {}
    RTLIMPLEMENT_IINTERFACE

    virtual byte * first(const void * parentRecord)         { init(parentRecord); return (byte *)iter.first(); }
    virtual byte * next()                                   { return (byte *)iter.next(); }
    virtual void init(const void * parentRecord) = 0;

    inline void setDataset(unsigned _numRows, byte * * _rows) { iter.setDataset(_numRows, _rows); }

protected:
    RtlSafeLinkedDatasetCursor  iter;
};
    
class CNormalizeStreamedChildIterator : implements INormalizeChildIterator, public RtlCInterface
{
public:
    CNormalizeStreamedChildIterator() {}
    RTLIMPLEMENT_IINTERFACE

    virtual byte * first(const void * parentRecord)         { init(parentRecord); return (byte *)iter.first(); }
    virtual byte * next()                                   { return (byte *)iter.next(); }
    virtual void init(const void * parentRecord) = 0;

    inline void setDataset(IRowStream * _streamed) { iter.init(_streamed); }

protected:
    RtlStreamedDatasetCursor  iter;
};

//---------------------------------------------------------------------------

class CThorArg : public RtlCInterface
{
public:
    ICodeContext * ctx;
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) {
        ctx = _ctx;
    }
};

class CThorIndexWriteArg : implements IHThorIndexWriteArg, public CThorArg
{
public:
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }
    virtual IOutputMetaData * queryOutputMeta() { return NULL; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)                       
    { 
        switch (which)
        {
        case TAIarg:
        case TAIindexwritearg_1:
            return static_cast<IHThorIndexWriteArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual unsigned getFlags()                             { return 0; }
    virtual const char * getDatasetName()                   { return NULL; }  // Never used, left in to keep VMT unchanged only
    virtual const char * getDistributeIndexName()           { return NULL; }
    virtual unsigned getExpiryDays()                        { return 0; }
    virtual void getUpdateCRCs(unsigned & eclCRC, unsigned __int64 & totalCRC)  { }
    virtual unsigned getFormatCrc() = 0;
    virtual const char * getCluster(unsigned idx) { return NULL; }
    virtual bool getIndexLayout(size32_t & _retLen, void * & _retData) { return false; }
    virtual bool getIndexMeta(size32_t & lenName, char * & name, size32_t & lenValue, char * & value, unsigned idx) { return false; }
    virtual unsigned getWidth() { return 0; }
    virtual ICompare * queryCompare() { return NULL; }
};

class CThorFirstNArg : implements IHThorFirstNArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)                       
    { 
        switch (which)
        {
        case TAIarg:
        case TAIfirstnarg_1:
            return static_cast<IHThorFirstNArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual __int64 numToSkip()                             { return 0; }
    virtual bool preserveGrouping()                         { return false; }
};

class CThorChooseSetsArg : implements IHThorChooseSetsArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)                       
    { 
        switch (which)
        {
        case TAIarg:
        case TAIchoosesetsarg_1:
            return static_cast<IHThorChooseSetsArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};

class CThorChooseSetsExArg : implements IHThorChooseSetsExArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)                       
    { 
        switch (which)
        {
        case TAIarg:
        case TAIchoosesetsexarg_1:
            return static_cast<IHThorChooseSetsExArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};

class CThorDiskWriteArg : implements IHThorDiskWriteArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }
    virtual IOutputMetaData * queryOutputMeta() { return NULL; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIdiskwritearg_1:
            return static_cast<IHThorDiskWriteArg *>(this);
        default:
            break;
        }
        return NULL;
    }
    
    virtual int getSequence()                               { return -3; }
    virtual unsigned getFlags()                             { return 0; }
    virtual unsigned getTempUsageCount()                    { return 0; }
    virtual unsigned getExpiryDays()                        { return 0; }
    virtual void getUpdateCRCs(unsigned & eclCRC, unsigned __int64 & totalCRC)  { }
    virtual void getEncryptKey(size32_t & keyLen, void * & key) { keyLen = 0; key = 0; }
    virtual const char * getCluster(unsigned idx)         { return NULL; }
};

class CThorPipeReadArg : implements IHThorPipeReadArg, public CThorArg
{
public:
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)                       
    { 
        switch (which)
        {
        case TAIarg:
        case TAIpipereadarg_1:
            return static_cast<IHThorPipeReadArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual bool needTransform()                            { return false; }
    virtual bool transformMayFilter()                       { return false; }
    virtual unsigned getPipeFlags()                         { return 0; }
    virtual ICsvToRowTransformer * queryCsvTransformer()    { return NULL; }
    virtual IXmlToRowTransformer * queryXmlTransformer()    { return NULL; }
    virtual const char * getXmlIteratorPath()             { return NULL; }
};

class CThorPipeWriteArg : implements IHThorPipeWriteArg, public CThorArg
{
public:
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }
    virtual IOutputMetaData * queryOutputMeta() { return NULL; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIpipewritearg_1:
            return static_cast<IHThorPipeWriteArg *>(this);
        default:
            break;
        }
        return NULL;
    }
    
    virtual char * getNameFromRow(const void * _self)       { return NULL; }
    virtual bool recreateEachRow()                          { return (getPipeFlags() & TPFrecreateeachrow) != 0; }
    virtual unsigned getPipeFlags()                         { return 0; }
    virtual IHThorCsvWriteExtra * queryCsvOutput()          { return NULL; }
    virtual IHThorXmlWriteExtra * queryXmlOutput()          { return NULL; }
};

class CThorPipeThroughArg : implements IHThorPipeThroughArg, public CThorArg
{
public:
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIpipethrougharg_1:
            return static_cast<IHThorPipeThroughArg *>(this);
        default:
            break;
        }
        return NULL;
    }
    
    virtual char * getNameFromRow(const void * _self)       { return NULL; }
    virtual bool recreateEachRow()                          { return (getPipeFlags() & TPFrecreateeachrow) != 0; }
    virtual unsigned getPipeFlags()                         { return 0; }
    virtual IHThorCsvWriteExtra * queryCsvOutput()          { return NULL; }
    virtual IHThorXmlWriteExtra * queryXmlOutput()          { return NULL; }
    virtual ICsvToRowTransformer * queryCsvTransformer()    { return NULL; }
    virtual IXmlToRowTransformer * queryXmlTransformer()    { return NULL; }
    virtual const char * getXmlIteratorPath()             { return NULL; }
};


class CThorFilterArg : implements IHThorFilterArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIfilterarg_1:
            return static_cast<IHThorFilterArg *>(this);
        default:
            break;
        }
        return NULL;
    }
    
    virtual bool canMatchAny()                              { return true; }
    virtual bool isValid(const void * _left)                { return true; }
};

class CThorFilterGroupArg : implements IHThorFilterGroupArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIfiltergrouparg_1:
            return static_cast<IHThorFilterGroupArg *>(this);
        default:
            break;
        }
        return NULL;
    }
    
    virtual bool canMatchAny()                              { return true; }
    virtual bool isValid(unsigned _num, const void * * _rows) { return true; }

};

class CThorGroupArg : implements IHThorGroupArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIgrouparg_1:
            return static_cast<IHThorGroupArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};

class CThorDegroupArg : implements IHThorDegroupArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIdegrouparg_1:
            return static_cast<IHThorDegroupArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};

class CThorIterateArg : implements IHThorIterateArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIiteratearg_1:
            return static_cast<IHThorIterateArg *>(this);
        default:
            break;
        }
        return NULL;
    }
    
    virtual bool canFilter()                                { return false; }
};


typedef CThorIterateArg CThorGroupIterateArg;

class CThorProcessArg : implements IHThorProcessArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIprocessarg_1:
            return static_cast<IHThorProcessArg *>(this);
        default:
            break;
        }
        return NULL;
    }
    
    virtual bool canFilter()                                { return false; }
};


class CThorProjectArg : implements IHThorProjectArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIprojectarg_1:
            return static_cast<IHThorProjectArg *>(this);
        default:
            break;
        }
        return NULL;
    }
    
    virtual bool canFilter()                                { return false; }
};

class CThorQuantileArg : implements IHThorQuantileArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIquantilearg_1:
            return static_cast<IHThorQuantileArg *>(this);
        }
        return NULL;
    }

    virtual unsigned getFlags() { return 0; }
    virtual unsigned __int64 getNumDivisions() { return 2; }
    virtual double getSkew() { return 0; }
    virtual unsigned __int64 getScore(const void * _left) { return 1; }
    virtual void getRange(bool & isAll, size32_t & tlen, void * & tgt) { isAll = true; tlen = 0; tgt = NULL; }
};

class CThorPrefetchProjectArg : implements IHThorPrefetchProjectArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIprefetchprojectarg_1:
            return static_cast<IHThorPrefetchProjectArg *>(this);
        default:
            break;
        }
        return NULL;
    }
    
    virtual bool canFilter() { return false; }
    virtual bool canMatchAny() { return true; }
    virtual unsigned getFlags() { return 0; }
    virtual unsigned getLookahead() { return 0; }
    virtual IThorChildGraph *queryChild() { return NULL; }
    virtual bool preTransform(rtlRowBuilder & extract, const void * _left, unsigned __int64 _counter) { return true; }
};

struct CThorFilterProjectArg : implements IHThorFilterProjectArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIfilterprojectarg_1:
            return static_cast<IHThorFilterProjectArg *>(this);
        default:
            break;
        }
        return NULL;
    }
    
    virtual bool canFilter() { return false; }
    virtual bool canMatchAny() { return true; }
};

class CThorCountProjectArg : implements IHThorCountProjectArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIcountprojectarg_1:
            return static_cast<IHThorCountProjectArg *>(this);
        default:
            break;
        }
        return NULL;
    }
    
    virtual bool canFilter()                                { return false; }
};

class CThorNormalizeArg : implements IHThorNormalizeArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAInormalizearg_1:
            return static_cast<IHThorNormalizeArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};

class CThorSelectNArg : implements IHThorSelectNArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIselectnarg_1:
            return static_cast<IHThorSelectNArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};


class CThorCombineArg : implements IHThorCombineArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIcombinearg_1:
            return static_cast<IHThorCombineArg *>(this);
        default:
            break;
        }
        return NULL;
    }
    
    virtual bool canFilter()                                { return false; }
};

class CThorCombineGroupArg : implements IHThorCombineGroupArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIcombinegrouparg_1:
            return static_cast<IHThorCombineGroupArg *>(this);
        default:
            break;
        }
        return NULL;
    }
    
    virtual bool canFilter()                                { return false; }
};

class CThorRollupGroupArg : implements IHThorRollupGroupArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIrollupgrouparg_1:
            return static_cast<IHThorRollupGroupArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};

class CThorRegroupArg : implements IHThorRegroupArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIregrouparg_1:
            return static_cast<IHThorRegroupArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};


class CThorNullArg : implements IHThorNullArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }
    virtual IOutputMetaData * queryOutputMeta() { return NULL; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAInullarg_1:
            return static_cast<IHThorNullArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};

class CThorActionArg : implements IHThorActionArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }
    virtual IOutputMetaData * queryOutputMeta() { return NULL; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIactionarg_1:
            return static_cast<IHThorActionArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual void action() {};
};

typedef CThorActionArg CThorSideEffectArg;

class CThorLimitArg : implements IHThorLimitArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIlimitarg_1:
            return static_cast<IHThorLimitArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual void onLimitExceeded() {}           // nothing generated for skip.
};


class CThorCreateRowLimitArg : implements IHThorLimitArg, implements IHThorLimitTransformExtra, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIlimitarg_1:
            return static_cast<IHThorLimitArg *>(this);
        case TAIlimittransformextra_1:
            return static_cast<IHThorLimitTransformExtra *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual void onLimitExceeded() {}
    virtual size32_t transformOnLimitExceeded(ARowBuilder & rowBuilder) { return 0; }
};


class CThorCatchArg : implements IHThorCatchArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIcatcharg_1:
            return static_cast<IHThorCatchArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual unsigned getFlags() { return 0; }
    virtual bool isHandler(IException * e) { return true; }
    virtual void onExceptionCaught() { }
    virtual size32_t transformOnExceptionCaught(ARowBuilder & rowBuilder, IException * e) { return 0; }
};


class CThorSplitArg : implements IHThorSplitArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIsplitarg_1:
            return static_cast<IHThorSplitArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual unsigned numBranches()                          { return 2; }
    virtual bool isBalanced()                               { return false; }
};

class CThorSpillArg : implements IHThorSpillArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIdiskwritearg_1:
            return static_cast<IHThorDiskWriteArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual IOutputMetaData * queryDiskRecordSize()             { return queryOutputMeta(); }
    virtual int getSequence()                               { return -3; }
    virtual unsigned getFlags()                             { return 0; }
    virtual unsigned getTempUsageCount()                    { return 1; }
    virtual unsigned getExpiryDays()                        { return 0; }
    virtual void getUpdateCRCs(unsigned & eclCRC, unsigned __int64 & totalCRC)  { }
    virtual void getEncryptKey(size32_t & keyLen, void * & key) { keyLen = 0; key = 0; }
    virtual const char * getCluster(unsigned idx)         { return NULL; }
};


class CThorNormalizeChildArg : implements IHThorNormalizeChildArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAInormalizechildarg_1:
            return static_cast<IHThorNormalizeChildArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};


class CThorNormalizeLinkedChildArg : implements IHThorNormalizeLinkedChildArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAInormalizelinkedchildarg_1:
            return static_cast<IHThorNormalizeLinkedChildArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};


class CThorChildIteratorArg : implements IHThorChildIteratorArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIchilditeratorarg_1:
            return static_cast<IHThorChildIteratorArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};


class CThorRawIteratorArg : implements IHThorRawIteratorArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIrawiteratorarg_1:
            return static_cast<IHThorRawIteratorArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};


class CThorLinkedRawIteratorArg : implements IHThorLinkedRawIteratorArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIlinkedrawiteratorarg_1:
            return static_cast<IHThorLinkedRawIteratorArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};


class CThorRollupArg : implements IHThorRollupArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIrolluparg_1:
            return static_cast<IHThorRollupArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual unsigned getFlags() { return 0; }
    virtual bool matches(const void * _left, const void * _right) { return true; }
};

class CThorDedupArg : implements IHThorDedupArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIdeduparg_1:
            return static_cast<IHThorDedupArg *>(this);
        default:
            break;
        }
        return NULL;
    }
    virtual bool matches(const void * _left, const void * _right) { return true; }
    virtual unsigned numToKeep() { return 1; }
    virtual ICompare * queryComparePrimary() { return NULL; }
    virtual unsigned getFlags() { return HDFkeepleft; }
    virtual ICompare * queryCompareBest() { return NULL; }
};

class CThorAggregateArg : implements IHThorAggregateArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIaggregatearg_1:
            return static_cast<IHThorAggregateArg *>(this);
        default:
            break;
        }
        return NULL;
    }
    virtual unsigned getAggregateFlags() { return 0; }
    virtual size32_t clearAggregate(ARowBuilder & rowBuilder) = 0;
    virtual size32_t processFirst(ARowBuilder & rowBuilder, const void * src) = 0;
    virtual size32_t processNext(ARowBuilder & rowBuilder, const void * src) = 0;
    virtual size32_t mergeAggregate(ARowBuilder & rowBuilder, const void * src) { rtlFailUnexpected(); return 0; }
};

class CThorCountAggregateArg : public CThorAggregateArg
{
    virtual size32_t clearAggregate(ARowBuilder & rowBuilder)           
    { 
        void * target = rowBuilder.getSelf();
        *((unsigned __int64 *)target) = 0; 
        return sizeof(unsigned __int64); 
    }
    virtual size32_t processFirst(ARowBuilder & rowBuilder, const void * src)       
    { 
        void * target = rowBuilder.getSelf();
        *((unsigned __int64 *)target) = 1; 
        return sizeof(unsigned __int64);
    }
    virtual size32_t processNext(ARowBuilder & rowBuilder, const void * src)        
    { 
        void * target = rowBuilder.getSelf();
        ++*((unsigned __int64 *)target); 
        return sizeof(unsigned __int64);
    }
    virtual size32_t mergeAggregate(ARowBuilder & rowBuilder, const void * src) 
    { 
        void * target = rowBuilder.getSelf();
        *((unsigned __int64 *)target) += *((unsigned __int64 *)src); 
        return sizeof(unsigned __int64);
    }
};

class CThorExistsAggregateArg : public CThorAggregateArg
{
    virtual size32_t clearAggregate(ARowBuilder & rowBuilder)           
    { 
        void * target = rowBuilder.getSelf();
        *((bool *)target) = false; 
        return sizeof(bool);
    }
    virtual size32_t processFirst(ARowBuilder & rowBuilder, const void * src)       
    { 
        void * target = rowBuilder.getSelf();
        *((bool *)target) = true;
        return sizeof(bool);
    }
    virtual size32_t processNext(ARowBuilder & rowBuilder, const void * src)        
    { 
        return sizeof(bool);
    }
    virtual size32_t mergeAggregate(ARowBuilder & rowBuilder, const void * src) 
    { 
        void * target = rowBuilder.getSelf();
        if (*((bool *)src)) 
            *((bool *)target) = true;
        return sizeof(bool);
    }
};

class CThorThroughAggregateArg : implements IHThorThroughAggregateArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIaggregatearg_1:
            return static_cast<IHThorAggregateArg *>(this);
        case TAIthroughaggregateextra_1:
            return static_cast<IHThorThroughAggregateExtra *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual unsigned getAggregateFlags() { return 0; }
    virtual size32_t mergeAggregate(ARowBuilder & rowBuilder, const void * src) { rtlFailUnexpected(); return 0; }
};

class CThorDistributionArg : implements IHThorDistributionArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }
    virtual IOutputMetaData * queryOutputMeta() { return NULL; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIdistributionarg_1:
            return static_cast<IHThorDistributionArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};

class CThorGroupAggregateArg : implements IHThorGroupAggregateArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIaggregatearg_1:
            return static_cast<IHThorGroupAggregateArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual unsigned getAggregateFlags() { return 0; }
    virtual size32_t mergeAggregate(ARowBuilder & rowBuilder, const void * src) { rtlFailUnexpected(); return 0; }
};

class CThorHashAggregateArg : implements IHThorHashAggregateArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIaggregatearg_1:
            return static_cast<IHThorAggregateArg *>(this);
        case TAIhashaggregateextra_1:
            return static_cast<IHThorHashAggregateExtra *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual unsigned getAggregateFlags() { return 0; }
    virtual size32_t mergeAggregate(ARowBuilder & rowBuilder, const void * src) { rtlFailUnexpected(); return 0; }
};

class CThorInlineTableArg : implements IHThorInlineTableArg, public CThorArg
{
public:
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIinlinetablearg_1:
            return static_cast<IHThorInlineTableArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual unsigned getFlags()                         { return 0; }
};

class CThorInlineRowArg : public CThorInlineTableArg
{
    virtual __uint64 numRows()                          { return 1; }
};

class CThorSampleArg : implements IHThorSampleArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIsamplearg_1:
            return static_cast<IHThorSampleArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};

class CThorEnthArg : implements IHThorEnthArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIentharg_1:
            return static_cast<IHThorEnthArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};

class CThorFunnelArg : implements IHThorFunnelArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIfunnelarg_1:
            return static_cast<IHThorFunnelArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual bool isOrdered()                            { return false; }
    virtual bool pullSequentially()                     { return false; }
};

class CThorNonEmptyArg : implements IHThorNonEmptyArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAInonemptyarg_1:
            return static_cast<IHThorNonEmptyArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};

class CThorMergeArg : implements IHThorMergeArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAImergearg_1:
            return static_cast<IHThorMergeArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual ISortKeySerializer * querySerialize()       { return NULL; }        // only if global
    virtual ICompare * queryCompareKey()                { return NULL; }
    virtual ICompare * queryCompareRowKey()             { return NULL; }        // row is lhs, key is rhs
    virtual bool dedup()                                { return false; }
};

class CThorRemoteResultArg : implements IHThorRemoteResultArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }
    virtual IOutputMetaData * queryOutputMeta() { return NULL; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIremoteresultarg_1:
            return static_cast<IHThorRemoteResultArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual int getSequence()                               { return -3; }
};

class CThorApplyArg : implements IHThorApplyArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }
    virtual IOutputMetaData * queryOutputMeta() { return NULL; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIapplyarg_1:
            return static_cast<IHThorApplyArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual void start() { } 
    virtual void end() { }
};

class CThorSortArg : implements IHThorSortArg, implements IHThorAlgorithm, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIsortarg_1:
            return static_cast<IHThorSortArg *>(this);
        case TAIalgorithm_1:
            return static_cast<IHThorAlgorithm *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual double getSkew()                            { return 0; }           // 0=default
    virtual bool hasManyRecords()                       { return false; }
    virtual double getTargetSkew()                      { return 0; }
    virtual ISortKeySerializer * querySerialize()       { return NULL; }
    virtual unsigned __int64 getThreshold()             { return 0; }
    virtual IOutputMetaData * querySortedRecordSize()       { return NULL; }
    virtual const char * getSortedFilename()            { return NULL; }
    virtual ICompare * queryCompareLeftRight()          { return NULL; }
    virtual unsigned getAlgorithmFlags()                { return TAFconstant; }
    virtual const char * getAlgorithm()               { return NULL; }
    virtual ICompare * queryCompareSerializedRow()      { return NULL; }
};

class CThorTopNArg : implements IHThorTopNArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIsortarg_1:
            return static_cast<IHThorSortArg *>(this);
        case TAItopnextra_1:
            return static_cast<IHThorTopNExtra *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual double getSkew()                            { return 0; }           // 0=default
    virtual bool hasManyRecords() { return false; }
    virtual double getTargetSkew()                      { return 0; }
    virtual ISortKeySerializer * querySerialize() { return NULL; }
    virtual unsigned __int64 getThreshold() { return 0; }
    virtual IOutputMetaData * querySortedRecordSize() { return NULL; }
    virtual const char * getSortedFilename() { return NULL; }
    virtual ICompare * queryCompareLeftRight() { return NULL; }
    virtual ICompare * queryCompareSerializedRow() { return NULL; }

    virtual bool hasBest() { return false; }
    virtual int compareBest(const void * _left) { return +1; }
};

class CThorSubSortArg : implements IHThorSubSortArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIsortarg_1:
            return static_cast<IHThorSortArg *>(this);
        case TAIsubsortextra_1:
            return static_cast<IHThorSubSortExtra *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual double getSkew()                            { return 0; }           // 0=default
    virtual bool hasManyRecords() { return false; }
    virtual double getTargetSkew()                      { return 0; }
    virtual ISortKeySerializer * querySerialize() { return NULL; }
    virtual unsigned __int64 getThreshold() { return 0; }
    virtual IOutputMetaData * querySortedRecordSize() { return NULL; }
    virtual const char * getSortedFilename() { return NULL; }
    virtual ICompare * queryCompareLeftRight() { return NULL; }
    virtual ICompare * queryCompareSerializedRow() { return NULL; }
};

class CThorKeyedJoinArg : implements IHThorKeyedJoinArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIkeyedjoinbasearg_1:
            return static_cast<IHThorKeyedJoinBaseArg *>(this);
        case TAIfetchcontext_1:
            return static_cast<IHThorFetchContext *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual bool diskAccessRequired() { return false; }
    virtual const char * getFileName() { return NULL; }
    virtual IOutputMetaData * queryDiskRecordSize() { return NULL; }
    virtual unsigned __int64 extractPosition(const void * _right) { return 0; }
    
    // For the data going to the indexRead remote activity:
    virtual bool leftCanMatch(const void * inputRow) { return true; }
    virtual bool indexReadMatch(const void * indexRow, const void * inputRow, unsigned __int64 keyedFpos, IBlobProvider * blobs) { return true; }

    virtual unsigned __int64 getRowLimit()                  { return (unsigned __int64) -1; }
    virtual void onLimitExceeded()                          { }
    virtual unsigned __int64 getSkipLimit()                 { return 0; }
    virtual unsigned getMatchAbortLimit()                   { return 0; }
    virtual void onMatchAbortLimitExceeded()                { }

    virtual unsigned getJoinLimit() { return 0; }
    virtual unsigned getKeepLimit() { return 0; }
    virtual unsigned getJoinFlags() { return 0; }
    virtual bool getIndexLayout(size32_t & _retLen, void * & _retData) { return false; }

    virtual size32_t extractFetchFields(ARowBuilder & rowBuilder, const void * _input) { return 0; }

    virtual bool fetchMatch(const void * diskRow, const void * inputRow) { return true; }

    virtual size32_t createDefaultRight(ARowBuilder & rowBuilder)           { return 0; }

    virtual size32_t onFailTransform(ARowBuilder & rowBuilder, const void * _dummyRight, const void * _origRow, unsigned __int64 keyedFpos, IException * e) { return 0; }
//Join:
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _joinFields, const void * _origRow, unsigned __int64 keyedFpos) { return 0; }
//Denormalize:
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _joinFields, const void * _origRow, unsigned __int64 keyedFpos, unsigned counter) { return 0; }
//Denormalize group:
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _joinFields, const void * _origRow, unsigned _numRows, const void * * _rows) { return 0; }
};

typedef CThorKeyedJoinArg CThorKeyedDenormalizeArg;
typedef CThorKeyedJoinArg CThorKeyedDenormalizeGroupArg;

class CThorJoinArg : implements IHThorJoinArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIjoinbasearg_1:
            return static_cast<IHThorJoinBaseArg *>(this);
        default:
            break;
        }
        return NULL;
    }
    
    virtual bool isLeftAlreadySorted()                      { return false; }
    virtual bool isRightAlreadySorted()                     { return false; }
    virtual size32_t createDefaultLeft(ARowBuilder & rowBuilder)        { return 0; }
    virtual size32_t createDefaultRight(ARowBuilder & rowBuilder)       { return 0; }
    virtual bool match(const void * _left, const void * _right) { return true; }
    virtual ISortKeySerializer * querySerializeLeft()       { return NULL; }
    virtual ISortKeySerializer * querySerializeRight()      { return NULL; }
    virtual unsigned __int64 getThreshold()                 { return 0; }
    virtual double getSkew()                            { return 0; }           // 0=default
    virtual double getTargetSkew()                      { return 0; }           // 0=default
    virtual unsigned getJoinLimit()                     { return 0; }
    virtual unsigned getKeepLimit()                     { return 0; }
    virtual unsigned getJoinFlags()                     { return 0; }
    virtual unsigned getMatchAbortLimit()               { return 0; }
    virtual void onMatchAbortLimitExceeded()            { }
    virtual ICompare * queryCompareLeftRightLower()     { return NULL; }
    virtual ICompare * queryCompareLeftRightUpper()     { return NULL; }
    virtual ICompare * queryPrefixCompare()             { return NULL; }
    virtual ICompare * queryCompareLeftKeyRightRow()    { return NULL; }
    virtual ICompare * queryCompareRightKeyLeftRow()    { return NULL; }

    virtual size32_t onFailTransform(ARowBuilder & rowBuilder, const void * _left, const void * _right, IException * e) { return 0; }

//Join:
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _left, const void * _right) { return 0; }
//Denormalize
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _left, const void * _right, unsigned _count) { return 0; }
//Denormalize group
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _left, const void * _right, unsigned _numRows, const void * * _rows) { return 0; }
};

typedef CThorJoinArg CThorDenormalizeArg;
typedef CThorJoinArg CThorDenormalizeGroupArg;

class CThorAllJoinArg : implements IHThorAllJoinArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIalljoinarg_1:
            return static_cast<IHThorAllJoinArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual size32_t createDefaultLeft(ARowBuilder & rowBuilder)    { return 0; }
    virtual size32_t createDefaultRight(ARowBuilder & rowBuilder)   { return 0; }
    virtual bool match(const void * _left, const void * _right) { return true; }
    virtual unsigned getKeepLimit()                     { return 0; }
    virtual unsigned getJoinFlags()                     { return 0; }

//Join:
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _left, const void * _right) { return 0; }
//Denormalize
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _left, const void * _right, unsigned _count) { return 0; }
//Denormalize group
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _left, const void * _right, unsigned _numRows, const void * * _rows) { return 0; }
};

typedef CThorAllJoinArg CThorAllDenormalizeArg;
typedef CThorAllJoinArg CThorAllDenormalizeGroupArg;

// Used for hash and lookup joins.
class CThorHashJoinArg : implements IHThorHashJoinArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIjoinbasearg_1:
            return static_cast<IHThorJoinBaseArg *>(this);
        case TAIhashjoinextra_1:
            return static_cast<IHThorHashJoinExtra *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual bool isLeftAlreadySorted()                      { return false; }
    virtual bool isRightAlreadySorted()                     { return false; }
    virtual size32_t createDefaultLeft(ARowBuilder & rowBuilder)        { return 0; }
    virtual size32_t createDefaultRight(ARowBuilder & rowBuilder)       { return 0; }
    virtual bool match(const void * _left, const void * _right) { return true; }
    virtual ISortKeySerializer * querySerializeLeft()       { return NULL; }
    virtual ISortKeySerializer * querySerializeRight()      { return NULL; }
    virtual unsigned __int64 getThreshold()                 { return 0; }
    virtual double getSkew()                            { return 0; }           // 0=default
    virtual double getTargetSkew()                      { return 0; }           // 0=default
    virtual unsigned getJoinLimit()                     { return 0; }
    virtual unsigned getKeepLimit()                     { return 0; }
    virtual unsigned getJoinFlags()                     { return 0; }
    virtual unsigned getMatchAbortLimit()               { return 0; }
    virtual void onMatchAbortLimitExceeded()            { }
    virtual ICompare * queryCompareLeftRightLower()     { return NULL; }
    virtual ICompare * queryCompareLeftRightUpper()     { return NULL; }
    virtual ICompare * queryCompareLeft()               { return NULL; }        // not needed for lookup
    virtual ICompare * queryCompareRight()              { return NULL; }        // not needed for many lookup
    virtual ICompare * queryPrefixCompare()             { return NULL; }
    virtual ICompare * queryCompareLeftKeyRightRow()    { return NULL; }
    virtual ICompare * queryCompareRightKeyLeftRow()    { return NULL; }

    virtual size32_t onFailTransform(ARowBuilder & rowBuilder, const void * _left, const void * _right, IException * e) { return 0; }

//Join:
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _left, const void * _right) { return 0; }
//Denormalize
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _left, const void * _right, unsigned _count) { return 0; }
//Denormalize group
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _left, const void * _right, unsigned _numRows, const void * * _rows) { return 0; }
};


typedef CThorHashJoinArg CThorHashDenormalizeArg;
typedef CThorHashJoinArg CThorHashDenormalizeGroupArg;


class CThorKeyedDistributeArg : implements IHThorKeyedDistributeArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIkeyeddistributearg_1:
            return static_cast<IHThorKeyedDistributeArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual unsigned getFlags()                             { return 0; }
    virtual bool getIndexLayout(size32_t & _retLen, void * & _retData) { return false; }
};


class CThorFetchArg : implements IHThorFetchArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIfetchbasearg_1:
            return static_cast<IHThorFetchBaseArg *>(this);
        case TAIfetchcontext_1:
            return static_cast<IHThorFetchContext *>(this);
        case TAIbinfetchextra_1:
            return static_cast<IHThorBinFetchExtra *>(this);
        default:
            break;
        }
        return NULL;
    }
};

class CThorWorkUnitWriteArg : implements IHThorWorkUnitWriteArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }
    virtual IOutputMetaData * queryOutputMeta() { return NULL; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIworkunitwritearg_1:
            return static_cast<IHThorWorkUnitWriteArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual int getSequence()                               { return -3; }
    virtual const char * queryName()                        { return NULL; }
    virtual unsigned getFlags()                             { return 0; }
    virtual void serializeXml(const byte * self, IXmlWriter & out) { rtlSysFail(1, "serializeXml not implemented"); }
    virtual unsigned getMaxSize()                           { return 0; }
};

class CThorXmlWorkunitWriteArg : implements IHThorXmlWorkunitWriteArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }
    virtual IOutputMetaData * queryOutputMeta() { return NULL; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIxmlworkunitwritearg_1:
            return static_cast<IHThorXmlWorkunitWriteArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual int getSequence()                               { return -3; }
    virtual const char * queryName()                        { return NULL; }
    virtual unsigned getFlags()                             { return 0; }
};

class CThorDictionaryWorkUnitWriteArg : implements IHThorDictionaryWorkUnitWriteArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }
    virtual IOutputMetaData * queryOutputMeta() { return NULL; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIdictionaryworkunitwritearg_1:
            return static_cast<IHThorDictionaryWorkUnitWriteArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual int getSequence()                               { return -3; }
    virtual const char * queryName()                        { return NULL; }
    virtual unsigned getFlags()                             { return 0; }
};

class CThorDictionaryResultWriteArg : implements IHThorDictionaryResultWriteArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }
    virtual IOutputMetaData * queryOutputMeta() { return NULL; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIdictionaryresultwritearg_1:
            return static_cast<IHThorDictionaryResultWriteArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual bool usedOutsideGraph() { return true; }
};

class CThorHashDistributeArg : implements IHThorHashDistributeArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIhashdistributearg_1:
            return static_cast<IHThorHashDistributeArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual IHash    * queryHash()                      { return NULL; }
    virtual double getSkew()                            { return 0; }           // 0=default
    virtual double getTargetSkew()                      { return 0; }           // 0=default
    virtual ICompare * queryMergeCompare()              { return NULL; }
};

class CThorHashDedupArg : implements IHThorHashDedupArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIhashdeduparg_1:
            return static_cast<IHThorHashDedupArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual unsigned getFlags() { return HDFkeepleft; }
    virtual ICompare * queryCompareBest() { return NULL; }
    virtual IOutputMetaData * queryKeySize() { return NULL; }
    virtual size32_t recordToKey(ARowBuilder & rowBuilder, const void * _record) { return 0; }
};

class CThorHashMinusArg : implements IHThorHashMinusArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIhashminusarg_1:
            return static_cast<IHThorHashMinusArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};


class CThorIfArg : implements IHThorIfArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }
    virtual IOutputMetaData * queryOutputMeta() { return NULL; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIifarg_1:
            return static_cast<IHThorIfArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};

class CThorCaseArg : implements IHThorCaseArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }
    virtual IOutputMetaData * queryOutputMeta() { return NULL; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIcasearg_1:
            return static_cast<IHThorCaseArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};


class CThorSequentialArg : implements IHThorSequentialArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }
    virtual IOutputMetaData * queryOutputMeta() { return NULL; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIsequentialarg_1:
            return static_cast<IHThorSequentialArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};


class CThorParallelArg : implements IHThorParallelArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }
    virtual IOutputMetaData * queryOutputMeta() { return NULL; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIparallelarg_1:
            return static_cast<IHThorParallelArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};


class CThorKeyDiffArg : implements IHThorKeyDiffArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }
    virtual IOutputMetaData * queryOutputMeta() { return NULL; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIkeydiffarg_1:
            return static_cast<IHThorKeyDiffArg *>(this);
        default:
            break;
        }
        return NULL;
    }
    
    virtual unsigned getFlags()                             { return 0; }
    virtual unsigned getExpiryDays()                        { return 0; }
};

class CThorKeyPatchArg : implements IHThorKeyPatchArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }
    virtual IOutputMetaData * queryOutputMeta() { return NULL; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIkeypatcharg_1:
            return static_cast<IHThorKeyPatchArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual unsigned getFlags()                             { return 0; }
    virtual unsigned getExpiryDays()                        { return 0; }
};


class CThorWorkunitReadArg : implements IHThorWorkunitReadArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIworkunitreadarg_1:
            return static_cast<IHThorWorkunitReadArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual int querySequence() { return -3; }
    virtual const char * getWUID() { return NULL; }
    virtual ICsvToRowTransformer * queryCsvTransformer() { return NULL; }
    virtual IXmlToRowTransformer * queryXmlTransformer() { return NULL; }
};

class CThorLocalResultReadArg : implements IHThorLocalResultReadArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIlocalresultreadarg_1:
            return static_cast<IHThorLocalResultReadArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};

class CThorLocalResultWriteArg : implements IHThorLocalResultWriteArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }
    virtual IOutputMetaData * queryOutputMeta() { return NULL; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIlocalresultwritearg_1:
            return static_cast<IHThorLocalResultWriteArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual bool usedOutsideGraph() { return true; }
};

typedef CThorLocalResultWriteArg CThorLocalResultSpillArg;

class CThorGraphLoopResultReadArg : implements IHThorGraphLoopResultReadArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIgraphloopresultreadarg_1:
            return static_cast<IHThorGraphLoopResultReadArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};

class CThorGraphLoopResultWriteArg : implements IHThorGraphLoopResultWriteArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }
    virtual IOutputMetaData * queryOutputMeta() { return NULL; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIgraphloopresultwritearg_1:
            return static_cast<IHThorGraphLoopResultWriteArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};

//-- Csv --

class CThorCsvWriteArg : implements IHThorCsvWriteArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }
    virtual IOutputMetaData * queryOutputMeta() { return NULL; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIdiskwritearg_1:
            return static_cast<IHThorDiskWriteArg *>(this);
        case TAIcsvwriteextra_1:
            return static_cast<IHThorCsvWriteExtra *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual int getSequence()                               { return -3; }
    virtual unsigned getFlags()                             { return 0; }
    virtual unsigned getTempUsageCount()                    { return 0; }
    virtual unsigned getExpiryDays()                        { return 0; }
    virtual void getUpdateCRCs(unsigned & eclCRC, unsigned __int64 & totalCRC)  { }
    virtual void getEncryptKey(size32_t & keyLen, void * & key) { keyLen = 0; key = 0; }
    virtual const char * getCluster(unsigned idx)         { return NULL; }
};


class CThorCsvFetchArg: implements IHThorCsvFetchArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIfetchbasearg_1:
            return static_cast<IHThorFetchBaseArg *>(this);
        case TAIfetchcontext_1:
            return static_cast<IHThorFetchContext *>(this);
        case TAIcsvfetchextra_1:
            return static_cast<IHThorCsvFetchExtra *>(this);
        default:
            break;
        }
        return NULL;
    }
};

//-- Xml --

class CThorXmlParseArg : implements IHThorXmlParseArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIxmlparsearg_1:
            return static_cast<IHThorXmlParseArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual bool requiresContents() { return false; }
};

class CThorXmlFetchArg : implements IHThorXmlFetchArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIfetchbasearg_1:
            return static_cast<IHThorFetchBaseArg *>(this);
        case TAIfetchcontext_1:
            return static_cast<IHThorFetchContext *>(this);
        case TAIxmlfetchextra_1:
            return static_cast<IHThorXmlFetchExtra *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual bool requiresContents() { return false; }
};

//Simple xml generation...
class CThorXmlWriteArg : implements IHThorXmlWriteArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }
    virtual IOutputMetaData * queryOutputMeta() { return NULL; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIdiskwritearg_1:
            return static_cast<IHThorDiskWriteArg *>(this);
        case TAIxmlwriteextra_1:
            return static_cast<IHThorXmlWriteExtra *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual const char * getXmlIteratorPath()        { return NULL; }             // supplies the prefix and suffix for a row
    virtual const char * getHeader()                 { return NULL; }
    virtual const char * getFooter()                 { return NULL; }
    virtual unsigned getXmlFlags()                     { return 0; }

    virtual int getSequence()                               { return -3; }
    virtual unsigned getFlags()                             { return 0; }
    virtual unsigned getTempUsageCount()                    { return 0; }
    virtual unsigned getExpiryDays()                        { return 0; }
    virtual void getUpdateCRCs(unsigned & eclCRC, unsigned __int64 & totalCRC)  { }
    virtual void getEncryptKey(size32_t & keyLen, void * & key) { keyLen = 0; key = 0; }
    virtual const char * getCluster(unsigned idx)         { return NULL; }
};


//-- SOAP --

class CThorSoapActionArg : implements IHThorSoapActionArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }
    virtual IOutputMetaData * queryOutputMeta() { return NULL; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIsoapactionarg_1:
            return static_cast<IHThorSoapActionArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual void toXML(const byte * self, IXmlWriter & out) { return; }
    virtual const char * getHeader()                  { return NULL; }
    virtual const char * getFooter()                  { return NULL; }
    virtual unsigned getFlags()                         { return 0; }
    virtual unsigned numParallelThreads()               { return 0; }
    virtual unsigned numRecordsPerBatch()               { return 0; }
    virtual int numRetries()                            { return -1; }
    virtual double getTimeout()                          { return -1.0; }
    virtual double getTimeLimit()                        { return 0.0; }
    virtual const char * getSoapAction()              { return NULL; }
    virtual const char * getNamespaceName()           { return NULL; }
    virtual const char * getNamespaceVar()            { return NULL; }
    virtual const char * getHttpHeaderName()          { return NULL; }
    virtual const char * getHttpHeaderValue()         { return NULL; }
    virtual const char * getHttpHeaders()             { return NULL; }
    virtual const char * getProxyAddress()            { return NULL; }
    virtual const char * getAcceptType()              { return NULL; }
    virtual void getLogText(size32_t & lenText, char * & text, const void * left) { lenText =0; text = NULL; }
};

class CThorSoapCallArg : implements IHThorSoapCallArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIsoapactionarg_1:
            return static_cast<IHThorSoapActionArg *>(this);
        case TAIsoapcallextra_1:
            return static_cast<IHThorSoapCallExtra *>(this);
        default:
            break;
        }
        return NULL;
    }
    
//writing to the soap service.
    virtual const char * getInputIteratorPath()       { return NULL; }
    virtual unsigned onFailTransform(ARowBuilder & rowBuilder, const void * left, IException * e) { return 0; }

    virtual void toXML(const byte * self, IXmlWriter & out) { return; }
    virtual const char * getHeader()                  { return NULL; }
    virtual const char * getFooter()                  { return NULL; }
    virtual unsigned getFlags()                         { return 0; }
    virtual unsigned numParallelThreads()               { return 0; }
    virtual unsigned numRecordsPerBatch()               { return 0; }
    virtual int numRetries()                            { return -1; }
    virtual double getTimeout()                          { return -1.0; }
    virtual double getTimeLimit()                        { return 0.0; }
    virtual const char * getSoapAction()              { return NULL; }
    virtual const char * getNamespaceName()           { return NULL; }
    virtual const char * getNamespaceVar()            { return NULL; }
    virtual const char * getHttpHeaderName()          { return NULL; }
    virtual const char * getHttpHeaderValue()         { return NULL; }
    virtual const char * getHttpHeaders()             { return NULL; }
    virtual const char * getProxyAddress()            { return NULL; }
    virtual const char * getAcceptType()              { return NULL; }
    virtual void getLogText(size32_t & lenText, char * & text, const void * left) { lenText =0; text = NULL; }
};
typedef CThorSoapCallArg CThorHttpCallArg;

typedef CThorNullArg CThorDatasetResultArg;
typedef CThorNullArg CThorRowResultArg;
typedef CThorNullArg CThorPullArg;

class CThorParseArg : implements IHThorParseArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIparsearg_1:
            return static_cast<IHThorParseArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual INlpHelper * queryHelper() { return NULL; }
    virtual unsigned getFlags() { return 0; }
    virtual IOutputMetaData * queryProductionMeta(unsigned id) { return NULL; }
    virtual size32_t executeProduction(ARowBuilder & rowBuilder, unsigned id, IProductionCallback * input) { return 0; }
};


class CThorIndexReadArg : implements IHThorIndexReadArg, implements IHThorSourceLimitTransformExtra, public CThorArg
{
protected:
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIcompoundbasearg_1:
        case TAIindexreadbasearg_1:
            return static_cast<IHThorIndexReadBaseArg *>(this);
        case TAIcompoundextra_1:
        case TAIcompoundreadextra_1:
            return static_cast<IHThorCompoundReadExtra *>(this);
        case TAIsourcelimittransformextra_1:
            return static_cast<IHThorSourceLimitTransformExtra *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual unsigned getFlags() { return 0; }
    virtual bool getIndexLayout(size32_t & _retLen, void * & _retData) { return false; }
    virtual void setCallback(IThorIndexCallback * _tc) { fpp = _tc; }

    virtual bool needTransform()                            { return false; }
    virtual bool transformMayFilter()                       { return false; }
    virtual unsigned __int64 getKeyedLimit()                { return (unsigned __int64) -1; }
    virtual void onKeyedLimitExceeded()                     { }
    virtual ISteppingMeta * queryRawSteppingMeta()          { return NULL; }
    virtual ISteppingMeta * queryProjectedSteppingMeta()    { return NULL; }
    virtual void mapOutputToInput(ARowBuilder & rowBuilder, const void * projectedRow, unsigned numFields) {}
    virtual size32_t unfilteredTransform(ARowBuilder & rowBuilder, const void * src) { return 0; }

    virtual size32_t transformOnLimitExceeded(ARowBuilder & rowBuilder) { return 0; }
    virtual size32_t transformOnKeyedLimitExceeded(ARowBuilder & rowBuilder) { return 0; }

public:
    IThorIndexCallback * fpp;
};

class CThorSteppedIndexReadArg : public CThorIndexReadArg, implements IHThorSteppedSourceExtra
{
    virtual void Link() const { CThorIndexReadArg::Link(); }
    virtual bool Release() const { return CThorIndexReadArg::Release(); }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIsteppedsourceextra_1:
            return static_cast<IHThorSteppedSourceExtra *>(this);
        default:
            break;
        }
        return CThorIndexReadArg::selectInterface(which);
    }

    virtual unsigned getSteppedFlags() { return 0; }
    virtual double getPriority() { return 0; }
    virtual unsigned getPrefetchSize() { return 0; }
};

class CThorIndexNormalizeArg : implements IHThorIndexNormalizeArg, implements IHThorSourceLimitTransformExtra, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIcompoundbasearg_1:
        case TAIindexreadbasearg_1:
            return static_cast<IHThorIndexReadBaseArg *>(this);
        case TAIcompoundextra_1:
        case TAIcompoundnormalizeextra_1:
            return static_cast<IHThorCompoundNormalizeExtra *>(this);
        case TAIsourcelimittransformextra_1:
            return static_cast<IHThorSourceLimitTransformExtra *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual unsigned getFlags() { return 0; }
    virtual bool getIndexLayout(size32_t & _retLen, void * & _retData) { return false; }
    virtual void setCallback(IThorIndexCallback * _tc) { fpp = _tc; }

    virtual size32_t transformOnLimitExceeded(ARowBuilder & rowBuilder) { return 0; }
    virtual size32_t transformOnKeyedLimitExceeded(ARowBuilder & rowBuilder) { return 0; }

public:
    IThorIndexCallback * fpp;
};

class CThorIndexAggregateArg : implements IHThorIndexAggregateArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIcompoundbasearg_1:
        case TAIindexreadbasearg_1:
            return static_cast<IHThorIndexReadBaseArg *>(this);
        case TAIcompoundaggregateextra_1:
            return static_cast<IHThorCompoundAggregateExtra *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual unsigned getFlags() { return 0; }
    virtual bool getIndexLayout(size32_t & _retLen, void * & _retData) { return false; }
    virtual void setCallback(IThorIndexCallback * _tc) { fpp = _tc; }
    virtual size32_t mergeAggregate(ARowBuilder & rowBuilder, const void * src) { rtlFailUnexpected(); return 0; }

public:
    IThorIndexCallback * fpp;
};

class CThorIndexCountArg : implements IHThorIndexCountArg, implements IHThorSourceCountLimit, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIcompoundbasearg_1:
        case TAIindexreadbasearg_1:
            return static_cast<IHThorIndexReadBaseArg *>(this);
        case TAIcompoundcountextra_1:
            return static_cast<IHThorCompoundCountExtra *>(this);
        case TAIsourcecountlimit_1:
            return static_cast<IHThorSourceCountLimit *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual unsigned getFlags() { return 0; }
    virtual bool getIndexLayout(size32_t & _retLen, void * & _retData) { return false; }
    virtual void setCallback(IThorIndexCallback * _tc) { fpp = _tc; }

    virtual unsigned __int64 getRowLimit()                  { return (unsigned __int64) -1; }
    virtual void onLimitExceeded()                          { }
    virtual unsigned __int64 getKeyedLimit()                { return (unsigned __int64) -1; }
    virtual void onKeyedLimitExceeded()                     { }

    virtual size32_t numValid(size32_t srcLen, const void * _src)
    {
        rtlFailUnexpected();
        return 0;
    }

public:
    IThorIndexCallback * fpp;
};

class CThorIndexGroupAggregateArg : implements IHThorIndexGroupAggregateArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

protected:
    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIcompoundbasearg_1:
        case TAIindexreadbasearg_1:
            return static_cast<IHThorIndexReadBaseArg *>(this);
        case TAIrowaggregator_1:
        case TAIcompoundgroupaggregateextra_1:
            return static_cast<IHThorCompoundGroupAggregateExtra *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual unsigned getFlags() { return 0; }
    virtual bool getIndexLayout(size32_t & _retLen, void * & _retData) { return false; }
    virtual void setCallback(IThorIndexCallback * _tc) { fpp = _tc; }
    virtual bool createGroupSegmentMonitors(IIndexReadContext *ctx) { return false; }
    virtual unsigned getGroupSegmentMonitorsSize() { return 0; }
    virtual size32_t initialiseCountGrouping(ARowBuilder & rowBuilder, const void * src) { rtlFailUnexpected(); return 0; }
    virtual size32_t processCountGrouping(ARowBuilder & rowBuilder, unsigned __int64 count) { rtlFailUnexpected(); return 0; }
    virtual size32_t mergeAggregate(ARowBuilder & rowBuilder, const void * src) { rtlFailUnexpected(); return 0; }

public:
    IThorIndexCallback * fpp;
};


class CThorDiskReadArg : implements IHThorDiskReadArg, implements IHThorSourceLimitTransformExtra, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIcompoundbasearg_1:
        case TAIdiskreadbasearg_1:
            return static_cast<IHThorDiskReadBaseArg *>(this);
        case TAIcompoundextra_1:
        case TAIcompoundreadextra_1:
            return static_cast<IHThorCompoundReadExtra *>(this);
        case TAIsourcelimittransformextra_1:
            return static_cast<IHThorSourceLimitTransformExtra *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual unsigned getFlags() { return 0; }
    virtual void setCallback(IThorDiskCallback * _tc) { fpp = _tc; }

    virtual bool needTransform()                            { return false; }
    virtual bool transformMayFilter()                       { return false; }
    virtual unsigned __int64 getKeyedLimit()                { return (unsigned __int64) -1; }
    virtual void onKeyedLimitExceeded()                     { }
    virtual ISteppingMeta * queryRawSteppingMeta()          { return NULL; }
    virtual ISteppingMeta * queryProjectedSteppingMeta()    { return NULL; }
    virtual void mapOutputToInput(ARowBuilder & rowBuilder, const void * projectedRow, unsigned numFields) { }
    virtual size32_t unfilteredTransform(ARowBuilder & rowBuilder, const void * src) { return 0; }

    virtual size32_t transformOnLimitExceeded(ARowBuilder & rowBuilder) { return 0; }
    virtual size32_t transformOnKeyedLimitExceeded(ARowBuilder & rowBuilder) { return 0; }

public:
    IThorDiskCallback * fpp;
};

class CThorDiskNormalizeArg : implements IHThorDiskNormalizeArg, implements IHThorSourceLimitTransformExtra, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIcompoundbasearg_1:
        case TAIdiskreadbasearg_1:
            return static_cast<IHThorDiskReadBaseArg *>(this);
        case TAIcompoundextra_1:
        case TAIcompoundnormalizeextra_1:
            return static_cast<IHThorCompoundNormalizeExtra *>(this);
        case TAIsourcelimittransformextra_1:
            return static_cast<IHThorSourceLimitTransformExtra *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual unsigned getFlags() { return 0; }
    virtual void setCallback(IThorDiskCallback * _tc) { fpp = _tc; }

    virtual size32_t transformOnLimitExceeded(ARowBuilder & rowBuilder) { return 0; }
    virtual size32_t transformOnKeyedLimitExceeded(ARowBuilder & rowBuilder) { return 0; }

public:
    IThorDiskCallback * fpp;
};

class CThorDiskAggregateArg : implements IHThorDiskAggregateArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIcompoundbasearg_1:
        case TAIdiskreadbasearg_1:
            return static_cast<IHThorDiskReadBaseArg *>(this);
        case TAIcompoundaggregateextra_1:
            return static_cast<IHThorCompoundAggregateExtra *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual unsigned getFlags() { return 0; }
    virtual void setCallback(IThorDiskCallback * _tc) { fpp = _tc; }
    virtual size32_t mergeAggregate(ARowBuilder & rowBuilder, const void * src) { rtlFailUnexpected(); return 0; }

public:
    IThorDiskCallback * fpp;
};

class CThorDiskCountArg : implements IHThorDiskCountArg, implements IHThorSourceCountLimit, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIcompoundbasearg_1:
        case TAIdiskreadbasearg_1:
            return static_cast<IHThorDiskReadBaseArg *>(this);
        case TAIcompoundcountextra_1:
            return static_cast<IHThorCompoundCountExtra *>(this);
        case TAIsourcecountlimit_1:
            return static_cast<IHThorSourceCountLimit *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual unsigned getFlags() { return 0; }
    virtual void setCallback(IThorDiskCallback * _tc) { fpp = _tc; }

    virtual unsigned __int64 getRowLimit()                  { return (unsigned __int64) -1; }
    virtual void onLimitExceeded()                          { }
    virtual unsigned __int64 getKeyedLimit()                { return (unsigned __int64) -1; }
    virtual void onKeyedLimitExceeded()                     { }

public:
    IThorDiskCallback * fpp;
};

class CThorDiskGroupAggregateArg : implements IHThorDiskGroupAggregateArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIcompoundbasearg_1:
        case TAIdiskreadbasearg_1:
            return static_cast<IHThorDiskReadBaseArg *>(this);
        case TAIrowaggregator_1:
        case TAIcompoundgroupaggregateextra_1:
            return static_cast<IHThorCompoundGroupAggregateExtra *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual unsigned getFlags() { return 0; }
    virtual void setCallback(IThorDiskCallback * _tc) { fpp = _tc; }
    virtual bool createGroupSegmentMonitors(IIndexReadContext *ctx) { return false; }
    virtual unsigned getGroupSegmentMonitorsSize() { return 0; }
    virtual size32_t initialiseCountGrouping(ARowBuilder & rowBuilder, const void * src) { rtlFailUnexpected(); return 0; }
    virtual size32_t processCountGrouping(ARowBuilder & rowBuilder, unsigned __int64 count) { rtlFailUnexpected(); return 0; }
    virtual size32_t mergeAggregate(ARowBuilder & rowBuilder, const void * src) { rtlFailUnexpected(); return 0; }

public:
    IThorDiskCallback * fpp;
};


class CThorCsvReadArg: implements IHThorCsvReadArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIcompoundbasearg_1:
        case TAIdiskreadbasearg_1:
        case TAIcsvreadarg_1:
            return static_cast<IHThorCsvReadArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual unsigned getFlags()                             { return 0; }
    virtual unsigned __int64 getChooseNLimit()              { return I64C(0x7fffffffffffffff); }
    virtual unsigned __int64 getRowLimit()                  { return (unsigned __int64) -1; }
    virtual void onLimitExceeded()                          { }
    virtual unsigned getFormatCrc()                         { return 0; }   // no meaning
    virtual void setCallback(IThorDiskCallback * _tc) { fpp = _tc; }

public:
    IThorDiskCallback * fpp;
};

class CThorXmlReadArg: implements IHThorXmlReadArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIcompoundbasearg_1:
        case TAIdiskreadbasearg_1:
        case TAIxmlreadarg_1:
            return static_cast<IHThorXmlReadArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual unsigned getFlags()                             { return 0; }
    virtual unsigned __int64 getChooseNLimit()              { return I64C(0x7fffffffffffffff); }
    virtual unsigned __int64 getRowLimit()                  { return (unsigned __int64) -1; }
    virtual void onLimitExceeded()                          { }
    virtual unsigned getFormatCrc()                         { return 0; }   // no meaning
    virtual void setCallback(IThorDiskCallback * _tc) { fpp = _tc; }

public:
    IThorDiskCallback * fpp;
};

//Normalize
class CThorChildNormalizeArg : implements IHThorChildNormalizeArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIchildnormalizearg_1:
            return static_cast<IHThorChildNormalizeArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};

//Aggregate
class CThorChildAggregateArg : implements IHThorChildAggregateArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIchildaggregatearg_1:
            return static_cast<IHThorChildAggregateArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};

//NormalizedAggregate
//NB: The child may actually be a grandchild/great-grand child, so need to store some sort of current state in the hash table
class CThorChildGroupAggregateArg : implements IHThorChildGroupAggregateArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIchildgroupaggregatearg_1:
            return static_cast<IHThorChildGroupAggregateBaseArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual size32_t mergeAggregate(ARowBuilder & rowBuilder, const void * src) { rtlFailUnexpected(); return 0; }
};

//Normalize
class CThorChildThroughNormalizeArg : implements IHThorChildThroughNormalizeArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIchildthroughnormalizebasearg_1:
            return static_cast<IHThorChildThroughNormalizeBaseArg *>(this);
        case TAIcompoundextra_1:
        case TAIcompoundnormalizeextra_1:
            return static_cast<IHThorCompoundNormalizeExtra *>(this);
        default:
            break;
        }
        return NULL;
    }
};

class CThorLoopArg : implements IHThorLoopArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIlooparg_1:
            return static_cast<IHThorLoopArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual unsigned getFlags() { return 0; }
    virtual bool sendToLoop(unsigned counter, const void * in) { return true; }
    virtual unsigned numIterations() { return 0; }
    virtual bool loopAgain(unsigned counter, unsigned num, const void * * _rows)    { return num != 0; }
    virtual unsigned defaultParallelIterations() { return 0; }
    virtual bool loopFirstTime() { return false; }
    virtual unsigned loopAgainResult() { return 0; }
};


class CThorGraphLoopArg : implements IHThorGraphLoopArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIgraphlooparg_1:
            return static_cast<IHThorGraphLoopArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual unsigned getFlags() { return 0; }
};


class CThorRemoteArg : implements IHThorRemoteArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIremotearg_1:
            return static_cast<IHThorRemoteArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual IOutputMetaData * queryOutputMeta()             { return NULL; }        // for action variety
    virtual unsigned __int64 getRowLimit()                  { return 10000; }
    virtual void onLimitExceeded()                          { rtlSysFail(1, "Too many records returned from ALLNODES()"); }
};


class CThorLibraryCallArg : implements IHThorLibraryCallArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }
    virtual IOutputMetaData * queryOutputMeta()             { return NULL; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIlibrarycallarg_1:
            return static_cast<IHThorLibraryCallArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};

class CThorNWayInputArg : implements IHThorNWayInputArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAInwayinputarg_1:
            return static_cast<IHThorNWayInputArg *>(this);
        default:
            break;
        }
        return NULL;
    }

};

class CThorNWayGraphLoopResultReadArg : implements IHThorNWayGraphLoopResultReadArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAInwaygraphloopresultreadarg_1:
            return static_cast<IHThorNWayGraphLoopResultReadArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual bool isGrouped() const { return false; }
};

class CThorNWayMergeArg : implements IHThorNWayMergeArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAInwaymergearg_1:
            return static_cast<IHThorMergeArg *>(this);
        case TAInwaymergeextra_1:
            return static_cast<IHThorNWayMergeExtra *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual ISortKeySerializer * querySerialize()       { return NULL; }        // only if global
    virtual ICompare * queryCompareKey()                { return NULL; }
    virtual ICompare * queryCompareRowKey()             { return NULL; }        // row is lhs, key is rhs
    virtual bool dedup()                                { return false; }
    virtual ISteppingMeta * querySteppingMeta()         { return NULL; }
};

class CThorNWayMergeJoinArg : implements IHThorNWayMergeJoinArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAInwaymergejoinarg_1:
            return static_cast<IHThorNWayMergeJoinArg *>(this);
        default:
            break;
        }
        return NULL;
    }

    virtual unsigned getJoinFlags() { return 0; }

    virtual ICompareEq * queryNonSteppedCompare()       { return NULL; }

//For range comparison
    virtual void adjustRangeValue(ARowBuilder & rowBuilder, const void * input, __int64 delta) {}
    virtual unsigned __int64 extractRangeValue(const void * input) { return 0; }
    virtual __int64 maxRightBeforeLeft() { return 0; }
    virtual __int64 maxLeftBeforeRight() { return 0; }

//MJFtransform
    virtual size32_t transform(ARowBuilder & rowBuilder, unsigned _num, const void * * _rows) { return 0; }

//MJFleftonly helper
    virtual bool createNextJoinValue(ARowBuilder & rowBuilder, const void * _value) { return false; }

//MJFmofn helper
    virtual unsigned getMinMatches() { return 0; }
    virtual unsigned getMaxMatches() { return 0x7fffffff; }

    virtual INaryCompareEq * queryGlobalCompare() { return NULL; }
    virtual size32_t createLowInputRow(ARowBuilder & rowBuilder) { return 0; }
    virtual ICompareEq * queryPartitionCompareEq() { return NULL; }
};


class CThorNWaySelectArg : implements IHThorNWaySelectArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAInwayselectarg_1:
            return static_cast<IHThorNWaySelectArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};

class CThorSectionArg : implements IHThorSectionArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIsectionarg_1:
            return static_cast<IHThorSectionArg *>(this);
        default:
            break;
        }
        return NULL;
    }
    
    virtual unsigned getFlags() { return 0; }
    virtual void getDescription(size32_t & _retLen, char * & _retData) { _retLen = 0; _retData = NULL; }
};

class CThorSectionInputArg : implements IHThorSectionInputArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIsectioninputarg_1:
            return static_cast<IHThorSectionInputArg *>(this);
        default:
            break;
        }
        return NULL;
    }
    
    virtual unsigned getFlags() { return 0; }
};

class CThorTraceArg : implements IHThorTraceArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual bool isValid(const void * _left) { return true; }
    virtual bool canMatchAny() { return true; }
    virtual unsigned getKeepLimit() { return (unsigned) -1; }
    virtual unsigned getSample() { return 0; }
    virtual unsigned getSkip() { return 0; }
    virtual const char *getName() { return NULL; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAItracearg_1:
            return static_cast<IHThorTraceArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};


class CThorWhenActionArg : implements IHThorWhenActionArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIwhenactionarg_1:
            return static_cast<IHThorWhenActionArg *>(this);
        default:
            break;
        }
        return NULL;
    }
};

class CThorStreamedIteratorArg : implements IHThorStreamedIteratorArg, public CThorArg
{
    virtual void Link() const { RtlCInterface::Link(); }
    virtual bool Release() const { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) { ctx = _ctx; }

    virtual IInterface * selectInterface(ActivityInterfaceEnum which)
    {
        switch (which)
        {
        case TAIarg:
        case TAIstreamediteratorarg_1:
            return static_cast<IHThorStreamedIteratorArg *>(this);
        default:
            break;
        }
        return NULL;
    }
    
};

//-- Full implementations of selective activities that don't ever require any access to the context.

class CLibraryNullArg : public CThorNullArg
{
public:
    inline CLibraryNullArg(IOutputMetaData * _meta = NULL) : meta(_meta) {}

    virtual IOutputMetaData * queryOutputMeta() { return meta; }

protected:
    IOutputMetaData * meta;
};

class CLibrarySplitArg : public CThorSplitArg
{
public:
    CLibrarySplitArg(unsigned _tempUsageCount, bool _balanced, IOutputMetaData * _meta) :
        tempUsageCount(_tempUsageCount), balanced(_balanced), meta(_meta) {}

    virtual unsigned numBranches()                          { return tempUsageCount; }
    virtual bool isBalanced()                               { return balanced; }
    virtual IOutputMetaData * queryOutputMeta()             { return meta; }

protected:
    unsigned tempUsageCount;
    IOutputMetaData * meta;
    bool balanced;
};

class CLibraryFunnelArg : public CThorFunnelArg
{
public:
    CLibraryFunnelArg(bool _ordered, bool _sequential, IOutputMetaData * _meta) :
        ordered(_ordered), sequential(_sequential), meta(_meta) {}

    virtual bool isOrdered()                            { return ordered; }
    virtual bool pullSequentially()                     { return sequential; }
    virtual IOutputMetaData * queryOutputMeta()             { return meta; }

protected:
    IOutputMetaData * meta;
    bool ordered;
    bool sequential;
};


class CLibraryLocalResultSpillArg : public CThorLocalResultSpillArg
{
public:
    CLibraryLocalResultSpillArg(unsigned _sequence, bool _usedOutside, IOutputMetaData * _meta) :
        sequence(_sequence), usedOutside(_usedOutside), meta(_meta) {}

    virtual unsigned querySequence() { return sequence; }
    virtual bool usedOutsideGraph() { return usedOutside; }
    virtual IOutputMetaData * queryOutputMeta()             { return meta; }

protected:
    unsigned sequence;
    IOutputMetaData * meta;
    bool usedOutside;
};


class CLibraryWorkUnitReadArg : public CThorWorkunitReadArg
{
public:
    CLibraryWorkUnitReadArg(const char * _name, IOutputMetaData * _meta) :
        name(_name), meta(_meta) {}

    virtual const char * queryName() { return name; }
    virtual IOutputMetaData * queryOutputMeta()             { return meta; }

protected:
    const char * name;
    IOutputMetaData * meta;
};


class CLibraryWorkUnitWriteArg : public CThorWorkUnitWriteArg
{
public:
    CLibraryWorkUnitWriteArg(const char * _name, unsigned _flags, IOutputMetaData * _meta) :
        name(_name), flags(_flags), meta(_meta)  {}

    virtual const char * queryName() { return name; }
    virtual unsigned getFlags() { return flags; }
    virtual IOutputMetaData * queryOutputMeta()             { return meta; }

protected:
    const char * name;
    IOutputMetaData * meta;
    unsigned flags;
};


class CLibraryMemorySpillSplitArg : public CThorSpillArg
{
public:
    CLibraryMemorySpillSplitArg(unsigned _tempUsageCount, const char * _filename, IOutputMetaData * _meta) :
        tempUsageCount(_tempUsageCount), filename(_filename), meta(_meta) {}

    virtual unsigned getFlags()                             { return TDXtemporary|TDXcompress|TDWnoreplicate; }
    virtual unsigned getTempUsageCount()                    { return tempUsageCount; }
    virtual unsigned getFormatCrc()                         { rtlFailUnexpected(); return 0; }
    virtual IOutputMetaData * queryOutputMeta()             { return meta; }
    virtual const char * queryRecordECL()                   { rtlFailUnexpected(); return NULL; }
    virtual const char * getFileName()                      { return filename; }

protected:
    IOutputMetaData * meta;
    const char * filename;
    unsigned tempUsageCount;
};


class CLibraryMemorySpillReadArg : public CThorDiskReadArg
{
public:
    CLibraryMemorySpillReadArg(const char * _filename, IOutputMetaData * _meta) :
        filename(_filename), meta(_meta) {}

    virtual unsigned getFlags()                             { return TDXtemporary|TDXcompress; }
    virtual unsigned getFormatCrc()                         { rtlFailUnexpected(); return 0; }
    virtual IOutputMetaData * queryDiskRecordSize()             { return meta; }
    virtual IOutputMetaData * queryOutputMeta()             { return meta; }
    virtual const char * getFileName()                      { return filename; }
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _left) { rtlFailUnexpected(); return 0; }

protected:
    const char * filename;
    IOutputMetaData * meta;
};


class CLibraryWhenActionArg : public CThorWhenActionArg
{
public:
    inline CLibraryWhenActionArg(IOutputMetaData * _meta) : meta(_meta) {}

    virtual IOutputMetaData * queryOutputMeta() { return meta; }

protected:
    IOutputMetaData * meta;
};


class CLibraryDegroupArg : public CThorDegroupArg
{
public:
    inline CLibraryDegroupArg(IOutputMetaData * _meta = NULL) : meta(_meta) {}

    virtual IOutputMetaData * queryOutputMeta() { return meta; }

protected:
    IOutputMetaData * meta;
};


typedef size32_t (*rowClearFunction)(ARowBuilder & crSelf, IResourceContext * ctx);
class CLibrarySelectNArg : public CThorSelectNArg
{
public:
    inline CLibrarySelectNArg(unsigned __int64 _row, rowClearFunction _rowClear, IOutputMetaData * _meta = NULL)
        : row(_row), rowClear(_rowClear), meta(_meta) {}

    virtual IOutputMetaData * queryOutputMeta() { return meta; }
    virtual unsigned __int64 getRowToSelect() { return row; }
    virtual size32_t createDefault(ARowBuilder & rowBuilder) { return rowClear(rowBuilder, ctx); }

protected:
    IOutputMetaData * meta;
    unsigned __int64 row;
    rowClearFunction rowClear;
};


class CLibraryLocalResultReadArg : public CThorLocalResultReadArg
{
public:
    inline CLibraryLocalResultReadArg(unsigned _sequence, IOutputMetaData * _meta = NULL)
        : sequence(_sequence), meta(_meta) {}

    virtual IOutputMetaData * queryOutputMeta() { return meta; }
    virtual unsigned querySequence() { return sequence; }

protected:
    IOutputMetaData * meta;
    unsigned sequence;
};


class CLibraryGraphLoopResultWriteArg : public CThorGraphLoopResultWriteArg
{
public:
    inline CLibraryGraphLoopResultWriteArg(IOutputMetaData * _meta)
        : meta(_meta) {}

    virtual IOutputMetaData * queryOutputMeta() { return meta; }

protected:
    IOutputMetaData * meta;
};

class CLibraryCountAggregateArg : public CThorCountAggregateArg
{
public:
    inline CLibraryCountAggregateArg(IOutputMetaData * _meta) : meta(_meta) {}

    virtual IOutputMetaData * queryOutputMeta() { return meta; }

protected:
    IOutputMetaData * meta;
};

class CLibraryExistsAggregateArg : public CThorExistsAggregateArg
{
public:
    inline CLibraryExistsAggregateArg(IOutputMetaData * _meta) : meta(_meta) {}

    virtual IOutputMetaData * queryOutputMeta() { return meta; }

protected:
    IOutputMetaData * meta;
};

class CLibraryConstantRawIteratorArg : public CThorLinkedRawIteratorArg
{
public:
    inline CLibraryConstantRawIteratorArg(unsigned _numRows, byte * * _rows, IOutputMetaData * _meta)
        : meta(_meta), numRows(_numRows), rows(_rows)
    {
        cur = 0;
    }

    virtual void onStart(const byte *, MemoryBuffer * ) { cur = 0U; }

    virtual byte * next()
    {
        if (cur < numRows)
            return rows[cur++];
        return NULL;
    }

    virtual IOutputMetaData * queryOutputMeta() { return meta; }

protected:
    unsigned numRows;
    IOutputMetaData * meta;
    byte * * rows;
    unsigned cur;
};

class EclProcess : implements IEclProcess, public RtlCInterface
{
public:
    RTLIMPLEMENT_IINTERFACE
};



#endif
