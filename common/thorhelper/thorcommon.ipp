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

#ifndef THORCOMMON_IPP
#define THORCOMMON_IPP

#include "eclrtl.hpp"
#include "thorcommon.hpp"
#include "jsort.hpp"
#include "rtlds_imp.hpp"
#include "jfile.hpp"

#ifdef THORHELPER_EXPORTS
#define THORHELPER_API DECL_EXPORT
#else
#define THORHELPER_API DECL_IMPORT
#endif

//------------------------------------------------------------------------------------------------

//An inline caching version of an IOutputMetaDataEx, which hides the backward compatibility issues, and caches
//fixed record information (similar to CachedRecordSize) for efficient access.

class THORHELPER_API CachedOutputMetaData
{
public:
    explicit inline CachedOutputMetaData(IOutputMetaData * _rs = NULL) { set(_rs); }

    inline void set(IOutputMetaData * _meta)
    {
        meta.set(_meta);
        if (_meta)
        {
            fixedSize = _meta->getFixedSize();
            minSize = _meta->getMinRecordSize();
            initialSize = minSize;
            metaFlags = meta->getMetaFlags();
        }
        else
        {
            initialSize = 0;
            fixedSize = 0;
            metaFlags = 0;
            minSize = 0;
        }
    }

//extra helpers
    inline size32_t getInitialSize() const                  { return initialSize; }
    inline size32_t getMinRecordSize() const                { return minSize; }
    inline bool isFixedSize() const                         { return (fixedSize != 0); }
    inline bool isVariableSize() const                      { return (fixedSize == 0); }

//IRecordSize
    inline size32_t getRecordSize(const void *rec) const    { return fixedSize ? fixedSize : meta->getRecordSize(rec); }
    inline size32_t getFixedSize() const                    { return fixedSize; }

//IOutputMetaData
    inline bool isGrouped() const                           { return (metaFlags & MDFgrouped) != 0; }
    inline void toXML(const byte * self, IXmlWriter & out)  { meta->toXML(self, out); }
    inline bool hasXML() const                              { return (metaFlags & MDFhasxml) != 0; }

//v1 member functions (can be called on any interface)
    inline unsigned getMetaFlags() const                    { return metaFlags; }
    inline bool needsDestruct() const                       { return (metaFlags & MDFneeddestruct) != 0; }
    inline bool needsSerializeDisk() const                  { return (metaFlags & MDFneedserializedisk) != 0; }
    inline void destruct(byte * self)
    {
        if (metaFlags & MDFneeddestruct)
            meta->destruct(self);
    }

    IOutputRowSerializer * createDiskSerializer(ICodeContext * ctx, unsigned activityId) const;
    IOutputRowDeserializer * createDiskDeserializer(ICodeContext * ctx, unsigned activityId) const;
    IOutputRowSerializer * createInternalSerializer(ICodeContext * ctx, unsigned activityId) const;
    IOutputRowDeserializer * createInternalDeserializer(ICodeContext * ctx, unsigned activityId) const;

    inline IOutputMetaData * querySerializedDiskMeta() const
    {
        if (metaFlags & MDFneedserializedisk)
            return meta->querySerializedDiskMeta();
        return meta;
    }
    inline IOutputMetaData * queryChildMeta(unsigned i) const
    {
        return meta->queryChildMeta(i);
    }

//cast operators.
    inline IOutputMetaData * queryOriginal() const          { return meta; }
    inline operator IRecordSize * () const                  { return meta; }
    inline operator IOutputMetaData * () const              { return meta; }

private:
    Owned<IOutputMetaData> meta;
    size32_t fixedSize;
    size32_t initialSize;
    size32_t minSize;
    unsigned metaFlags;
};

//------------------------------------------------------------------------------------------------

class THORHELPER_API MemoryBufferBuilder : public RtlRowBuilderBase
{
public:
    MemoryBufferBuilder(MemoryBuffer & _buffer, unsigned _minSize)
        : buffer(_buffer), minSize(_minSize)
    {
        reserved = 0;
    }

    virtual byte * ensureCapacity(size32_t required, const char * fieldName)
    {
        if (required > reserved)
        {
            void * next = buffer.reserve(required-reserved);
            self = (byte *)next - reserved;
            reserved = required;
        }
        return self;
    }

    void finishRow(size32_t length)
    {
        assertex(length <= reserved);
        size32_t newLength = (buffer.length() - reserved) + length;
        buffer.setLength(newLength);
        self = NULL;
        reserved = 0;
    }
    virtual IEngineRowAllocator *queryAllocator() const
    {
        return NULL;
    }

protected:
    virtual byte * createSelf()
    {
        return ensureCapacity(minSize, NULL);
    }

protected:
    MemoryBuffer & buffer;
    size32_t minSize;
    size32_t reserved;
};



//------------------------------------------------------------------------------------------------

//This class is only ever used to apply a delta to a self pointer, it is never finalized, and the builder must stay alive.
class THORHELPER_API CPrefixedRowBuilder : implements RtlRowBuilderBase
{
public:
    inline CPrefixedRowBuilder(size32_t _offset, ARowBuilder & _builder) : offset(_offset), builder(_builder)
    { 
        self = builder.getSelf()+offset;
    }

    virtual byte * ensureCapacity(size32_t required, const char * fieldName)
    {
        self = builder.ensureCapacity(offset+required, fieldName) + offset;
        return getSelf();
    }

    virtual byte * createSelf()
    {
        self = builder.getSelf()+offset;
        return self;
    }

    virtual IEngineRowAllocator *queryAllocator() const
    {
        return builder.queryAllocator();
    }
protected:
    size32_t offset;
    ARowBuilder & builder;
};

//------------------------------------------------------------------------------------------------

class AggregateRowBuilder : public RtlDynamicRowBuilder, public CInterface
{
public:
    AggregateRowBuilder(IEngineRowAllocator *_rowAllocator, unsigned _elementHash)
        : RtlDynamicRowBuilder(_rowAllocator, true), elementHash(_elementHash)
    {
        size = 0;
    }
    inline unsigned queryHash() const
    {
        return elementHash;
    }
    inline void setSize(size32_t _newSize)
    {
        size = _newSize;
    }
    inline const void *finalizeRowClear()
    {
        return RtlDynamicRowBuilder::finalizeRowClear(size);
    }
    inline size32_t querySize() const
    {
        return size;
    }
private:
    unsigned elementHash;
    size32_t size;
};

class THORHELPER_API RowAggregator : private SuperHashTable
{
    // We have to be careful with ownership of the items in the hashtable. Because we free them as we iterate, we need to make sure
    // that we always do exactly one iteration through the hash table. We therefore DON'T free anything in onRemove.
    
public:
    RowAggregator(IHThorHashAggregateExtra &_extra, IHThorRowAggregator & _helper);
    ~RowAggregator();
    IMPLEMENT_IINTERFACE

    void reset();
    void start(IEngineRowAllocator *rowAllocator);
    AggregateRowBuilder &addRow(const void * row);
    void mergeElement(const void * otherElement);
    AggregateRowBuilder *nextResult();
    unsigned elementCount() const { return count(); }
    memsize_t queryMem() const { return SuperHashTable::queryMem() + totalSize + overhead; };

protected:
    virtual void onAdd(void *et) {}
    virtual void onRemove(void *et) {} 
    virtual unsigned getHashFromElement(const void *et) const { return hashFromElement(et); }
    virtual unsigned getHashFromFindParam(const void *fp) const { return hasher->hash(fp); }
    virtual const void * getFindParam(const void *et) const;
    virtual bool matchesFindParam(const void *et, const void *key, unsigned fphash) const;
    virtual bool matchesElement(const void *et, const void * searchET) const;

private:
    void releaseAll(void); // No implementation.

    inline unsigned hashFromElement(const void * et) const
    {
        const AggregateRowBuilder *rb = static_cast<const AggregateRowBuilder*>(et);
        return rb->queryHash();
    }

    IHash * hasher;
    IHash * elementHasher;
    ICompare * comparer;
    ICompare * elementComparer;
    IHThorRowAggregator & helper;
    const void * cursor;
    bool eof;
    Owned<IEngineRowAllocator> rowAllocator;
    memsize_t totalSize, overhead;
};

//------------------------------------------------------------------------------------------------

class THORHELPER_API CPrefixedRowSerializer : implements IOutputRowSerializer, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    CPrefixedRowSerializer(size32_t _offset, IOutputRowSerializer *_original) : offset(_offset), original(_original)
    {
    }
    virtual void serialize(IRowSerializerTarget & out, const byte * self)
    {
        out.put(offset, self);
        original->serialize(out, self+offset);
    }

protected:
    size32_t offset; 
    Owned<IOutputRowSerializer> original;
};

class THORHELPER_API CPrefixedRowDeserializer : implements IOutputRowDeserializer, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    CPrefixedRowDeserializer(size32_t _offset, IOutputRowDeserializer *_original) : offset(_offset), original(_original)
    {
    }

    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in)
    {
        in.read(offset, rowBuilder.getSelf());

        //Need to apply a delta to the self return from getSelf()
        CPrefixedRowBuilder prefixedBuilder(offset, rowBuilder);
        return original->deserialize(prefixedBuilder, in)+offset;
    }
    
protected:
    size32_t offset; 
    Owned<IOutputRowDeserializer> original;
};

class THORHELPER_API CPrefixedRowPrefetcher : implements ISourceRowPrefetcher, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    CPrefixedRowPrefetcher(size32_t _offset, ISourceRowPrefetcher *_original) : offset(_offset), original(_original)
    {
    }

    virtual void readAhead(IRowDeserializerSource & in)
    {
        in.skip(offset);
        original->readAhead(in);
    }
    
protected:
    size32_t offset; 
    Owned<ISourceRowPrefetcher> original;
};

class THORHELPER_API CPrefixedOutputMeta : implements IOutputMetaData, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    CPrefixedOutputMeta(size32_t _offset, IOutputMetaData *_original)
    {
        offset = _offset;
        original = _original;
        IOutputMetaData * originalSerialized = _original->querySerializedDiskMeta();
        if (originalSerialized != original)
            serializedMeta.setown(new CPrefixedOutputMeta(_offset, originalSerialized));
    }
    
    virtual size32_t getRecordSize(const void *rec) 
    {
        if (rec)
            rec = ((const byte *) rec) + offset;
        return original->getRecordSize(rec) + offset;
    }
    
    virtual size32_t getMinRecordSize() const
    {
        return original->getMinRecordSize() + offset;
    }

    virtual size32_t getFixedSize() const
    {
        size32_t ret = original->getFixedSize();
        if (ret)
            ret += offset;
        return ret;
    }

    virtual void toXML(const byte * self, IXmlWriter & out)  { original->toXML(self+offset, out); }
    virtual unsigned getVersion() const { return original->getVersion(); }

    virtual unsigned getMetaFlags() { return original->getMetaFlags(); }
    virtual void destruct(byte * self) { original->destruct(self+offset); }
    virtual IOutputRowSerializer * createDiskSerializer(ICodeContext * ctx, unsigned activityId)
    {
        return new CPrefixedRowSerializer(offset, original->createDiskSerializer(ctx, activityId));
    }
    virtual IOutputRowDeserializer * createDiskDeserializer(ICodeContext * ctx, unsigned activityId) 
    {
        return new CPrefixedRowDeserializer(offset, original->createDiskDeserializer(ctx, activityId));
    }
    virtual ISourceRowPrefetcher * createDiskPrefetcher(ICodeContext * ctx, unsigned activityId) 
    {
        return new CPrefixedRowPrefetcher(offset, original->createDiskPrefetcher(ctx, activityId));
    }
    virtual IOutputMetaData * querySerializedDiskMeta()
    {
        if (serializedMeta)
            return serializedMeta.get();
        return this;
    }
    virtual IOutputRowSerializer * createInternalSerializer(ICodeContext * ctx, unsigned activityId)
    {
        return new CPrefixedRowSerializer(offset, original->createInternalSerializer(ctx, activityId));
    }
    virtual IOutputRowDeserializer * createInternalDeserializer(ICodeContext * ctx, unsigned activityId)
    {
        return new CPrefixedRowDeserializer(offset, original->createInternalDeserializer(ctx, activityId));
    }
    virtual void walkIndirectMembers(const byte * self, IIndirectMemberVisitor & visitor)
    {
        original->walkIndirectMembers(self+offset, visitor);
    }
    virtual IOutputMetaData * queryChildMeta(unsigned i)
    {
        return original->queryChildMeta(i);
    }
    virtual const RtlRecord &queryRecordAccessor(bool expand) const
    {
        UNIMPLEMENTED;  // If needed we could implement a version of RtlRecord that added/subtracted offset as needed
    }
protected:
    size32_t offset;
    IOutputMetaData *original;
    Owned<IOutputMetaData> serializedMeta;
};

//------------------------------------------------------------------------------------------------

class THORHELPER_API CSuffixedRowSerializer : implements IOutputRowSerializer, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    CSuffixedRowSerializer(size32_t _offset, IOutputRowSerializer *_original) : offset(_offset), original(_original)
    {
    }
    virtual void serialize(IRowSerializerTarget & out, const byte * self)
    {
        original->serialize(out, self+offset);
        out.put(offset, self);
    }

protected:
    size32_t offset; 
    Owned<IOutputRowSerializer> original;
};

class THORHELPER_API CSuffixedRowDeserializer : implements IOutputRowDeserializer, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    CSuffixedRowDeserializer(size32_t _offset, IOutputRowDeserializer *_original) : offset(_offset), original(_original)
    {
    }

    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in)
    {
        size32_t size = original->deserialize(rowBuilder, in);
        in.read(offset, rowBuilder.getSelf()+size);
        return size+offset;
    }
    
protected:
    size32_t offset; 
    Owned<IOutputRowDeserializer> original;
};

class THORHELPER_API CSuffixedRowPrefetcher : implements ISourceRowPrefetcher, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    CSuffixedRowPrefetcher(size32_t _offset, ISourceRowPrefetcher *_original) : offset(_offset), original(_original)
    {
    }

    virtual void readAhead(IRowDeserializerSource & in)
    {
        original->readAhead(in);
        in.skip(offset);
    }
    
protected:
    size32_t offset; 
    Owned<ISourceRowPrefetcher> original;
};

class THORHELPER_API CSuffixedOutputMeta : implements IOutputMetaData, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    CSuffixedOutputMeta(size32_t _offset, IOutputMetaData *_original) : original(_original)
    {
        offset = _offset;
        IOutputMetaData * originalSerialized = _original->querySerializedDiskMeta();
        if (originalSerialized != original)
            serializedMeta.setown(new CSuffixedOutputMeta(_offset, originalSerialized));
    }
    
    virtual size32_t getRecordSize(const void *rec) 
    {
        return original->getRecordSize(rec) + offset;
    }
    
    virtual size32_t getMinRecordSize() const
    {
        return original->getMinRecordSize() + offset;
    }

    virtual size32_t getFixedSize() const
    {
        size32_t ret = original->getFixedSize();
        if (ret)
            ret += offset;
        return ret;
    }

    virtual void toXML(const byte * self, IXmlWriter & out)  { original->toXML(self, out); }
    virtual unsigned getVersion() const { return original->getVersion(); }

    virtual unsigned getMetaFlags() { return original->getMetaFlags(); }
    virtual void destruct(byte * self) { original->destruct(self); }
    virtual IOutputRowSerializer * createDiskSerializer(ICodeContext * ctx, unsigned activityId)
    {
        return new CSuffixedRowSerializer(offset, original->createDiskSerializer(ctx, activityId));
    }
    virtual IOutputRowDeserializer * createDiskDeserializer(ICodeContext * ctx, unsigned activityId) 
    {
        return new CSuffixedRowDeserializer(offset, original->createDiskDeserializer(ctx, activityId));
    }
    virtual ISourceRowPrefetcher * createDiskPrefetcher(ICodeContext * ctx, unsigned activityId) 
    {
        return new CSuffixedRowPrefetcher(offset, original->createDiskPrefetcher(ctx, activityId));
    }
    virtual IOutputMetaData * querySerializedDiskMeta()
    {
        if (serializedMeta)
            return serializedMeta.get();
        return this;
    }
    virtual IOutputRowSerializer * createInternalSerializer(ICodeContext * ctx, unsigned activityId)
    {
        return new CSuffixedRowSerializer(offset, original->createInternalSerializer(ctx, activityId));
    }
    virtual IOutputRowDeserializer * createInternalDeserializer(ICodeContext * ctx, unsigned activityId)
    {
        return new CSuffixedRowDeserializer(offset, original->createInternalDeserializer(ctx, activityId));
    }
    virtual void walkIndirectMembers(const byte * self, IIndirectMemberVisitor & visitor)
    {
        original->walkIndirectMembers(self, visitor);
    }
    virtual IOutputMetaData * queryChildMeta(unsigned i)
    {
        return original->queryChildMeta(i);
    }
    virtual const RtlRecord &queryRecordAccessor(bool expand) const override { return original->queryRecordAccessor(expand); }

protected:
    size32_t offset;
    Linked<IOutputMetaData> original;
    Owned<IOutputMetaData> serializedMeta;
};

//------------------------------------------------------------------------------------------------


struct SmartStepExtra;
class THORHELPER_API CStreamMerger : public CInterface
{
public:
    CStreamMerger(bool pullConsumes);
    ~CStreamMerger();

    void cleanup();
    void init(ICompare * _compare, bool _dedup, IRangeCompare * _rangeCompare);
    void initInputs(unsigned _numInputs);
    inline void done() { cleanup(); }
    const void * nextRow();
    const void * nextRowGE(const void * seek, unsigned numFields, bool & wasCompleteMatch, const SmartStepExtra & stepExtra);
    void primeRows(const void * * rows);
    const void * queryNextRow();                        // look ahead at the next item
    unsigned queryNextInput();                  // which input will the next item come from?
    inline void reset() { clearPending(); }
    void skipRow();
    
protected:
    //The following function must fill in pending[i] AND pendingMatches[i]
    virtual bool pullInput(unsigned i, const void * seek, unsigned numFields, const SmartStepExtra * stepExtra) = 0;
    virtual void skipInput(unsigned i);
    virtual void consumeInput(unsigned i);
    virtual void releaseRow(const void * row) = 0;

    bool ensureNext();
    void permute();

private:
    const void * consumeTop();
    bool ensureNext(const void * seek, unsigned numFields, bool & wasCompleteMatch, const SmartStepExtra * stepExtra);
    void clearPending();
    void fillheap(const void * seek, unsigned numFields, const SmartStepExtra * stepExtra);
    void permute(const void * seek, unsigned numFields, const SmartStepExtra * stepExtra);
    bool promote(unsigned p);
    bool siftDown(unsigned p) { return heap_push_down(p, activeInputs, mergeheap, pending, compare); }
    void siftDownDedupTop(const void * seek, unsigned numFields, const SmartStepExtra * stepExtra);

protected:
    unsigned *mergeheap;
    unsigned activeInputs;
    unsigned numInputs;
    const void **pending;
    bool *pendingMatches;
    ICompare *compare;
    IRangeCompare * rangeCompare;
    bool first;
    bool dedup;
    bool pullConsumes;              // true if pull consumes from input, and takes ownership, false if pullInput just looks ahead
};


class THORHELPER_API CRawRowSerializer : implements IRowSerializerTarget
{
public:
    CRawRowSerializer(size32_t _len, byte *_buf)
    : maxSize(_len), buffer(_buf)
    {
        pos = 0;
        nesting = 0;
    }

    virtual void put(size32_t len, const void * ptr)
    {
        assertex(pos+len <= maxSize);
        memcpy(buffer+pos, ptr, len);
        pos += len;
    }
    virtual size32_t beginNested(size32_t count)
    {
        nesting++;
        size32_t ret = pos;
        size32_t placeholder = 0;
        put(sizeof(placeholder), &placeholder);
        return ret;
    }
    virtual void endNested(size32_t position)
    {
        * (size32_t *) (buffer+position) = pos - (position + sizeof(size32_t));
        nesting--;
    }

    inline size32_t size() const { return pos; }
    
protected:
    byte *buffer;
    unsigned maxSize;
    unsigned pos;
    unsigned nesting;
};

class THORHELPER_API CThorDemoRowSerializer : implements IRowSerializerTarget
{
public:
    CThorDemoRowSerializer(MemoryBuffer & _buffer);

    virtual void put(size32_t len, const void * ptr);
    virtual size32_t beginNested(size32_t count);
    virtual void endNested(size32_t position);

protected:
    MemoryBuffer & buffer;
    unsigned nesting;
};


class THORHELPER_API CSimpleFixedRowSerializer : implements IOutputRowSerializer, public CInterface
{
public:
    CSimpleFixedRowSerializer(size32_t _fixedSize) : fixedSize(_fixedSize) {}
    IMPLEMENT_IINTERFACE

    virtual void serialize(IRowSerializerTarget & out, const byte * self)
    {
        out.put(fixedSize, self);
    }

protected:
    size32_t fixedSize;
};

class THORHELPER_API CSimpleFixedRowDeserializer : implements IOutputRowDeserializer, public CInterface
{
public:
    CSimpleFixedRowDeserializer(size32_t _fixedSize) : fixedSize(_fixedSize) {}
    IMPLEMENT_IINTERFACE

    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in)
    {
        in.read(fixedSize, rowBuilder.getSelf());
        return fixedSize;
    }
        
protected:
    size32_t fixedSize;
};

class THORHELPER_API CSimpleVariableRowSerializer : implements IOutputRowSerializer, public CInterface
{
public:
    CSimpleVariableRowSerializer(const CachedOutputMetaData * _meta) : meta(_meta) {}
    IMPLEMENT_IINTERFACE

    virtual void serialize(IRowSerializerTarget & out, const byte * self)
    {
        out.put(meta->getRecordSize(self), self);
    }

protected:
    const CachedOutputMetaData * meta;        // assume lifetime is shorter than this meta
};

//This should never be created in practice - need to use a streamer. Pseudocode below for illustration purposes only
#if 0
class THORHELPER_API CSimpleVariableRowDeserializer : public CInterface, implements IOutputRowDeserializer
{
public:
    CSimpleVariableRowDeserializer(CachedOutputMetaData * _meta) : meta(_meta) {}
    IMPLEMENT_IINTERFACE

    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in)
    {
        const byte * next = in.peek(meta->getMaxSize()); // This is wrong! We don't know the maximum size...
        size32_t size = meta->getRecordSize(next);
        in.read(size, rowBuilder.ensureCapacity(size, NULL));
        return size;
    }
        
protected:
    CachedOutputMetaData * meta;        // assume lifetime is shorter than this meta
};
#endif



class NullDiskCallback : public IThorDiskCallback, extends CInterface
{
    IMPLEMENT_IINTERFACE

    virtual unsigned __int64 getFilePosition(const void * row) { return 0; }
    virtual unsigned __int64 getLocalFilePosition(const void * row) { return 0; }
    virtual const char * queryLogicalFilename(const void * row) { return NULL; }
};

extern THORHELPER_API size32_t cloneRow(ARowBuilder & rowBuilder, const void * row, IOutputMetaData * meta);

//The CThorContiguousRowBuffer is the source for a readAhead call to ensure the entire row
//is in a contiguous block of memory.  The read() and skip() functions must be implemented
class THORHELPER_API CThorContiguousRowBuffer : implements IRowDeserializerSource
{
public:
    CThorContiguousRowBuffer(ISerialStream * _in);

    inline void setStream(ISerialStream *_in) { in.set(_in); maxOffset = 0; readOffset = 0; }

    virtual const byte * peek(size32_t maxSize);
    virtual offset_t beginNested();
    virtual bool finishedNested(offset_t & len);

    virtual size32_t read(size32_t len, void * ptr);
    virtual size32_t readSize();
    virtual size32_t readPackedInt(void * ptr);
    virtual size32_t readUtf8(ARowBuilder & target, size32_t offset, size32_t fixedSize, size32_t len);
    virtual size32_t readVStr(ARowBuilder & target, size32_t offset, size32_t fixedSize);
    virtual size32_t readVUni(ARowBuilder & target, size32_t offset, size32_t fixedSize);

    //These shouldn't really be called since this class is meant to be used for a deserialize.
    //If we allowed padding/alignment fields in the input then the first function would make sense.
    virtual void skip(size32_t size);
    virtual void skipPackedInt();
    virtual void skipUtf8(size32_t len);
    virtual void skipVStr();
    virtual void skipVUni();

    inline bool eos()
    {
        return in->eos();
    }

    inline offset_t tell()
    {
        return in->tell();
    }

    inline void clearStream()
    {
        in.clear();
        maxOffset = 0;
        readOffset = 0;
    }

    inline const byte * queryRow() { return buffer; }
    inline size32_t queryRowSize() { return readOffset; }
    inline void finishedRow()
    {
        if (readOffset)
            in->skip(readOffset);
        maxOffset = 0;
        readOffset = 0;
    }


protected:
    size32_t sizePackedInt();
    size32_t sizeUtf8(size32_t len);
    size32_t sizeVStr();
    size32_t sizeVUni();
    void reportReadFail();

private:
    inline void doPeek(size32_t maxSize)
    {
        buffer = static_cast<const byte *>(in->peek(maxSize, maxOffset));
    }

    void doRead(size32_t len, void * ptr);

    inline void ensureAccessible(size32_t required)
    {
        if (required > maxOffset)
        {
            doPeek(required);
            assertex(required <= maxOffset);
        }
    }

protected:
    Linked<ISerialStream> in;
    const byte * buffer;
    size32_t maxOffset;
    size32_t readOffset;
};


//=====================================================================================================

class ChildRowLinkerWalker : implements IIndirectMemberVisitor
{
public:
    virtual void visitRowset(size32_t count, byte * * rows)
    {
        rtlLinkRowset(rows);
    }
    virtual void visitRow(const byte * row)
    {
        rtlLinkRow(row);
    }
};

#endif // THORCOMMON_IPP
