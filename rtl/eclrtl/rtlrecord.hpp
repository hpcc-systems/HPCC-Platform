/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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

#ifndef rtlrecord_hpp
#define rtlrecord_hpp

#if defined(_WIN32)
#include <malloc.h>
#else
#include <alloca.h>
#endif

#include "eclrtl_imp.hpp"
#include "rtlds_imp.hpp"
#include "rtlfield.hpp"

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

//---------------------------------------------------------------------------

class CXmlToRowTransformer : implements IXmlToRowTransformer, public CGlobalHelperClass
{
public:
    inline CXmlToRowTransformer(unsigned _activityId) : CGlobalHelperClass(_activityId) {}
    RTLIMPLEMENT_IINTERFACE

};

//---------------------------------------------------------------------------
// Record size handling.

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


//These classes provides a relatively efficient way to access fields within a variable length record structure.
// Probably convert to an interface with various concrete implementations for varying degrees of complexity
//
// Complications:
// * ifblocks
// * nested records.
// * alien data types
//

class FieldNameToFieldNumMap;

class ECLRTL_API RtlRecord
{
public:
    friend class RtlRow;
    RtlRecord(const RtlRecordTypeInfo & fields, bool expandFields);
    RtlRecord(const RtlFieldInfo * const * fields, bool expandFields);
    ~RtlRecord();

    void calcRowOffsets(size_t * variableOffsets, const void * _row, unsigned numFieldsUsed = (unsigned) -1) const;

    virtual size32_t getFixedSize() const
    {
        return numVarFields ? 0 : fixedOffsets[numFields];
    }

    size_t getOffset(size_t * variableOffsets, unsigned field) const
    {
        return fixedOffsets[field] + variableOffsets[whichVariableOffset[field]];
    }


    size_t getRecordSize(size_t * variableOffsets) const
    {
        return getOffset(variableOffsets, numFields);
    }

    virtual size32_t getMinRecordSize() const;

    inline unsigned getNumFields() const { return numFields; }
    inline unsigned getNumVarFields() const { return numVarFields; }
    inline const RtlFieldInfo * queryField(unsigned field) const { return fields[field]; }
    inline const RtlTypeInfo * queryType(unsigned field) const { return fields[field]->type; }
    const char * queryName(unsigned field) const;
    unsigned getFieldNum(const char *fieldName) const;
    const RtlRecord *queryNested(unsigned field) const;
protected:
    size_t * fixedOffsets;         // fixed portion of the field offsets + 1 extra
    unsigned * whichVariableOffset;// which variable offset should be added to the fixed
    unsigned * variableFieldIds;   // map variable field to real field id.
    unsigned * tableIds;           // map nested table id to real field id.
    unsigned numFields;
    unsigned numVarFields;
    unsigned numTables;
    const RtlFieldInfo * const * fields;
    const RtlFieldInfo * const * originalFields;
    const RtlRecord **nestedTables;
    const char **names;
    mutable const FieldNameToFieldNumMap *nameMap;
};

class ECLRTL_API RtlRow
{
public:
    RtlRow(const RtlRecord & _info, const void * optRow, unsigned numOffsets, size_t * _variableOffsets);

    __int64 getInt(unsigned field) const;
    double getReal(unsigned field) const;
    void getString(size32_t & resultLen, char * & result, unsigned field) const;
    void getUtf8(size32_t & resultLen, char * & result, unsigned field) const;

    size_t getOffset(unsigned field) const
    {
        return info.getOffset(variableOffsets, field);
    }

    size_t getSize(unsigned field) const
    {
        return info.getOffset(variableOffsets, field+1) - info.getOffset(variableOffsets, field);
    }

    size_t getRecordSize() const
    {
        return info.getRecordSize(variableOffsets);
    }

    void setRow(const void * _row);
    void setRow(const void * _row, unsigned _numFields);

    const void *queryRow() const
    {
        return row;
    }
protected:
    const RtlRecord & info;
    const void * row;
    size_t * variableOffsets;       // [0 + 1 entry for each variable size field ]
};

struct ECLRTL_API RtlDynRow : public RtlRow
{
public:
    RtlDynRow(const RtlRecord & _info, const void * optRow = NULL);
    ~RtlDynRow();
};

//The following template class is used from the generated code to avoid allocating the offset array
template <unsigned NUM_VARIABLE_FIELDS>
struct ECLRTL_API RtlStaticRow : RtlRow
{
public:
    RtlStaticRow(const RtlRecord & _info, const void * optRow = NULL) : RtlRow(_info, optRow, NUM_VARIABLE_FIELDS+1, off) {}
public:
    size_t off[NUM_VARIABLE_FIELDS+1];
};

class ECLRTL_API RtlRecordSize : public IRecordSize, public RtlCInterface
{
public:
    RtlRecordSize(const RtlRecordTypeInfo & fields) : offsetInformation(fields, true) {}
    RTLIMPLEMENT_IINTERFACE

    virtual size32_t getRecordSize(const void * row)
    {
        //Allocate a temporary offset array on the stack to avoid runtime overhead.
        unsigned numOffsets = offsetInformation.getNumVarFields() + 1;
        size_t * variableOffsets = (size_t *)alloca(numOffsets * sizeof(size_t));
        RtlRow offsetCalculator(offsetInformation, row, numOffsets, variableOffsets);
        return offsetCalculator.getRecordSize();
    }

    virtual size32_t getFixedSize() const
    {
        return offsetInformation.getFixedSize();
    }
    // returns 0 for variable row size
    virtual size32_t getMinRecordSize() const
    {
        return offsetInformation.getMinRecordSize();
    }

protected:
    RtlRecord offsetInformation;
};

class CSourceRowPrefetcher;

class ECLRTL_API COutputMetaData : implements IOutputMetaData, public RtlCInterface
{
public:
    RTLIMPLEMENT_IINTERFACE
    COutputMetaData();
    ~COutputMetaData();

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
    virtual IOutputRowSerializer * createDiskSerializer(ICodeContext * ctx, unsigned activityId);
    virtual ISourceRowPrefetcher * createDiskPrefetcher(ICodeContext * ctx, unsigned activityId);
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

    virtual const RtlRecord &queryRecordAccessor(bool expand) const;

protected:
    //This is the prefetch function that is actually generated by the code generator
    virtual CSourceRowPrefetcher * doCreateDiskPrefetcher(unsigned activityId) { return NULL; }

    ISourceRowPrefetcher * defaultCreateDiskPrefetcher(ICodeContext * ctx, unsigned activityId);
    mutable RtlRecord *recordAccessor[2];
};

class ECLRTL_API CFixedOutputMetaData : public COutputMetaData
{
public:
    CFixedOutputMetaData(size32_t _fixedSize)               { fixedSize = _fixedSize; }

    virtual size32_t getRecordSize(const void *rec)         { return fixedSize; }
    virtual size32_t getMinRecordSize() const               { return fixedSize; }
    virtual size32_t getFixedSize() const                   { return fixedSize; }

    virtual IOutputRowSerializer * createDiskSerializer(ICodeContext * ctx, unsigned activityId);
    virtual IOutputRowDeserializer * createDiskDeserializer(ICodeContext * ctx, unsigned activityId);
    virtual ISourceRowPrefetcher * createDiskPrefetcher(ICodeContext * ctx, unsigned activityId);

protected:
    size32_t fixedSize;
};

class ECLRTL_API CVariableOutputMetaData : public COutputMetaData
{
public:
    CVariableOutputMetaData(size32_t _minSize) : minSize(_minSize) { }

    virtual size32_t getMinRecordSize() const               { return minSize; }
    virtual size32_t getFixedSize() const                   { return 0; }  // is variable

protected:
    size32_t minSize;
};

class ECLRTL_API CActionOutputMetaData : public COutputMetaData
{
public:
    virtual size32_t getRecordSize(const void *)            { return 0; }
    virtual size32_t getMinRecordSize() const               { return 0; }
    virtual size32_t getFixedSize() const                   { return 0; }  // is pseudo-variable
    virtual void toXML(const byte * self, IXmlWriter & out) { }
    virtual IOutputRowSerializer * createDiskSerializer(ICodeContext * ctx, unsigned activityId);
    virtual IOutputRowDeserializer * createDiskDeserializer(ICodeContext * ctx, unsigned activityId);
    virtual ISourceRowPrefetcher * createDiskPrefetcher(ICodeContext * ctx, unsigned activityId);
};


#endif
