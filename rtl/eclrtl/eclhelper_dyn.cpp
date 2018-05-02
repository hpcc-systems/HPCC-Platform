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
#include "jptree.hpp"
#include "eclrtl.hpp"
#include "eclhelper.hpp"
#include "rtlds_imp.hpp"
#include "eclhelper_base.hpp"
#include "eclhelper_dyn.hpp"

#include "rtlfield.hpp"
#include "rtlrecord.hpp"
#include "rtldynfield.hpp"
#include "rtlkey.hpp"

//---------------------------------------------------------------------------------------------------------------------

/**
* class CDeserializedOutputMetaData
*
* An implementation of IOutputMetaData for use with serialized rtlTypeInfo information
*
*/

class CDeserializedOutputMetaData : public COutputMetaData
{
public:
    CDeserializedOutputMetaData(MemoryBuffer &binInfo, bool isGrouped, IThorIndexCallback *callback);
    CDeserializedOutputMetaData(IPropertyTree &jsonInfo, bool isGrouped, IThorIndexCallback *callback);
    CDeserializedOutputMetaData(const char *json, bool isGrouped, IThorIndexCallback *callback);

    virtual const RtlTypeInfo * queryTypeInfo() const override { return typeInfo; }
    virtual unsigned getMetaFlags() override { return flags; }
protected:
    Owned<IRtlFieldTypeDeserializer> deserializer;
    const RtlTypeInfo *typeInfo = nullptr;
    unsigned flags = MDFhasserialize|MDFhasxml;

};

CDeserializedOutputMetaData::CDeserializedOutputMetaData(MemoryBuffer &binInfo, bool isGrouped, IThorIndexCallback *callback)
{
    deserializer.setown(createRtlFieldTypeDeserializer(callback));
    typeInfo = deserializer->deserialize(binInfo);
    if (isGrouped)
        flags |= MDFgrouped;
}

CDeserializedOutputMetaData::CDeserializedOutputMetaData(IPropertyTree &jsonInfo, bool isGrouped, IThorIndexCallback *callback)
{
    deserializer.setown(createRtlFieldTypeDeserializer(callback));
    typeInfo = deserializer->deserialize(jsonInfo);
    if (isGrouped)
        flags |= MDFgrouped;
}

CDeserializedOutputMetaData::CDeserializedOutputMetaData(const char *json, bool isGrouped, IThorIndexCallback *callback)
{
    deserializer.setown(createRtlFieldTypeDeserializer(callback));
    typeInfo = deserializer->deserialize(json);
    if (isGrouped)
        flags |= MDFgrouped;
}

extern ECLRTL_API IOutputMetaData *createTypeInfoOutputMetaData(MemoryBuffer &binInfo, bool isGrouped, IThorIndexCallback *callback)
{
    return new CDeserializedOutputMetaData(binInfo, isGrouped, callback);
}

extern ECLRTL_API IOutputMetaData *createTypeInfoOutputMetaData(IPropertyTree &jsonInfo, bool isGrouped, IThorIndexCallback *callback)
{
    return new CDeserializedOutputMetaData(jsonInfo, isGrouped, callback);
}

extern ECLRTL_API IOutputMetaData *createTypeInfoOutputMetaData(const char *json, bool isGrouped, IThorIndexCallback *callback)
{
    return new CDeserializedOutputMetaData(json, isGrouped, callback);
}
//---------------------------------------------------------------------------------------------------------------------

static int compareOffsets(const unsigned *a, const unsigned *b)
{
    if (*a < *b)
        return -1;
    else if (*a==*b)
        return 0;
    else
        return 1;
}

class ECLRTL_API CDynamicDiskReadArg : public CThorDiskReadArg
{
public:
    CDynamicDiskReadArg(const char *_fileName, IOutputMetaData *_in, IOutputMetaData *_projected, IOutputMetaData *_out, unsigned __int64 _chooseN, unsigned __int64 _skipN, unsigned __int64 _rowLimit)
        : fileName(_fileName), in(_in), projected(_projected), out(_out), chooseN(_chooseN), skipN(_skipN), rowLimit(_rowLimit)
    {
        translator.setown(createRecordTranslator(out->queryRecordAccessor(true), projected->queryRecordAccessor(true)));
    }
    virtual bool needTransform() override
    {
        return translator->needsTranslate();
    }
    virtual unsigned getFlags() override
    {
        return flags;
    }
    virtual void createSegmentMonitors(IIndexReadContext *irc) override
    {
        filters.createSegmentMonitors(irc);
    }

    virtual IOutputMetaData * queryOutputMeta() override
    {
        return out;
    }
    virtual const char * getFileName() override final
    {
        return fileName;
    }
    virtual IOutputMetaData * queryDiskRecordSize() override final
    {
        return in;
    }
    virtual IOutputMetaData * queryProjectedDiskRecordSize() override final
    {
        return projected;
    }
    virtual unsigned getDiskFormatCrc() override
    {
        return 0;  // engines should treat 0 as 'ignore'
    }
    virtual unsigned getProjectedFormatCrc() override
    {
        return 0;  // engines should treat 0 as 'ignore'
    }
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * src) override
    {
        return translator->translate(rowBuilder, fieldCallback, (const byte *) src);
    }
    virtual unsigned __int64 getChooseNLimit() { return chooseN; }
    virtual unsigned __int64 getRowLimit() { return rowLimit; }
    void addFilter(const char *filter)
    {
        filters.addFilter(in->queryRecordAccessor(true), filter);
        flags |= TDRkeyed;
    }
private:
    StringAttr fileName;
    UnexpectedVirtualFieldCallback fieldCallback;
    unsigned flags = 0;
    Owned<IOutputMetaData> in;
    Owned<IOutputMetaData> projected;
    Owned<IOutputMetaData> out;
    Owned<const IDynamicTransform> translator;
    RowFilter filters;
    unsigned __int64 chooseN = I64C(0x7fffffffffffffff); // constant(s) should be commoned up somewhere
    unsigned __int64 skipN = 0;
    unsigned __int64 rowLimit = (unsigned __int64) -1;
};

class LegacyFilterSet
{
public:
    LegacyFilterSet(const RtlRecord &_inrec) : inrec(_inrec)
    {

    }
    void addFilter(const char *filter)
    {
        // Format of a filter is:
        // field[..n]: valuestring
        // value string format specifies ranges using a comma-separated list of ranges.
        // Each range is specified as paren lower, upper paren, where the paren is either ( or [ depending
        // on whether the specified bound is inclusive or exclusive.
        // If only one bound is specified then it is used for both upper and lower bound (only meaningful with [] )
        //
        // ( A means values > A - exclusive
        // [ means values >= A - inclusive
        // A ) means values < A - exclusive
        // A ] means values <= A - inclusive
        // For example:
        // [A] matches just A
        // (,A),(A,) matches all but A
        // (A] of [A) are both empty ranges
        // [A,B) means A*
        // Values use the ECL syntax for constants. String constants are always utf8. Binary use d'xx' format (hexpairs)
        // Note that binary serialization format is different

        assertex(filter);
        const char *epos = strpbrk(filter,"=~");
        if (!epos)
            throw MakeStringException(0, "Invalid filter string: expected = or ~ after fieldname");
        StringBuffer fieldName(epos-filter, filter);

        unsigned fieldNum;
        if (isdigit(fieldName[0]))
            fieldNum = atoi(fieldName);
        else
            fieldNum = inrec.getFieldNum(fieldName);
        if (fieldNum == (unsigned) -1)
            throw MakeStringException(0, "Invalid filter string: field '%s' not recognized", fieldName.str());
        unsigned numOffsets = inrec.getNumVarFields() + 1;
        unsigned fieldOffset = inrec.getFixedOffset(fieldNum);
        unsigned fieldSize = inrec.getFixedOffset(fieldNum+1) - fieldOffset;
        const RtlTypeInfo *fieldType = inrec.queryType(fieldNum);
        filter = epos+1;
        if (*filter=='~')
        {
            UNIMPLEMENTED;  // use a regex?
        }
        else
        {
            MemoryBuffer lobuffer;
            MemoryBuffer hibuffer;
            Owned<IStringSet> filterSet = createStringSet(fieldSize);
            deserializeSet(*filterSet, inrec.getMinRecordSize(), fieldType, filter);
            while (filters.length()<=fieldNum)
            {
                unsigned dummyOffset = inrec.getFixedOffset(filters.length());
                filterOffsets.append(dummyOffset);
                filterSizes.append(inrec.getFixedOffset(filters.length()+1) - dummyOffset);
                filters.append(nullptr);
            }
            IStringSet *prev = filters.item(fieldNum);
            if (prev)
                filterSet.setown(prev->unionSet(filterSet)); // Debatable - would intersect be more appropriate?
            filters.replace(filterSet.getClear(), fieldNum);
            filterOffsets.replace(fieldOffset, fieldNum); // MORE - probably refactor this in  a bit
            filterSizes.replace(fieldSize, fieldNum); // MORE - probably refactor this in  a bit
        }
    }
    void createSegmentMonitors(IIndexReadContext *irc)
    {
        ForEachItemIn(idx, filters)
        {
            IStringSet *filter = filters.item(idx);
            if (filter)
                irc->append(createKeySegmentMonitor(false, LINK(filter), idx, filterOffsets.item(idx), filter->getSize()));
        }
    }
    void createSegmentMonitorsWithWild(IIndexReadContext *irc, unsigned keySize)
    {
        unsigned lastOffset = 0;
        ForEachItemIn(idx, filters)
        {
            IStringSet *filter = filters.item(idx);
            unsigned offset =  filterOffsets.item(idx);
            unsigned size =  filterSizes.item(idx);
            if (filter)
            {
                assertex(size == filter->getSize());
                irc->append(createKeySegmentMonitor(false, LINK(filter), idx, offset, size));
            }
            else
                irc->append(createWildKeySegmentMonitor(idx, offset, size)); // MORE - move this logic to irc::append ?
        }
        // MORE - trailing wild needs adding
        /*
        if (keySize > lastOffset)
            irc->append(createWildKeySegmentMonitor(lastOffset, keySize-lastOffset));
            */
    }
protected:
    IPointerArrayOf<IStringSet> filters;
    UnsignedArray filterOffsets;
    UnsignedArray filterSizes;
    const RtlRecord &inrec;
};

class ECLRTL_API CDynamicIndexReadArg : public CThorIndexReadArg, implements IDynamicIndexReadArg
{
public:
    CDynamicIndexReadArg(const char *_fileName, IOutputMetaData *_in, IOutputMetaData *_projected, IOutputMetaData *_out, unsigned __int64 _chooseN, unsigned __int64 _skipN, unsigned __int64 _rowLimit, unsigned _flags)
        : fileName(_fileName), in(_in), projected(_projected), out(_out), chooseN(_chooseN), skipN(_skipN), rowLimit(_rowLimit), flags(_flags), filters(in->queryRecordAccessor(true))
    {
        translator.setown(createRecordTranslator(out->queryRecordAccessor(true), projected->queryRecordAccessor(true)));
    }
    virtual bool needTransform() override
    {
        return translator->needsTranslate();
    }
    virtual unsigned getFlags() override
    {
        return flags;
    }
    virtual void createSegmentMonitors(IIndexReadContext *irc) override
    {
        filters.createSegmentMonitorsWithWild(irc, 0); // Should be the total keyed size, but that's not available. And probably should not really be needed. Why should I create trailing wildsegs?
    }

    virtual IOutputMetaData * queryOutputMeta() override
    {
        return out;
    }
    virtual const char * getFileName() override final
    {
        return fileName;
    }
    virtual IOutputMetaData * queryDiskRecordSize() override final
    {
        return in;
    }
    virtual IOutputMetaData * queryProjectedDiskRecordSize() override final
    {
        return projected;
    }
    virtual unsigned getFormatCrc() override
    {
        return 0;  // engines should treat 0 as 'ignore'
    }
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * src) override
    {
        return translator->translate(rowBuilder, fieldCallback, (const byte *) src);
    }
    virtual unsigned __int64 getChooseNLimit() { return chooseN; }
    virtual unsigned __int64 getRowLimit() { return rowLimit; }
    virtual void addFilter(const char *filter) override
    {
        filters.addFilter(filter);
    }

private:
    UnexpectedVirtualFieldCallback fieldCallback;
    StringAttr fileName;
    unsigned flags = 0;
    Owned<IOutputMetaData> in;
    Owned<IOutputMetaData> projected;
    Owned<IOutputMetaData> out;
    Owned<const IDynamicTransform> translator;
    LegacyFilterSet filters;
    unsigned __int64 chooseN = I64C(0x7fffffffffffffff); // constant(s) should be commoned up somewhere
    unsigned __int64 skipN = 0;
    unsigned __int64 rowLimit = (unsigned __int64) -1;
};

class ECLRTL_API CDynamicWorkUnitWriteArg : public CThorWorkUnitWriteArg
{
public:
    CDynamicWorkUnitWriteArg(IOutputMetaData *_in) : in(_in)
    {
    }
    virtual int getSequence() override final { return 0; }
    virtual IOutputMetaData * queryOutputMeta() override final { return in; }
private:
    Owned<IOutputMetaData> in;
};

static IOutputMetaData *loadTypeInfo(IPropertyTree &xgmml, const char *key)
{
    StringBuffer xpath;
    MemoryBuffer binInfo;
    xgmml.getPropBin(xpath.setf("att[@name='%s_binary']/value", key), binInfo);
    assertex(binInfo.length());
    bool grouped = xgmml.getPropBool(xpath.setf("att[@name='%s_binary']/value", key), false);

    return new CDeserializedOutputMetaData(binInfo, grouped, nullptr);
}

extern ECLRTL_API IHThorDiskReadArg *createDiskReadArg(IPropertyTree &xgmml)
{
    Owned <IOutputMetaData> in = loadTypeInfo(xgmml, "input");
    Owned <IOutputMetaData> out = loadTypeInfo(xgmml, "output");
    const char *fileName = xgmml.queryProp("att[@name=\"_fileName\"]/@value");
    unsigned __int64 chooseN = xgmml.getPropInt64("att[@name=\"chooseN\"]/@value", -1);
    unsigned __int64 skipN = xgmml.getPropInt64("att[@name=\"skipN\"]/@value", -1);
    unsigned __int64 rowLimit = xgmml.getPropInt64("att[@name=\"rowLimit\"]/@value", -1);
    Owned<CDynamicDiskReadArg> ret = new CDynamicDiskReadArg(fileName, in.getClear(), LINK(out), LINK(out), chooseN, skipN, rowLimit);
    Owned<IPropertyTreeIterator> filters = xgmml.getElements("att[@name=\"keyfilter\"]");
    ForEach(*filters)
        ret->addFilter(filters->query().queryProp("@value"));
    return ret.getClear();
}

extern ECLRTL_API IHThorDiskReadArg *createDiskReadArg(const char *fileName, IOutputMetaData *in, IOutputMetaData *projected, IOutputMetaData *out, unsigned __int64 chooseN, unsigned __int64 skipN, unsigned __int64 rowLimit)
{
    return new CDynamicDiskReadArg(fileName, in, projected, out, chooseN, skipN, rowLimit);
}

extern ECLRTL_API IHThorIndexReadArg *createIndexReadArg(const char *fileName, IOutputMetaData *in, IOutputMetaData *projected, IOutputMetaData *out, unsigned __int64 chooseN, unsigned __int64 skipN, unsigned __int64 rowLimit, unsigned flags)
{
    return new CDynamicIndexReadArg(fileName, in, projected, out, chooseN, skipN, rowLimit, flags);
}


extern ECLRTL_API IHThorArg *createWorkunitWriteArg(IPropertyTree &xgmml)
{
    Owned <IOutputMetaData> in = loadTypeInfo(xgmml, "input");
    return new CDynamicWorkUnitWriteArg(in.getClear());
}

struct ECLRTL_API DynamicEclProcess : public EclProcess {
    virtual unsigned getActivityVersion() const override { return ACTIVITY_INTERFACE_VERSION; }
    virtual int perform(IGlobalCodeContext * gctx, unsigned wfid) override {
        ICodeContext * ctx;
        ctx = gctx->queryCodeContext();
        ctx->executeGraph("graph1",false,0,NULL);
        return 1U;
    }
};

extern ECLRTL_API IEclProcess* createDynamicEclProcess()
{
    return new DynamicEclProcess;
}

