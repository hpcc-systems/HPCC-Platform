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
    CDeserializedOutputMetaData(MemoryBuffer &binInfo);
    CDeserializedOutputMetaData(IPropertyTree &jsonInfo);
    CDeserializedOutputMetaData(const char *json);

    virtual const RtlTypeInfo * queryTypeInfo() const override { return typeInfo; }
protected:
    Owned<IRtlFieldTypeDeserializer> deserializer;
    const RtlTypeInfo *typeInfo = nullptr;
};


CDeserializedOutputMetaData::CDeserializedOutputMetaData(MemoryBuffer &binInfo)
{
    deserializer.setown(createRtlFieldTypeDeserializer());
    typeInfo = deserializer->deserialize(binInfo);
}

CDeserializedOutputMetaData::CDeserializedOutputMetaData(IPropertyTree &jsonInfo)
{
    deserializer.setown(createRtlFieldTypeDeserializer());
    typeInfo = deserializer->deserialize(jsonInfo);
}

CDeserializedOutputMetaData::CDeserializedOutputMetaData(const char *json)
{
    deserializer.setown(createRtlFieldTypeDeserializer());
    typeInfo = deserializer->deserialize(json);
}

//---------------------------------------------------------------------------------------------------------------------

class ECLRTL_API CDynamicDiskReadArg : public CThorDiskReadArg
{
public:
    CDynamicDiskReadArg(const char *_fileName, IOutputMetaData *_in, IOutputMetaData *_out, unsigned __int64 _chooseN, unsigned __int64 _skipN, unsigned __int64 _rowLimit)
        : fileName(_fileName), in(_in), out(_out), chooseN(_chooseN), skipN(_skipN), rowLimit(_rowLimit)
    {
        inrec = &in->queryRecordAccessor(true);
        numOffsets = inrec->getNumVarFields() + 1;
        translator.setown(createRecordTranslator(queryOutputMeta()->queryRecordAccessor(true), *inrec));
    }
    virtual bool needTransform() override
    {
        return true;
        //return translator->needsTranslate(); might be more appropriate?
    }
    virtual unsigned getFlags() override
    {
        return flags;
    }
    virtual void createSegmentMonitors(IIndexReadContext *irc) override
    {
        ForEachItemIn(idx, filters)
        {
            IStringSet &filter = filters.item(idx);
            irc->append(createKeySegmentMonitor(false, LINK(&filter), filterOffsets.item(idx), filter.getSize()));
        }
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
    virtual unsigned getFormatCrc() override
    {
        return 0;  // engines should treat 0 as 'ignore'
    }
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * src) override
    {
        return translator->translate(rowBuilder, (const byte *) src);
    }
    virtual unsigned __int64 getChooseNLimit() { return chooseN; }
    virtual unsigned __int64 getRowLimit() { return rowLimit; }

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
        const char *epos = strchr(filter,'=');
        assertex(epos);
        StringBuffer fieldName(epos-filter, filter);
        unsigned fieldNum = inrec->getFieldNum(fieldName);
        assertex(fieldNum != (unsigned) -1);
        size_t * variableOffsets = (size_t *)alloca(numOffsets * sizeof(size_t));
        RtlRow offsetCalculator(*inrec, nullptr, numOffsets, variableOffsets);
        unsigned fieldOffset = offsetCalculator.getOffset(fieldNum);
        unsigned fieldSize = offsetCalculator.getSize(fieldNum);
        const RtlTypeInfo *fieldType = inrec->queryType(fieldNum);
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
            deserializeSet(*filterSet, inrec->getMinRecordSize(), fieldType, filter);
            filters.append(*filterSet.getClear());
            filterOffsets.append(fieldOffset);
            flags |= TDRkeyed;
        }
    }
private:
    StringAttr fileName;
    unsigned numOffsets = 0;
    unsigned flags = 0;
    Owned<IRtlFieldTypeDeserializer> indeserializer;   // Owns the resulting ITypeInfo structures, so needs to be kept around
    Owned<IRtlFieldTypeDeserializer> outdeserializer;  // Owns the resulting ITypeInfo structures, so needs to be kept around
    Owned<IOutputMetaData> in;
    Owned<IOutputMetaData> out;
    IArrayOf<IStringSet> filters;
    UnsignedArray filterOffsets;
    const RtlRecord *inrec = nullptr;
    Owned<const IDynamicTransform> translator;
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
    return new CDeserializedOutputMetaData(binInfo);
}

extern ECLRTL_API IHThorDiskReadArg *createDiskReadArg(IPropertyTree &xgmml)
{
    Owned <IOutputMetaData> in = loadTypeInfo(xgmml, "input");
    Owned <IOutputMetaData> out = loadTypeInfo(xgmml, "output");
    const char *fileName = xgmml.queryProp("att[@name=\"_fileName\"]/@value");
    unsigned __int64 chooseN = xgmml.getPropInt64("att[@name=\"chooseN\"]/@value", -1);
    unsigned __int64 skipN = xgmml.getPropInt64("att[@name=\"skipN\"]/@value", -1);
    unsigned __int64 rowLimit = xgmml.getPropInt64("att[@name=\"rowLimit\"]/@value", -1);
    Owned<CDynamicDiskReadArg> ret = new CDynamicDiskReadArg(fileName, in.getClear(), out.getClear(), chooseN, skipN, rowLimit);
    Owned<IPropertyTreeIterator> filters = xgmml.getElements("att[@name=\"keyfilter\"]");
    ForEach(*filters)
        ret->addFilter(filters->query().queryProp("@value"));
    return ret.getClear();
}

extern ECLRTL_API IHThorDiskReadArg *createDiskReadArg(const char *fileName, IOutputMetaData *in, IOutputMetaData *out, unsigned __int64 chooseN, unsigned __int64 skipN, unsigned __int64 rowLimit)
{
    return new CDynamicDiskReadArg(fileName, in, out, chooseN, skipN, rowLimit);
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

