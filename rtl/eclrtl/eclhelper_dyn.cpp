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

class ECLRTL_API CDynamicDiskReadArg : public CThorDiskReadArg, implements IDynamicIndexReadArg
{
public:
    CDynamicDiskReadArg(const char *_fileName, IOutputMetaData *_in, IOutputMetaData *_projected, IOutputMetaData *_out, unsigned __int64 _chooseN, unsigned __int64 _skipN, unsigned __int64 _rowLimit)
        : fileName(_fileName), in(_in), projected(_projected), out(_out), chooseN(_chooseN), skipN(_skipN), rowLimit(_rowLimit)
    {
        translator.setown(createRecordTranslator(out->queryRecordAccessor(true), projected->queryRecordAccessor(true)));
    }
    virtual bool needTransform() override final
    {
        return translator->needsTranslate();
    }
    virtual unsigned getFlags() override final
    {
        return flags;
    }
    virtual void createSegmentMonitors(IIndexReadContext *irc) override final
    {
        filters.createSegmentMonitors(irc);
    }

    virtual IOutputMetaData * queryOutputMeta() override final
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
    virtual unsigned getProjectedFormatCrc() override final
    {
        return 0;  // engines should treat 0 as 'ignore'
    }
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * src) override final
    {
        return translator->translate(rowBuilder, fieldCallback, (const byte *) src);
    }
    virtual unsigned __int64 getChooseNLimit() override final { return chooseN; }
    virtual unsigned __int64 getRowLimit() override final { return rowLimit; }
// IDynamicIndexReadArg
    virtual void addFilter(const char *filter) override final
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

class ECLRTL_API CDynamicIndexReadArg : public CThorIndexReadArg, implements IDynamicIndexReadArg
{
public:
    CDynamicIndexReadArg(const char *_fileName, IOutputMetaData *_in, IOutputMetaData *_projected, IOutputMetaData *_out, unsigned __int64 _chooseN, unsigned __int64 _skipN, unsigned __int64 _rowLimit)
        : fileName(_fileName), in(_in), projected(_projected), out(_out), chooseN(_chooseN), skipN(_skipN), rowLimit(_rowLimit)
    {
        translator.setown(createRecordTranslator(out->queryRecordAccessor(true), projected->queryRecordAccessor(true)));
    }
    virtual bool needTransform() override final
    {
        return translator->needsTranslate();
    }
    virtual unsigned getFlags() override final
    {
        return TIRnewfilters;
    }
    virtual void createSegmentMonitors(IIndexReadContext *irc) override final
    {
        filters.createSegmentMonitors(irc);
    }

    virtual IOutputMetaData * queryOutputMeta() override final
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
    virtual unsigned getProjectedFormatCrc() override final
    {
        return 0;  // engines should treat 0 as 'ignore'
    }
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * src) override final
    {
        return translator->translate(rowBuilder, fieldCallback, (const byte *) src);
    }
    virtual unsigned __int64 getChooseNLimit() override final { return chooseN; }
    virtual unsigned __int64 getRowLimit() override final { return rowLimit; }
// IDynamicIndexReadArg
    virtual void addFilter(const char *filter) override final
    {
        filters.addFilter(in->queryRecordAccessor(true), filter);
    }

private:
    UnexpectedVirtualFieldCallback fieldCallback;
    StringAttr fileName;
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


extern ECLRTL_API IHThorDiskReadArg *createDiskReadArg(const char *fileName, IOutputMetaData *in, IOutputMetaData *projected, IOutputMetaData *out, unsigned __int64 chooseN, unsigned __int64 skipN, unsigned __int64 rowLimit)
{
    return new CDynamicDiskReadArg(fileName, in, projected, out, chooseN, skipN, rowLimit);
}

extern ECLRTL_API IHThorIndexReadArg *createIndexReadArg(const char *fileName, IOutputMetaData *in, IOutputMetaData *projected, IOutputMetaData *out, unsigned __int64 chooseN, unsigned __int64 skipN, unsigned __int64 rowLimit)
{
    return new CDynamicIndexReadArg(fileName, in, projected, out, chooseN, skipN, rowLimit);
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

