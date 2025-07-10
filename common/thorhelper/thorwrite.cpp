/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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
#include "jliball.hpp"

#include "thorfile.hpp"

#include "eclhelper.hpp"
#include "eclrtl.hpp"
#include "eclrtl_imp.hpp"
#include "rtlfield.hpp"
#include "rtlds_imp.hpp"
#include "rtldynfield.hpp"
#include "rtlformat.hpp"
#include "roxiemem.hpp"

#include "rmtclient.hpp"
#include "rmtfile.hpp"

#include "thorwrite.hpp"
#include "rtlcommon.hpp"
#include "thorcommon.hpp"
#include "csvsplitter.hpp"
#include "thorxmlread.hpp"

//---------------------------------------------------------------------------------------------------------------------

/*
 * A class that implements IRowWriteFormatMapping - which provides all the information representing a translation 
 * from projected->expected->actual.
 */
class DiskWriteMapping : public CInterfaceOf<IRowWriteFormatMapping>
{
public:
    DiskWriteMapping(RecordTranslationMode _mode, const char * _format, IOutputMetaData & _projected, unsigned _expectedCrc, IOutputMetaData & _expected, unsigned _projectedCrc, const IPropertyTree * _formatOptions)
    : mode(_mode), format(_format), expectedCrc(_expectedCrc), projectedCrc(_projectedCrc), projectedMeta(&_projected), expectedMeta(&_expected), formatOptions(_formatOptions)
    {}

    virtual const char * queryFormat() const override { return format; }
    virtual unsigned getExpectedCrc() const override { return expectedCrc; }
    virtual unsigned getProjectedCrc() const override { return projectedCrc; }
    virtual IOutputMetaData * queryProjectedMeta() const override { return projectedMeta; }
    virtual IOutputMetaData * queryExpectedMeta() const override { return expectedMeta; }
    virtual const IPropertyTree * queryFormatOptions() const override { return formatOptions; }
    virtual RecordTranslationMode queryTranslationMode() const override { return mode; }

    virtual bool matches(const IRowWriteFormatMapping * other) const
    {
        if ((mode != other->queryTranslationMode()) || !streq(format, other->queryFormat()))
            return false;
        if ((expectedCrc && expectedCrc == other->getExpectedCrc()) || (expectedMeta == other->queryExpectedMeta()))
        {
            if (!areMatchingPTrees(formatOptions, other->queryFormatOptions()))
                return false;
            return true;
        }
        return false;
    }

protected:
    RecordTranslationMode mode;
    StringAttr format;
    unsigned expectedCrc;
    unsigned projectedCrc;
    Linked<IOutputMetaData> projectedMeta;
    Linked<IOutputMetaData> expectedMeta;
    Linked<const IPropertyTree> formatOptions;
};

THORHELPER_API IRowWriteFormatMapping * createRowWriteFormatMapping(RecordTranslationMode mode, const char * format, IOutputMetaData & projected, unsigned expectedCrc, IOutputMetaData & expected, unsigned projectedCrc, const IPropertyTree * formatOptions)
{
    assertex(formatOptions);
    return new DiskWriteMapping(mode, format, projected, expectedCrc, expected, projectedCrc, formatOptions);
}


void createGenericOptionsFromHelper(FileAccessOptions & options, IHThorGenericDiskWriteArg & helper, IPropertyTree * node, const char * defaultStoragePlaneName)
{
    Owned<IPropertyTree> formatOptions(createPTree());
    Owned<IPropertyTree> providerOptions(createPTree());

    unsigned helperFlags = helper.getFlags();
    bool isGeneric = (helperFlags & TDXgeneric) != 0;

    if (isGeneric)
        options.format.set(helper.queryFormat());
    else
        options.format.set("flat");

    roxiemem::OwnedRoxieString helperCluster(helper.getCluster(0));
    const char * targetPlaneName = helperCluster.get();
    if (!targetPlaneName)
        targetPlaneName = defaultStoragePlaneName;

    Owned<const IStoragePlane> storagePlane = getStoragePlaneByName(targetPlaneName, false);
    if (storagePlane)
    {
        //MORE: Get the default compression when that is implemented
        providerOptions->setPropInt64("@sizeIoBuffer", storagePlane->getAttribute(BlockedSequentialIO));
    }

    if (node)
    {
        const char *recordTranslationModeHintText = node->queryProp("hint[@name='layouttranslation']/@value");
        if (recordTranslationModeHintText)
            options.recordTranslationMode = getTranslationMode(recordTranslationModeHintText, true);
    }

    providerOptions->setPropBool("@forceCompressed", (helperFlags & TDXcompress) != 0);
    formatOptions->setPropBool("@grouped", ((helperFlags & TDXgrouped) != 0));

    if (isGeneric)
    {
        CPropertyTreeWriter formatWriter(formatOptions);
        helper.getFormatOptions(formatWriter);

        CPropertyTreeWriter providerWriter(providerOptions);
        helper.getProviderOptions(providerWriter);
    }

    rtlDataAttr k;
    size32_t kl;
    helper.getEncryptKey(kl,k.refdata());
    if (kl)
    {
        providerOptions->setPropBin("encryptionKey", kl, k.getdata());
        providerOptions->setPropBool("@blockcompressed", true);
        providerOptions->setPropBool("@compressed", true);
    }

    options.formatOptions.setown(formatOptions.getClear());
    options.providerOptions.setown(providerOptions.getClear());
}
