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

#include "jprop.hpp"
#include "jstring.hpp"
#include "eclhelper_dyn.hpp"
#include "hqlexpr.hpp"

#include "commonext.hpp"

#include "thgraphmaster.ipp"
#include "thormisc.hpp"
#include "thactivitymaster.ipp"
#include "thexception.hpp"

actmaster_decl CGraphElementBase *createMasterContainer(IPropertyTree &xgmml, CGraphBase &owner, CGraphBase *resultsGraph);
MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    registerCreateFunc(createMasterContainer);
    return true;
}

#include "action/thaction.ipp"
#include "aggregate/thaggregate.ipp"
#include "apply/thapply.ipp"
#include "catch/thcatch.ipp"
#include "choosesets/thchoosesets.ipp"
#include "countproject/thcountproject.ipp"
#include "csvread/thcsvread.ipp"
#include "diskread/thdiskread.ipp"
#include "diskwrite/thdiskwrite.ipp"
#include "distribution/thdistribution.ipp"
#include "enth/thenth.ipp"
#include "external/thexternal.ipp"
#include "filter/thfilter.ipp"
#include "firstn/thfirstn.ipp"
#include "funnel/thfunnel.ipp"
#include "hashdistrib/thhashdistrib.ipp"
#include "indexread/thindexread.ipp"
#include "indexwrite/thindexwrite.ipp"
#include "iterate/thiterate.ipp"
#include "join/thjoin.ipp"
#include "keydiff/thkeydiff.ipp"
#include "keyedjoin/thkeyedjoin.ipp"
#include "keypatch/thkeypatch.ipp"
#include "limit/thlimit.ipp"
#include "lookupjoin/thlookupjoin.ipp"
#include "msort/thmsort.ipp"
#include "nullaction/thnullaction.ipp"
#include "pipewrite/thpipewrite.ipp"
#include "result/thresult.ipp"
#include "rollup/throllup.ipp"
#include "selectnth/thselectnth.ipp"
#include "spill/thspill.ipp"
#include "topn/thtopn.ipp"
#include "when/thwhen.ipp"
#include "wuidread/thwuidread.ipp"
#include "wuidwrite/thwuidwrite.ipp"
#include "xmlread/thxmlread.ipp"
#include "xmlwrite/thxmlwrite.ipp"
#include "merge/thmerge.ipp"
#include "fetch/thfetch.ipp"
#include "loop/thloop.ipp"

CActivityBase *createGroupActivityMaster(CMasterGraphElement *container);

static bool getWorkunitResultFilename(CGraphElementBase &container, StringBuffer & diskFilename, const char * wuid, const char * stepname, int sequence)
{
    try
    {
        Owned<IConstWUResult> result;
        if (wuid)
            result.setown(container.queryCodeContext()->getExternalResult(wuid, stepname, sequence));
        else
            result.setown(container.queryCodeContext()->getResultForGet(stepname, sequence));
        if (!result)
            throw MakeThorException(TE_FailedToRetrieveWorkunitValue, "Failed to find value %s:%d in workunit %s", stepname?stepname:"(null)", sequence, wuid?wuid:"(null)");

        SCMStringBuffer tempFilename;
        result->getResultFilename(tempFilename);
        if (tempFilename.length() == 0)
            return false;

        diskFilename.append("~").append(tempFilename.str());
        return true;
    }
    catch (IException * e) 
    {
        StringBuffer text; 
        e->errorMessage(text); 
        e->Release();
        throw MakeThorException(TE_FailedToRetrieveWorkunitValue, "Failed to find value %s:%d in workunit %s [%s]", stepname?stepname:"(null)", sequence, wuid?wuid:"(null)", text.str());
    }
    return false;
}


class CGenericMasterGraphElement : public CMasterGraphElement
{
    StringBuffer wuidDiskFilename;
    bool wuidDiskReadChecked = false;

    void checkWuidDiskRead()
    {
        if (wuidDiskReadChecked)
            return;
        // replace wuidread act. with diskread act. if needed
        wuidDiskFilename.clear();
        IHThorWorkunitReadArg *wuReadHelper = (IHThorWorkunitReadArg *)queryHelper();
        wuReadHelper->onCreate(queryCodeContext(), NULL, NULL);
        OwnedRoxieString fromWuid(wuReadHelper->getWUID());
        if (getWorkunitResultFilename(*this, wuidDiskFilename, fromWuid, wuReadHelper->queryName(), wuReadHelper->querySequence()))
        {
            Owned<IHThorDiskReadArg> diskReadHelper = createWorkUnitReadArg(wuidDiskFilename, LINK(wuReadHelper));
            activity.setown(createDiskReadActivityMaster(this, diskReadHelper));
        }
        else
            activity.setown(factory(kind));
        wuidDiskReadChecked = true;
    }
public:
    CGenericMasterGraphElement(CGraphBase &owner, IPropertyTree &xgmml) : CMasterGraphElement(owner, xgmml)
    {
    }
    virtual void serializeCreateContext(MemoryBuffer &mb) override
    {
        // bit of hack, need to tell slave if wuidread converted to diskread (see master activity)
        CMasterGraphElement::serializeCreateContext(mb);
        if (kind == TAKworkunitread)
        {
            checkWuidDiskRead();

            /* the meta information below is used by the worker at factory time
             * to create the correct activity
             */
            if (wuidDiskFilename.length())
            {
                mb.append(true); // flag to slaves that they should create diskread
                mb.append(wuidDiskFilename);
            }
            else
                mb.append(false);
        }
    }
    virtual CActivityBase *factory(ThorActivityKind kind)
    {
        CActivityBase *ret = NULL;
        switch (kind)
        {
            case TAKfiltergroup:
            case TAKlocalresultread:
            case TAKif:
            case TAKchildif:
            case TAKcase:
            case TAKchildcase:
            case TAKdegroup:
            case TAKsplit:
            case TAKproject:
            case TAKprefetchproject:
            case TAKprefetchcountproject:
            case TAKxmlparse:
            case TAKchilditerator:
            case TAKlinkedrawiterator:
            case TAKcatch:
            case TAKsample:
            case TAKnormalize:
            case TAKnormalizechild:
            case TAKnormalizelinkedchild:
            case TAKinlinetable:
            case TAKpull:
            case TAKnull:
            case TAKpiperead:
            case TAKpipethrough:
            case TAKparse:
            case TAKchildaggregate:
            case TAKchildgroupaggregate:
            case TAKchildthroughnormalize:
            case TAKchildnormalize:
            case TAKapply:
            case TAKfunnel:
            case TAKcombine:
            case TAKcombinegroup:
            case TAKregroup:
            case TAKsorted:
            case TAKnwayinput:
            case TAKnwayselect:
            case TAKnwaymerge:
            case TAKnwaymergejoin:
            case TAKnwayjoin:
            case TAKgraphloopresultread:
            case TAKstreamediterator:
            case TAKsoap_rowdataset:
            case TAKsoap_rowaction:
            case TAKsoap_datasetdataset:
            case TAKsoap_datasetaction:
            case TAKhttp_rowdataset:
            case TAKdistributed:
            case TAKtrace:
            case TAKemptyaction:
                ret = new CMasterActivity(this);
                break;
            case TAKskipcatch:
            case TAKcreaterowcatch:
                ret = createSkipCatchActivityMaster(this);
                break;
            case TAKdiskread:
            case TAKdisknormalize:
            case TAKspillread:
                ret = createDiskReadActivityMaster(this);
                break;
            case TAKdiskaggregate:
                ret = createDiskAggregateActivityMaster(this);
                break;
            case TAKdiskcount:
                ret = createDiskCountActivityMaster(this);
                break;
            case TAKdiskgroupaggregate:
                ret = createDiskGroupAggregateActivityMaster(this);
                break;  
            case TAKindexread:
                ret = createIndexReadActivityMaster(this);
                break;
            case TAKindexcount:
                ret = createIndexCountActivityMaster(this);
                break;
            case TAKindexnormalize:
                ret = createIndexNormalizeActivityMaster(this);
                break;
            case TAKindexaggregate:
                ret = createIndexAggregateActivityMaster(this);
                break;
            case TAKindexgroupaggregate:
            case TAKindexgroupexists:
            case TAKindexgroupcount:
                ret = createIndexGroupAggregateActivityMaster(this);
                break;
            case TAKdiskwrite:
            case TAKspillwrite:
                ret = createDiskWriteActivityMaster(this);
                break;
            case TAKcsvwrite:
                ret = createCsvWriteActivityMaster(this);
                break;
            case TAKspill:
                ret = createSpillActivityMaster(this);
                break;
            case TAKdedup:
            case TAKrollup:
            case TAKrollupgroup:
                ret = createDedupRollupActivityMaster(this);
                break;
            case TAKfilter:
            case TAKfilterproject:
                ret = createFilterActivityMaster(this);
                break;
            case TAKnonempty:
                ret = createNonEmptyActivityMaster(this);
                break;
            case TAKsort:
                ret = createSortActivityMaster(this);
                break;
            case TAKgroup:
                ret = createGroupActivityMaster(this);
                break;
            case TAKprocess:
            case TAKiterate:
                ret = createIterateActivityMaster(this);
                break;
            case TAKthroughaggregate:
                ret = createThroughAggregateActivityMaster(this);
                break;
            case TAKaggregate:
            case TAKexistsaggregate:
            case TAKcountaggregate:
                ret = createAggregateActivityMaster(this);
                break;
            case TAKnwaydistribute:
                ret = createNWayDistributeActivityMaster(this);
                break;
            case TAKhashdistribute:
            case TAKpartition:
                ret = createHashDistributeActivityMaster(this);
                break;
            case TAKhashaggregate:
                ret = createHashAggregateActivityMaster(this);
                break;
            case TAKhashjoin:
            case TAKhashdenormalize:
            case TAKhashdenormalizegroup:
                ret= createHashJoinActivityMaster(this);
                break;
            case TAKkeyeddistribute:
                ret = createKeyedDistributeActivityMaster(this);
                break;
            case TAKhashdistributemerge: 
                ret = createDistributeMergeActivityMaster(this);
                break;
            case TAKhashdedup:
                ret = createHashDedupMergeActivityMaster(this);
                break;
            case TAKfirstn:
                ret = createFirstNActivityMaster(this);
                break;
            case TAKjoin:
            case TAKselfjoin:
            case TAKselfjoinlight:
            case TAKdenormalize:
            case TAKdenormalizegroup:
                ret = createJoinActivityMaster(this);
                break;

            case TAKlookupjoin:
            case TAKalljoin:
            case TAKlookupdenormalize:
            case TAKlookupdenormalizegroup:
            case TAKsmartjoin:
            case TAKsmartdenormalize:
            case TAKsmartdenormalizegroup:
            case TAKalldenormalize:
            case TAKalldenormalizegroup:
                ret = createLookupJoinActivityMaster(this);
                break;
            case TAKkeyedjoin:
            case TAKkeyeddenormalize:
            case TAKkeyeddenormalizegroup:
                ret = createKeyedJoinActivityMaster(this);
                break;
            case TAKworkunitwrite:
                ret = createWorkUnitWriteActivityMaster(this);
                break;
            case TAKdictionaryworkunitwrite:
                ret = createDictionaryWorkunitWriteMaster(this);
                break;
            case TAKdictionaryresultwrite:
                if (!queryOwner().queryOwner() || queryOwner().isGlobal()) // don't need dictionary in master if in local child query
                    ret = createDictionaryResultActivityMaster(this);
                else
                    ret = new CMasterActivity(this);
                break;
                break;
            case TAKremoteresult:
                ret = createResultActivityMaster(this);
                break;
            case TAKselectn:
                ret = createSelectNthActivityMaster(this);
                break;
            case TAKenth:
                ret = createEnthActivityMaster(this);
                break;
            case TAKdistribution:
                ret = createDistributionActivityMaster(this);
                break;
            case TAKcountproject:
                ret = createCountProjectActivityMaster(this);
                break;
            case TAKchoosesets:
                ret = createChooseSetsActivityMaster(this);
                break;
            case TAKchoosesetsenth:
            case TAKchoosesetslast:
                ret = createChooseSetsPlusActivityMaster(this);
                break;
            case TAKpipewrite:
                ret = createPipeWriteActivityMaster(this);
                break;
            case TAKcsvread:
                ret = createCCsvReadActivityMaster(this);
                break;
            case TAKindexwrite:
                ret = createIndexWriteActivityMaster(this);
                break;
            case TAKfetch:
                ret = createFetchActivityMaster(this);
                break;
            case TAKcsvfetch:
                ret = createCsvFetchActivityMaster(this);
                break;
            case TAKxmlfetch:
            case TAKjsonfetch:
                ret = createXmlFetchActivityMaster(this);
                break;
            case TAKworkunitread:
                ret = createWorkUnitActivityMaster(this);
                break;
            case TAKsideeffect:
                ret = createNullActionActivityMaster(this);
                break;
            case TAKsimpleaction:
                ret = createActionActivityMaster(this);
                break;
            case TAKtopn:
                ret = createTopNActivityMaster(this);
                break;
            case TAKxmlread:
            case TAKjsonread:
                ret = createXmlReadActivityMaster(this);
                break;
            case TAKxmlwrite:
            case TAKjsonwrite:
                ret = createXmlWriteActivityMaster(this, kind);
                break;
            case TAKmerge:
                ret = createMergeActivityMaster(this);
                break;
            case TAKkeydiff:
                ret = createKeyDiffActivityMaster(this);
                break;
            case TAKkeypatch:
                ret = createKeyPatchActivityMaster(this);
                break;
            case TAKlimit:
            case TAKskiplimit:
            case TAKcreaterowlimit:
                ret = createLimitActivityMaster(this);
                break;
            case TAKlooprow:
            case TAKloopcount:
            case TAKloopdataset:
                ret = createLoopActivityMaster(this);
                break;
            case TAKgraphloop:
            case TAKparallelgraphloop:
                ret = createGraphLoopActivityMaster(this);
                break;
            case TAKlocalresultspill:
            case TAKlocalresultwrite:
                /* NB: create even if non-global child graph, because although the result itself
                 * won't be used, codegen. graph initialization code, may reference the result on the master
                 */
                ret = createLocalResultActivityMaster(this);
                break;
            case TAKgraphloopresultwrite:
                ret = createGraphLoopResultActivityMaster(this);
                break;
            case TAKchilddataset:
                UNIMPLEMENTED;
            case TAKifaction:
                throwUnexpected();
            case TAKwhen_dataset:
                ret = createWhenActivityMaster(this);
                break;
            case TAKexternalprocess:
            case TAKexternalsink:
            case TAKexternalsource:
                ret = createExternalActivityMaster(this);
                break;
            default:
                throw MakeActivityException(this, TE_UnsupportedActivityKind, "Unsupported activity kind: %s", activityKindStr(kind));
        }
        return ret;
    }
};

actmaster_decl CGraphElementBase *createMasterContainer(IPropertyTree &xgmml, CGraphBase &owner, CGraphBase *resultsGraph)
{
    return new CGenericMasterGraphElement(owner, xgmml);
}

void updateActivityResult(IConstWorkUnit &workunit, unsigned helperFlags, unsigned sequence, const char *logicalFilename, unsigned __int64 recordCount)
{
    Owned<IWorkUnit> wu = &workunit.lock();
    Owned<IWUResult> r;
    r.setown(updateWorkUnitResult(wu, logicalFilename, sequence));
    r->setResultTotalRowCount(recordCount); 
    r->setResultStatus(ResultStatusCalculated);
    if (TDWresult & helperFlags)
        r->setResultFilename(logicalFilename);
    else
        r->setResultLogicalName(logicalFilename);
    r.clear();
    wu.clear();
}

void CSlavePartMapping::getParts(unsigned i, IArrayOf<IPartDescriptor> &parts)
{
    if (local)
        i = 0;
    if (i>=maps.ordinality()) return;

    CSlaveMap &map = maps.item(i);
    ForEachItemIn(m, map)
        parts.append(*LINK(&map.item(m)));
}

void CSlavePartMapping::serializeNullMap(MemoryBuffer &mb)
{
    mb.append((unsigned)0);
}

void CSlavePartMapping::serializeNullOffsetMap(MemoryBuffer &mb)
{
    mb.append((unsigned)0);
}

void CSlavePartMapping::serializeMap(unsigned i, MemoryBuffer &mb, bool countPrefix)
{
    if (local)
        i = 0;
    if (i >= maps.ordinality() || (0 == maps.item(i).ordinality()))
    {
        if (countPrefix)
            mb.append((unsigned)0);
        return;
    }

    CSlaveMap &map = maps.item(i);
    unsigned nPos = mb.length();
    unsigned n=0;
    if (countPrefix)
        mb.append(n);
    UnsignedArray parts;
    ForEachItemIn(m, map)
        parts.append(map.item(m).queryPartIndex());
    if (countPrefix)
    {
        n = parts.ordinality();
        mb.writeDirect(nPos, sizeof(n), &n);
    }
    fileDesc->serializeParts(mb, parts);
}

CSlavePartMapping::CSlavePartMapping(const char *_logicalName, IFileDescriptor &_fileDesc, IUserDescriptor *_userDesc, IGroup &localGroup, bool _local, bool index, IHash *hash, IDistributedSuperFile *super)
    : fileDesc(&_fileDesc), userDesc(_userDesc), local(_local)
{
    unsigned maxWidth = local ? 1 : localGroup.ordinality();
    logicalName.set(_logicalName);
    fileWidth = fileDesc->numParts();
    if (super && fileWidth)
    {
        bool merge = index;
        unsigned _maxWidth = super->querySubFile(0,true).numParts();
        if (_maxWidth > 1)
        {
            if (index)
            {
                fileWidth -= super->numSubFiles(true); // tlk's
                if (merge)
                    _maxWidth -= 1; // tlk
            }
            if (merge && _maxWidth < maxWidth)
                maxWidth = _maxWidth;
        }
    }
    else if (index && fileWidth>1)
        fileWidth -= 1;

    unsigned p;
    unsigned which = 0;

    if (fileWidth<=maxWidth || NULL!=hash)
    {
        if (fileWidth>maxWidth && 0 != fileWidth % maxWidth)
            throw MakeThorException(0, "Unimplemented - attempting to read distributed file (%s), on smaller cluster that is not a factor of original", logicalName.get());
        
        for (p=0; p<fileWidth; p++)
        {
            Owned<IPartDescriptor> partDesc = fileDesc->getPart(p);
            CSlaveMap *map;
            if (maps.isItem(which))
                map = &maps.item(which);
            else
            {
                map = new CSlaveMap();
                maps.append(*map);
            }
            map->append(*LINK(partDesc));
            partToNode.append(which);
            which++;
            if (which>=maxWidth) which = 0;
        }
    }
    else
    {
        unsigned tally = 0;
        for (p=0; p<fileWidth; p++)
        {
            Owned<IPartDescriptor> partDesc = fileDesc->getPart(p);
            CSlaveMap *map;
            if (maps.isItem(which))
                map = &maps.item(which);
            else
            {
                map = new CSlaveMap();
                maps.append(*map);
            }
            map->append(*LINK(partDesc));
            partToNode.append(which);
            tally += maxWidth;
            if (tally >= fileWidth)
            {
                tally -= fileWidth;
                which++;
            }
        }
    }
}

#include "../activities/fetch/thfetchcommon.hpp"
void CSlavePartMapping::serializeFileOffsetMap(MemoryBuffer &mb)
{
    mb.append(fileWidth);
    DelayedSizeMarker sizeMark(mb);
    ForEachItemIn(sm, maps)
    {
        CSlaveMap &map = maps.item(sm);
        ForEachItemIn(m, map)
        {
            IPartDescriptor &partDesc = map.item(m);
            IPropertyTree &props = partDesc.queryProperties();
            FPosTableEntry entry;
            entry.base = props.getPropInt64("@offset");                     // should check
            entry.top = entry.base+props.getPropInt64("@size");             // was -1?
            entry.index = sm;
            mb.append(sizeof(FPosTableEntry), &entry);
        }
    }
    sizeMark.write();
}

CSlavePartMapping *getFileSlaveMaps(const char *logicalName, IFileDescriptor &fileDesc, IUserDescriptor *userDesc, IGroup &localGroup, bool local, bool index, IHash *hash, IDistributedSuperFile *super)
{
    return new CSlavePartMapping(logicalName, fileDesc, userDesc, localGroup, local, index, hash, super);
}

WUFileKind getDiskOutputKind(unsigned flags)
{
    if (TDXtemporary & flags)
        return WUFileTemporary;
    else if(TDXjobtemp & flags)
        return WUFileJobOwned;
    else if(TDWowned & flags)
        return WUFileOwned;
    else
        return WUFileStandard;
}

void checkSuperFileOwnership(IDistributedFile &file)
{
    if (file.queryAttributes().hasProp("SuperOwner"))
    {
        StringBuffer owners;
        Owned<IPropertyTreeIterator> iter = file.queryAttributes().getElements("SuperOwner");
        if (iter->first())
        {
            for (;;)
            {
                iter->query().getProp(NULL, owners);
                if (!iter->next())
                    break;
                owners.append(", ");
            }
        }
        throw MakeStringException(TE_MemberOfSuperFile, "Cannot write %s, as owned by superfile(s): %s", file.queryLogicalName(), owners.str());
    }
}

void checkFormatCrc(CActivityBase *activity, IDistributedFile *file, unsigned expectedFormatCrc, IOutputMetaData *expected,
                               unsigned projectedFormatCrc, IOutputMetaData *projected, bool index)
{
    IDistributedFile *f = file;
    IDistributedSuperFile *super = f->querySuperFile();
    Owned<IDistributedFileIterator> iter;
    if (super)
    {
        iter.setown(super->getSubFileIterator(true));
        verifyex(iter->first());
        f = &iter->query();
    }
    unsigned prevFormatCrc = 0;
    StringBuffer kindStr(activityKindStr(activity->queryContainer().getKind()));
    for (;;)
    {
        unsigned dfsCrc = 0;
        if (f->getFormatCrc(dfsCrc)) // can't validate if missing
        {
            if (prevFormatCrc && (prevFormatCrc != dfsCrc)) // NB: all subfiles must have same dfsCrc and will use same translators for now (see HPCC-21834)
            {
                StringBuffer fileStr;
                if (super)
                    fileStr.append("Superfile: ").append(file->queryLogicalName()).append(", subfile: ");
                else fileStr.append("File: ");
                fileStr.append(f->queryLogicalName());

                Owned<IThorException> e = MakeActivityException(activity, TE_FormatCrcMismatch, "%s: Layout does not match published layout. %s", kindStr.str(), fileStr.str());
                if (index && !f->queryAttributes().hasProp("_record_layout")) // Cannot verify if _true_ crc mismatch if soft layout missing anymore
                    LOG(MCwarning, thorJob, e);
                else
                {
                    if (activity->queryContainer().queryJob().getWorkUnitValueBool("skipFileFormatCrcCheck", false))
                    {
                        // propagate as warning to workunit
                        e->setAction(tea_warning);
                        e->setSeverity(SeverityWarning);
                        activity->fireException(e);
                    }
                    else
                        throw e.getClear();
                }
            }
        }
        prevFormatCrc = dfsCrc;
        if (!super||!iter->next())
            break;
        f = &iter->query();
    }
}

void loadMasters()
{
}
