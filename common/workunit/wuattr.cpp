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

#include "wuattr.hpp"
#include "jptree.hpp"
#include "jstats.h"

struct WuAttrInfo
{
public:
    WuAttr kind;                // The attribute enumeration
    StatisticMeasure measure;   // units for the measure
    const char * name;          // text version of the attribute
    const char * graphPath;     // The xpath required to extract a result from a graph node.
    const char * overridePath;  // Alternative xpath to check 1st for some overloaded attributes
    const char * childPath;     // The name of the <atr> for setting, or matching when iterating
    const char * dft;           // default value if not present
    WuAttr singleKind;
    WuAttr multiKind;
};

#define CHILDPATH(x) "att[@name='" x "']/@value"
#define CHILDMPATH(x) "att[@name='" x "'][1]/@value"
#define ATTR(kind, measure, path)           { Wa ## kind, measure, #kind, path, nullptr, nullptr, nullptr, WaNone, WaNone }
#define ALTATTR(kind, measure, path, alt)   { Wa ## kind, measure, #kind, path, alt, nullptr, nullptr, WaNone, WaNone }
#define CHILD(kind, measure, path)          { Wa ## kind, measure, #kind, CHILDPATH(path), nullptr, path, nullptr, WaNone, WaNone }
#define CHILD_MULTI(kind, measure, path)    { Wa ## kind, measure, #kind, CHILDMPATH(path), nullptr, path, nullptr, Wa ## kind, Wa ## kind ## List }
#define CHILD_LIST(kind, measure)           { Wa ## kind ## List, measure, #kind "List", nullptr, nullptr, nullptr, nullptr, Wa ## kind, Wa ## kind ## List }
#define CHILD_D(kind, measure, path, dft)   { Wa ## kind, measure, #kind, CHILDPATH(path), nullptr, path, dft, WaNone, WaNone  }
#define ATTRX(kind, measure)                { Wa ## kind, measure, #kind, nullptr, nullptr, nullptr, nullptr, WaNone, WaNone  }
#define ATTRX_MULTI(kind, measure)          { Wa ## kind , measure, #kind, nullptr, nullptr, nullptr, nullptr, Wa ## kind, Wa ## kind ## List  }
#define ATTRX_LIST(kind, measure)           { Wa ## kind ## List, measure, #kind "List", nullptr, nullptr, nullptr, nullptr, Wa ## kind, Wa ## kind ## List }

const static WuAttrInfo attrInfo[] = {
    { WaNone, SMeasureNone, "none", nullptr, nullptr, nullptr, nullptr, WaNone, WaNone  },
    { WaAll, SMeasureNone, "all", nullptr, nullptr, nullptr, nullptr, WaNone, WaNone  },
    CHILD(Kind, SMeasureEnum, "_kind"),
    ALTATTR(IdSource, SMeasureId, "@source", "att[@name='_sourceActivity']/@value"),
    ALTATTR(IdTarget, SMeasureId, "@target", "att[@name='_targetActivity']/@value"),
    CHILD_D(SourceIndex, SMeasureText, "_sourceIndex", "0"),
    CHILD_D(TargetIndex, SMeasureText, "_targetIndex", "0"),
    ATTR(Label, SMeasureText, "@label"),
    CHILD(IsDependency, SMeasureBool, "_dependsOn"),
    CHILD(IsChildGraph, SMeasureBool, "_childGraph"),
    CHILD_MULTI(Definition, SMeasureText, "definition"),
    CHILD_LIST(Definition, SMeasureText),
    CHILD_MULTI(EclName, SMeasureText, "name"),
    CHILD_LIST(EclName, SMeasureText),
    CHILD(EclText, SMeasureText, "ecl"),
    CHILD(RecordSize, SMeasureText, "recordSize"),
    CHILD(PredictedCount, SMeasureText, "predictedCount"),
    CHILD(IsGrouped, SMeasureBool, "grouped"),
    CHILD(IsLocal, SMeasureBool, "local"),
    CHILD(IsCoLocal, SMeasureBool, "coLocal"),
    CHILD(IsOrdered, SMeasureBool, "ordered"),
    CHILD(IsInternal, SMeasureBool, "_internal"),
    CHILD(IsNoAccess, SMeasureBool, "noAccess"),
    CHILD(IsGraphIndependent, SMeasureBool, "_graphIndependent"),
    CHILD(IsUpdateIfChanged, SMeasureBool, "_updateIfChanged"),
    CHILD(IsKeyed, SMeasureBool, "_isKeyed"),
    CHILD(IsPreload, SMeasureBool, "preload"),
    CHILD(IsDiskAccessRequired, SMeasureBool, "_diskAccessRequired"),
    CHILD(IsFileOpt, SMeasureBool, "_isOpt"),
    CHILD(IsIndexOpt, SMeasureBool, "_isIndexOpt"),
    CHILD(IsPatchOpt, SMeasureBool, "_isPatchOpt"),
    CHILD(Filename, SMeasureFilename, "_fileName"),
    CHILD(Indexname, SMeasureFilename, "_indexFileName"),
    CHILD(PatchFilename, SMeasureFilename, "_patchName"),
    CHILD(IsFilenameDynamic, SMeasureBool, "_file_dynamic"),
    CHILD(IsIndexnameDynamic, SMeasureBool, "_indexFile_dynamic"),
    CHILD(IsPatchFilenameDynamic, SMeasureBool, "_patch_dynamic"),
    CHILD(IsEmbedded, SMeasureBool, "embedded"),
    CHILD(IsSpill, SMeasureBool, "_isSpill"),
    CHILD(IsGlobalSpill, SMeasureBool, "_isSpillGlobal"),
    CHILD(IsAccessedFromChild, SMeasureBool, "_fromChild"),
    CHILD(IsTransformSpill, SMeasureBool, "_isTransformSpill"),
    CHILD(OriginalFilename, SMeasureFilename, "_originalName"),
    CHILD(OutputFilename, SMeasureFilename, "_outputName"),
    CHILD(UpdatedFilename, SMeasureFilename, "_updatedName"),
    CHILD(DistributeIndexname, SMeasureFilename, "_distributeIndexName"),
    CHILD(IsOriginalFilenameDynamic, SMeasureBool, "_originalName_dynamic"),
    CHILD(IsOutputFilenameDynamic, SMeasureBool, "_outputName_dynamic"),
    CHILD(IsUpdatedFilenameDynamic, SMeasureBool, "_updatedName_dynamic"),
    CHILD(IsDistributeIndexnameDynamic, SMeasureBool, "_distributeIndexName_dynamic"),
    CHILD(SignedBy, SMeasureText, "signedBy"),
    CHILD(MetaDistribution, SMeasureText, "metaDistribution"),
    CHILD(MetaGrouping, SMeasureText, "metaGrouping"),
    CHILD(MetaGlobalSortOrder, SMeasureText, "metaGlobalSortOrder"),
    CHILD(MetaLocalSortOrder, SMeasureText, "metaLocalSortOrder"),
    CHILD(MetaGroupSortOrder, SMeasureText, "metaGroupSortOrder"),
    CHILD_MULTI(Section, SMeasureText, "section"),
    CHILD_LIST(Section, SMeasureText),
    CHILD(LibraryName, SMeasureText, "libname"),
    CHILD(MatchLikelihood, SMeasureText, "matchLikelihood"),
    CHILD(SpillReason, SMeasureText, "spillReason"),
    CHILD(NumChildQueries, SMeasureCount, "childQueries"),
    CHILD(SizeClassApprox, SMeasureSize, "approxClassSize"),
    CHILD(IdParentActivity, SMeasureId, "_parentActivity"),
    CHILD(NumParallel, SMeasureCount, "parallel"),
    CHILD(Algorithm, SMeasureText, "algorithm"),
    CHILD(SizePreload, SMeasureSize, "_preloadSize"),
    CHILD(IdLoop, SMeasureId, "_loopid"),
    CHILD(IdSubGraph, SMeasureId, "_subgraph"),
    CHILD(IdGraph, SMeasureId, "graph"),
    CHILD(IdChildGraph, SMeasureId, "_graphId"),
    CHILD(IdAmbiguousGraph, SMeasureId, "_graphid"),
    CHILD(IdLibraryGraph, SMeasureId, "_libraryGraphId"),
    CHILD(IdRemoteSubGraph, SMeasureId, "_remoteSubGraph"),
    CHILD(InterfaceHash, SMeasureText, "_interfaceHash"),
    CHILD(NumMaxOutputs, SMeasureCount, "_maxOutputs"),
    CHILD(IsRootGraph, SMeasureBool, "rootGraph"),
    ATTR(IsNWay, SMeasureBool, "@nWay"),
    CHILD(IsCosort, SMeasureBool, "cosort"),
    CHILD(NumGlobalUses, SMeasureCount, "_globalUsageCount"),
    ATTR(IsMultiInstance, SMeasureBool, "@multiInstance"),
    ATTR(IsDelayed, SMeasureBool, "@delayed"),
    ATTR(IsChild, SMeasureBool, "@child"),
    ATTR(IsSequential, SMeasureBool, "@sequential"),
    ATTR(IsLoopBody, SMeasureBool, "@loopBody"),
    CHILD(NumResults, SMeasureCount, "_numResults"),
    CHILD(WhenIndex, SMeasureText, "_when"),
    ATTRX_MULTI(IdDependency, SMeasureId),
    ATTRX_LIST(IdDependency, SMeasureId),
    ATTRX(IsScheduled, SMeasureBool),
    ATTRX(IdSuccess, SMeasureId),
    ATTRX(IdFailure, SMeasureId),
    ATTRX(IdRecovery, SMeasureId),
    ATTRX(IdPersist, SMeasureId),
    ATTRX(IdScheduled, SMeasureId),
    ATTRX(PersistName, SMeasureText),
    ATTRX(Type, SMeasureText),
    ATTRX(Mode, SMeasureText),
    ATTRX(State, SMeasureText),
    ATTRX(Cluster, SMeasureText),
    ATTRX(CriticalSection, SMeasureText),
    CHILD(DiskFormat, SMeasureText, "diskFormat"),
    CHILD(RecordFormat, SMeasureText, "recordFormat"),
    CHILD(ServiceName, SMeasureText, "serviceName"),
    { WaMax, SMeasureNone, nullptr, nullptr, nullptr, nullptr, nullptr, WaNone, WaNone }
};

static MapConstStringTo<WuAttr, WuAttr> nameToWa(true);        // Names are case insensitive
static MapConstStringTo<WuAttr, WuAttr> graphAttrToWa(false);  // paths and attrs are cases sensitive
static MapConstStringTo<WuAttr, WuAttr> childAttrToWa(false);

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    static_assert(_elements_in(attrInfo) >= (WaMax-WaNone)+1, "Elements missing from attrInfo[]");
    static_assert(_elements_in(attrInfo) <= (WaMax-WaNone)+1, "Extra elements in attrInfo[]");
    for (unsigned i=0; i < _elements_in(attrInfo); i++)
    {
        const WuAttrInfo & info = attrInfo[i];
        WuAttr attr = (WuAttr)(WaNone + i);
        assertex(info.kind == attr);
        if (info.name)
            nameToWa.setValue(info.name, attr); // cannot clash since derived from a unique name

#ifdef _DEBUG
        const char * prefix = queryMeasurePrefix(info.measure);
        if (info.name && prefix && *prefix)
        {
            if (!startsWith(info.name, prefix))
                printf("Mismatched prefix %s %s\n", info.name, prefix);
        }
#endif

        const char * path = info.graphPath;
        if (path && path[0] == '@')
        {
            if (graphAttrToWa.getValue(path+1))
                throwUnexpectedX("Duplicate Graph Attr");
            graphAttrToWa.setValue(path+1, attr);
        }

        const char * childPath = info.childPath;
        if (childPath)
        {
            if (childAttrToWa.getValue(childPath))
                throwUnexpectedX("Duplicate Child Attr");
            childAttrToWa.setValue(childPath, attr);
        }
    }
    return true;
}


const char * queryWuAttributeName(WuAttr kind)
{
    if ((kind >= WaNone) && (kind < WaMax))
        return attrInfo[kind-WaNone].name;
    return nullptr;
}

WuAttr queryWuAttribute(const char * kind, WuAttr dft)
{
    WuAttr * match = nameToWa.getValue(kind);
    if (match)
        return *match;
    return dft;
}

extern WORKUNIT_API const char * queryAttributeValue(IPropertyTree & src, WuAttr kind, StringBuffer & scratchpad)
{
    if ((kind <= WaNone) || (kind >= WaMax))
        return nullptr;

    const WuAttrInfo & info = attrInfo[kind-WaNone];
    const char * path = info.graphPath;
    const char * altpath = info.overridePath;
    const char * value = altpath ? src.queryProp(altpath) : nullptr;
    if (!value)
        value = src.queryProp(path);
    if (!value && info.dft)
        value = info.dft;

    //The following switch statement allows the value returned to be transformed from the value stored.
    switch (kind)
    {
    case WaIdSource:
    case WaIdTarget:
    case WaIdParentActivity:
        value = scratchpad.clear().append(ActivityScopePrefix).append(value).str();
        break;
    case WaIdLibraryGraph:
        value = scratchpad.clear().append(GraphScopePrefix).append(value).str();
        break;
//    case WaIdLoop:
        //?Prefix with the graph id?
        //value = scratchpad.clear().append(ChildGraphScopePrefix).append(value).str();
        break;
    case WaIdLoop:
    case WaIdSubGraph:
    case WaIdRemoteSubGraph:
        value = scratchpad.clear().append(ChildGraphScopePrefix).append(value).str();
        break;
    }

    return value;
}

extern WORKUNIT_API WuAttr queryGraphAttrToWuAttr(const char * name)
{
    WuAttr * match = graphAttrToWa.getValue(name);
    if (match)
        return *match;
    return WaNone;
}

extern WORKUNIT_API WuAttr queryGraphChildAttToWuAttr(const char * name)
{
    WuAttr * match = childAttrToWa.getValue(name);
    if (match)
        return *match;
    return WaNone;
}


static IPropertyTree * addGraphAttribute(IPropertyTree * node, const char * name)
{
    IPropertyTree * att = createPTree();
    att->setProp("@name", name);
    return node->addPropTree("att", att);
}

void setAttributeValue(IPropertyTree & tgt, WuAttr kind, const char * value)
{
    const WuAttrInfo & info = attrInfo[kind-WaNone];
    const char * path = info.graphPath;
    if (path[0] == '@')
        tgt.setProp(path, value);
    else
        addGraphAttribute(&tgt, info.childPath)->setProp("@value", value);
}

void setAttributeValueBool(IPropertyTree & tgt, WuAttr kind, bool value, bool alwaysAdd)
{
    const WuAttrInfo & info = attrInfo[kind-WaNone];
    const char * path = info.graphPath;
    if (value || alwaysAdd)
    {
        if (path[0] == '@')
            tgt.setPropBool(path, value);
        else
            addGraphAttribute(&tgt, info.childPath)->setPropBool("@value", value);
    }
}

void setAttributeValueInt(IPropertyTree & tgt, WuAttr kind, __int64 value)
{
    const WuAttrInfo & info = attrInfo[kind-WaNone];
    const char * path = info.graphPath;
    if (path[0] == '@')
        tgt.setPropInt64(path, value);
    else
        addGraphAttribute(&tgt, info.childPath)->setPropInt64("@value", value);
}

bool isListAttribute(WuAttr kind)
{
    const WuAttrInfo & info = attrInfo[kind-WaNone];
    return (info.multiKind != WaNone) && (info.multiKind == kind);
}

bool isMultiAttribute(WuAttr kind)
{
    const WuAttrInfo & info = attrInfo[kind-WaNone];
    return (info.singleKind != WaNone) && (info.singleKind == kind);
}

WuAttr getListAttribute(WuAttr kind)
{
    const WuAttrInfo & info = attrInfo[kind-WaNone];
    WuAttr multiKind = info.multiKind;
    return (multiKind != WaNone) && (multiKind != kind) ? multiKind : WaNone;
}

WuAttr getSingleKindOfListAttribute(WuAttr kind)
{
    const WuAttrInfo & info = attrInfo[kind-WaNone];
    return info.singleKind;
}
