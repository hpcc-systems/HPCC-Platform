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

#ifndef WUATTR_HPP
#define WUATTR_HPP

#include "jlib.hpp"
#include "jstatcodes.h"

#ifdef WORKUNIT_EXPORTS
    #define WORKUNIT_API DECL_EXPORT
#else
    #define WORKUNIT_API DECL_IMPORT
#endif

//The wuattribute values start from a high value - so that they do not overlap with StXXX
//They are not currently persisted, so it is fine to modify the list for major releases
enum WuAttr : unsigned
{
    WaNone = 0x80000000,
    WaAll,
    WaKind,
    WaIdSource,
    WaIdTarget,
    WaSourceIndex,
    WaTargetIndex,
    WaLabel,
    WaIsDependency,
    WaIsChildGraph,
    WaDefinition,
    WaDefinitionList,
    WaEclName,
    WaEclNameList,
    WaEclText,
    WaRecordSize,
    WaPredictedCount,
    WaIsGrouped,
    WaIsLocal,
    WaIsCoLocal,
    WaIsOrdered,
    WaIsInternal,
    WaIsNoAccess,
    WaIsGraphIndependent,
    WaIsUpdateIfChanged,
    WaIsKeyed,
    WaIsPreload,
    WaIsDiskAccessRequired,
    WaIsFileOpt,
    WaIsIndexOpt,
    WaIsPatchOpt,
    WaFilename,
    WaIndexname,
    WaPatchFilename,
    WaIsFilenameDynamic,
    WaIsIndexnameDynamic,
    WaIsPatchFilenameDynamic,
    WaIsEmbedded,
    WaIsSpill,
    WaIsGlobalSpill,
    WaIsAccessedFromChild,
    WaIsTransformSpill,
    WaOriginalFilename,
    WaOutputFilename,
    WaUpdatedFilename,
    WaDistributeIndexname,
    WaIsOriginalFilenameDynamic,
    WaIsOutputFilenameDynamic,
    WaIsUpdatedFilenameDynamic,
    WaIsDistributeIndexnameDynamic,
    WaSignedBy,
    WaMetaDistribution,
    WaMetaGrouping,
    WaMetaGlobalSortOrder,
    WaMetaLocalSortOrder,
    WaMetaGroupSortOrder,
    WaSection,
    WaSectionList,
    WaLibraryName,
    WaMatchLikelihood,
    WaSpillReason,
    WaNumChildQueries,
    WaSizeClassApprox,
    WaIdParentActivity,
    WaNumParallel,
    WaAlgorithm,
    WaSizePreload,
    WaIdLoop,
    WaIdSubGraph,
    WaIdGraph,
    WaIdChildGraph,
    WaIdAmbiguousGraph,      // unfortunately used for global graphs (library) and child graphs (remote), change in another PR
    WaIdLibraryGraph,
    WaIdRemoteSubGraph,
    WaInterfaceHash,
    WaNumMaxOutputs,
    WaIsRootGraph,
    WaIsNWay,
    WaIsCosort,
    WaNumGlobalUses,
    WaIsMultiInstance,
    WaIsDelayed,
    WaIsChild,
    WaIsSequential,
    WaIsLoopBody,
    WaNumResults,
    WaWhenIndex,
    WaIdDependency,
    WaIdDependencyList,
    WaIsScheduled,
    WaIdSuccess,
    WaIdFailure,
    WaIdRecovery,
    WaIdPersist,
    WaIdScheduled,
    WaPersistName,
    WaType,
    WaMode,
    WaState,
    WaCluster,
    WaCriticalSection,
    WaDiskFormat,
    WaRecordFormat,
    WaServiceName,
    WaMax
};
inline WuAttr & operator++(WuAttr & x) { assert(x<WaMax); x = (WuAttr)(x+1); return x; }
extern WORKUNIT_API const char * queryWuAttributeName(WuAttr kind);
extern WORKUNIT_API WuAttr queryWuAttribute(const char * kind, WuAttr dft);
extern WORKUNIT_API const char * queryAttributeValue(IPropertyTree & src, WuAttr kind, StringBuffer & scratchpad); // may return pointer to scratchpad if necessary
extern WORKUNIT_API WuAttr queryGraphAttrToWuAttr(const char * name);
extern WORKUNIT_API WuAttr queryGraphChildAttToWuAttr(const char * name);

extern WORKUNIT_API void setAttributeValue(IPropertyTree & tgt, WuAttr kind, const char * value);
extern WORKUNIT_API void setAttributeValueBool(IPropertyTree & tgt, WuAttr kind, bool value, bool alwaysAdd = false);
extern WORKUNIT_API void setAttributeValueInt(IPropertyTree & tgt, WuAttr kind, __int64 value);

extern WORKUNIT_API bool isListAttribute(WuAttr kind);
extern WORKUNIT_API bool isMultiAttribute(WuAttr kind);
extern WORKUNIT_API WuAttr getListAttribute(WuAttr kind);
extern WORKUNIT_API WuAttr getSingleKindOfListAttribute(WuAttr kind);

#endif
