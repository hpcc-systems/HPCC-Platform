/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
#ifndef HQLATOM_INCL
#define HQLATOM_INCL

#include "jhash.hpp"        // strangely also includes hqlatom.ipp


extern HQL_API _ATOM abstractAtom;
extern HQL_API _ATOM accessAtom;
extern HQL_API _ATOM actionAtom;
extern HQL_API _ATOM activeAtom;
extern HQL_API _ATOM activeFailureAtom;
extern HQL_API _ATOM activeNlpAtom;
extern HQL_API _ATOM afterAtom;
extern HQL_API _ATOM aggregateAtom;
extern HQL_API _ATOM allAtom;
extern HQL_API _ATOM algorithmAtom;
extern HQL_API _ATOM allocatorAtom;
extern HQL_API _ATOM _alreadyAssignedNestedTag_Atom;
extern HQL_API _ATOM alreadyVisitedAtom;
extern HQL_API _ATOM _alreadyVisitedMarker_Atom;
extern HQL_API _ATOM alwaysAtom;
extern HQL_API _ATOM _array_Atom;
extern HQL_API _ATOM asciiAtom;
extern HQL_API _ATOM assertAtom;
extern HQL_API _ATOM assertConstAtom;
extern HQL_API _ATOM atAtom;
extern HQL_API _ATOM atmostAtom;
extern HQL_API _ATOM _attrAligned_Atom;
extern HQL_API _ATOM _attrDiskSerializedForm_Atom;
extern HQL_API _ATOM _attrInternalSerializedForm_Atom;
extern HQL_API _ATOM _attrLocationIndependent_Atom;
extern HQL_API _ATOM _attrRecordCount_Atom;
extern HQL_API _ATOM _attrSize_Atom;
extern HQL_API _ATOM _attrUnadorned_Atom;
extern HQL_API _ATOM aveAtom;
extern HQL_API _ATOM backupAtom;
extern HQL_API _ATOM bcdAtom;
extern HQL_API _ATOM beforeAtom;
extern HQL_API _ATOM bestAtom;
extern HQL_API _ATOM bindBooleanParamAtom;
extern HQL_API _ATOM bindDataParamAtom;
extern HQL_API _ATOM bindRealParamAtom;
extern HQL_API _ATOM bindSetParamAtom;
extern HQL_API _ATOM bindSignedParamAtom;
extern HQL_API _ATOM bindStringParamAtom;
extern HQL_API _ATOM bindVStringParamAtom;
extern HQL_API _ATOM bindUnicodeParamAtom;
extern HQL_API _ATOM bindUnsignedParamAtom;
extern HQL_API _ATOM bindUtf8ParamAtom;
extern HQL_API _ATOM bitmapAtom;
extern HQL_API _ATOM blobAtom;
extern HQL_API _ATOM cAtom;
extern HQL_API _ATOM caseAtom;
extern HQL_API _ATOM cardinalityAtom;
extern HQL_API _ATOM checkinAtom;
extern HQL_API _ATOM checkoutAtom;
extern HQL_API _ATOM _childAttr_Atom;
extern HQL_API _ATOM choosenAtom;
extern HQL_API _ATOM clusterAtom;
extern HQL_API _ATOM _colocal_Atom;
extern HQL_API _ATOM commonAtom;
extern HQL_API _ATOM compileEmbeddedScriptAtom;
extern HQL_API _ATOM _complexKeyed_Atom;
extern HQL_API _ATOM compressedAtom;
extern HQL_API _ATOM __compressed__Atom;
extern HQL_API _ATOM _conditionFolded_Atom;
extern HQL_API _ATOM constAtom;
extern HQL_API _ATOM contextAtom;
extern HQL_API _ATOM contextSensitiveAtom;
extern HQL_API _ATOM costAtom;
extern HQL_API _ATOM countAtom;
extern HQL_API _ATOM _countProject_Atom;
extern HQL_API _ATOM cppAtom;
extern HQL_API _ATOM _cppBody_Atom;
extern HQL_API _ATOM csvAtom;
extern HQL_API _ATOM ctxmethodAtom;
extern HQL_API _ATOM dataAtom;
extern HQL_API _ATOM _dataset_Atom;
extern HQL_API _ATOM debugAtom;
extern HQL_API _ATOM dedupAtom;
extern HQL_API _ATOM defaultAtom;
extern HQL_API _ATOM _default_Atom;
extern HQL_API _ATOM defaultFieldNameAtom;
extern HQL_API _ATOM defineAtom;
extern HQL_API _ATOM definitionAtom;
extern HQL_API _ATOM deprecatedAtom;
extern HQL_API _ATOM descAtom;
extern HQL_API _ATOM diskAtom;
extern HQL_API _ATOM distributedAtom;
extern HQL_API _ATOM _distributed_Atom;
extern HQL_API _ATOM _dot_Atom;
extern HQL_API _ATOM dynamicAtom;
extern HQL_API _ATOM ebcdicAtom;
extern HQL_API _ATOM eclrtlAtom;
extern HQL_API _ATOM embeddedAtom;
extern HQL_API _ATOM _empty_str_Atom;
extern HQL_API _ATOM encodingAtom;
extern HQL_API _ATOM encryptAtom;
extern HQL_API _ATOM ensureAtom;
extern HQL_API _ATOM enthAtom;
extern HQL_API _ATOM entrypointAtom;
extern HQL_API _ATOM errorAtom;
extern HQL_API _ATOM exceptAtom;
extern HQL_API _ATOM exclusiveAtom;
extern HQL_API _ATOM expireAtom;
extern HQL_API _ATOM exportAtom;
extern HQL_API _ATOM extendAtom;
extern HQL_API _ATOM externalAtom;
extern HQL_API _ATOM failAtom;
extern HQL_API _ATOM failureAtom;
extern HQL_API _ATOM falseAtom;
extern HQL_API _ATOM fastAtom;
extern HQL_API _ATOM fewAtom;
extern HQL_API _ATOM fieldAtom;
extern HQL_API _ATOM fieldsAtom;
extern HQL_API _ATOM filenameAtom;
extern HQL_API _ATOM __fileposAtom;
extern HQL_API _ATOM filepositionAtom;
extern HQL_API _ATOM _files_Atom;
extern HQL_API _ATOM filterAtom;
extern HQL_API _ATOM filteredAtom;
extern HQL_API _ATOM _filtered_Atom;
extern HQL_API _ATOM firstAtom;
extern HQL_API _ATOM firstLeftAtom;
extern HQL_API _ATOM firstRightAtom;
extern HQL_API _ATOM fixedAtom;
extern HQL_API _ATOM flagAtom;
extern HQL_API _ATOM flagsAtom;
extern HQL_API _ATOM flatAtom;
extern HQL_API _ATOM _folded_Atom;
extern HQL_API _ATOM formatAtom;
extern HQL_API _ATOM forwardAtom;
extern HQL_API _ATOM fullonlyAtom;
extern HQL_API _ATOM fullouterAtom;
extern HQL_API _ATOM _function_Atom;
extern HQL_API _ATOM gctxmethodAtom;
extern HQL_API _ATOM getAtom;
extern HQL_API _ATOM getEmbedContextAtom;
extern HQL_API _ATOM getBooleanResultAtom;
extern HQL_API _ATOM getDataResultAtom;
extern HQL_API _ATOM getRealResultAtom;
extern HQL_API _ATOM getSetResultAtom;
extern HQL_API _ATOM getSignedResultAtom;
extern HQL_API _ATOM getStringResultAtom;
extern HQL_API _ATOM getUnicodeResultAtom;
extern HQL_API _ATOM getUnsignedResultAtom;
extern HQL_API _ATOM getUTF8ResultAtom;
extern HQL_API _ATOM globalAtom;
extern HQL_API _ATOM globalContextAtom;
extern HQL_API _ATOM graphAtom;
extern HQL_API _ATOM _graphLocal_Atom;
extern HQL_API _ATOM groupAtom;
extern HQL_API _ATOM groupedAtom;
extern HQL_API _ATOM hashAtom;
extern HQL_API _ATOM headingAtom;
extern HQL_API _ATOM _hidden_Atom;
extern HQL_API _ATOM hintAtom;
extern HQL_API _ATOM holeAtom;
extern HQL_API _ATOM holeposAtom;
extern HQL_API _ATOM __ifblockAtom;
extern HQL_API _ATOM ignoreAtom;
extern HQL_API _ATOM ignoreBaseAtom;
extern HQL_API _ATOM implementsAtom;
extern HQL_API _ATOM _implicitFpos_Atom;
extern HQL_API _ATOM _implicitSorted_Atom;
extern HQL_API _ATOM importAtom;
extern HQL_API _ATOM includeAtom;
extern HQL_API _ATOM indeterminateAtom;
extern HQL_API _ATOM indexAtom;
extern HQL_API _ATOM initfunctionAtom;
extern HQL_API _ATOM inlineAtom;
extern HQL_API _ATOM innerAtom;
extern HQL_API _ATOM interfaceAtom;
extern HQL_API _ATOM internalAtom;
extern HQL_API _ATOM _internal_Atom;
extern HQL_API _ATOM internalFlagsAtom;
extern HQL_API _ATOM _isBlobInIndex_Atom;
extern HQL_API _ATOM _isFunctional_Atom;
extern HQL_API _ATOM isNullAtom;
extern HQL_API _ATOM isValidAtom;
extern HQL_API _ATOM jobAtom;
extern HQL_API _ATOM jobTempAtom;
extern HQL_API _ATOM keepAtom;
extern HQL_API _ATOM keyedAtom;
extern HQL_API _ATOM labeledAtom;
extern HQL_API _ATOM languageAtom;
extern HQL_API _ATOM lastAtom;
extern HQL_API _ATOM leftAtom;
extern HQL_API _ATOM leftonlyAtom;
extern HQL_API _ATOM leftouterAtom;
extern HQL_API _ATOM libraryAtom;
extern HQL_API _ATOM lightweightAtom;
extern HQL_API _ATOM _lightweight_Atom;
extern HQL_API _ATOM limitAtom;
extern HQL_API _ATOM lineIdAtom;
extern HQL_API _ATOM linkAtom;
extern HQL_API _ATOM _linkCounted_Atom;
extern HQL_API _ATOM literalAtom;
extern HQL_API _ATOM loadAtom;
extern HQL_API _ATOM localAtom;
extern HQL_API _ATOM localUploadAtom;
extern HQL_API _ATOM localeAtom;
extern HQL_API _ATOM localFilePositionAtom;
extern HQL_API _ATOM _location_Atom;
extern HQL_API _ATOM logAtom;
extern HQL_API _ATOM logicalFilenameAtom;
extern HQL_API _ATOM lookupAtom;
extern HQL_API _ATOM lzwAtom;
extern HQL_API _ATOM macroAtom;
extern HQL_API _ATOM manyAtom;
extern HQL_API _ATOM markerAtom;
extern HQL_API _ATOM matchxxxPseudoFileAtom;
extern HQL_API _ATOM maxAtom;
extern HQL_API _ATOM maxCountAtom;
extern HQL_API _ATOM maxLengthAtom;
extern HQL_API _ATOM maxSizeAtom;
extern HQL_API _ATOM memoryAtom;
extern HQL_API _ATOM mergeAtom;
extern HQL_API _ATOM mergeTransformAtom;
extern HQL_API _ATOM _metadata_Atom;
extern HQL_API _ATOM methodAtom;
extern HQL_API _ATOM minAtom;
extern HQL_API _ATOM minimalAtom;
extern HQL_API _ATOM moduleAtom;
extern HQL_API _ATOM mofnAtom;
extern HQL_API _ATOM nameAtom;
extern HQL_API _ATOM namedAtom;
extern HQL_API _ATOM namespaceAtom;
extern HQL_API _ATOM newAtom;
extern HQL_API _ATOM newSetAtom;
extern HQL_API _ATOM noBoundCheckAtom;
extern HQL_API _ATOM noCaseAtom;
extern HQL_API _ATOM _noHoist_Atom;
extern HQL_API _ATOM noLocalAtom;
extern HQL_API _ATOM _nonEmpty_Atom;
extern HQL_API _ATOM noOverwriteAtom;
extern HQL_API _ATOM _normalized_Atom;
extern HQL_API _ATOM noRootAtom;
extern HQL_API _ATOM noScanAtom;
extern HQL_API _ATOM noSortAtom;
extern HQL_API _ATOM notAtom;
extern HQL_API _ATOM notMatchedAtom;
extern HQL_API _ATOM notMatchedOnlyAtom;
extern HQL_API _ATOM _noStreaming_Atom;
extern HQL_API _ATOM noTrimAtom;
extern HQL_API _ATOM noTypeAtom;
extern HQL_API _ATOM noXpathAtom;
extern HQL_API _ATOM oldSetFormatAtom;
extern HQL_API _ATOM omethodAtom;
extern HQL_API _ATOM _omitted_Atom;
extern HQL_API _ATOM onceAtom;
extern HQL_API _ATOM onFailAtom;
extern HQL_API _ATOM onWarningAtom;
extern HQL_API _ATOM optAtom;
extern HQL_API _ATOM _ordered_Atom;
extern HQL_API _ATOM _orderedPull_Atom;
extern HQL_API _ATOM _origin_Atom;
extern HQL_API _ATOM _original_Atom;
extern HQL_API _ATOM outAtom;
extern HQL_API _ATOM outoflineAtom;
extern HQL_API _ATOM outputAtom;
extern HQL_API _ATOM overwriteAtom;
extern HQL_API _ATOM ownedAtom;
extern HQL_API _ATOM packedAtom;
extern HQL_API _ATOM parallelAtom;
extern HQL_API _ATOM parameterAtom;
extern HQL_API _ATOM partitionAtom;
extern HQL_API _ATOM partitionLeftAtom;
extern HQL_API _ATOM partitionRightAtom;
extern HQL_API _ATOM payloadAtom;
extern HQL_API _ATOM _payload_Atom;
extern HQL_API _ATOM persistAtom;
extern HQL_API _ATOM physicalFilenameAtom;
extern HQL_API _ATOM physicalLengthAtom;
extern HQL_API _ATOM pluginAtom;
extern HQL_API _ATOM prefetchAtom;
extern HQL_API _ATOM preloadAtom;
extern HQL_API _ATOM priorityAtom;
extern HQL_API _ATOM privateAtom;
extern HQL_API _ATOM pseudoentrypointAtom;
extern HQL_API _ATOM pullAtom;
extern HQL_API _ATOM pulledAtom;
extern HQL_API _ATOM pureAtom;
extern HQL_API _ATOM quoteAtom;
extern HQL_API _ATOM randomAtom;
extern HQL_API _ATOM rangeAtom;
extern HQL_API _ATOM rawAtom;
extern HQL_API _ATOM recordAtom;
extern HQL_API _ATOM recursiveAtom;
extern HQL_API _ATOM referenceAtom;
extern HQL_API _ATOM refreshAtom;
extern HQL_API _ATOM _remote_Atom;
extern HQL_API _ATOM renameAtom;
extern HQL_API _ATOM repeatAtom;
extern HQL_API _ATOM _resourced_Atom;
extern HQL_API _ATOM responseAtom;
extern HQL_API _ATOM restartAtom;
extern HQL_API _ATOM resultAtom;
extern HQL_API _ATOM _results_Atom;
extern HQL_API _ATOM retryAtom;
extern HQL_API _ATOM rightAtom;
extern HQL_API _ATOM rightonlyAtom;
extern HQL_API _ATOM rightouterAtom;
extern HQL_API _ATOM rollbackAtom;
extern HQL_API _ATOM _root_Atom;
extern HQL_API _ATOM rowAtom;
extern HQL_API _ATOM _rowsid_Atom;
extern HQL_API _ATOM rowLimitAtom;
extern HQL_API _ATOM ruleAtom;
extern HQL_API _ATOM saveAtom;
extern HQL_API _ATOM scanAtom;
extern HQL_API _ATOM scanAllAtom;
extern HQL_API _ATOM scopeAtom;
extern HQL_API _ATOM scopeCheckingAtom;
extern HQL_API _ATOM sectionAtom;
extern HQL_API _ATOM _selectorSequence_Atom;
extern HQL_API _ATOM selfAtom;
extern HQL_API _ATOM _selectors_Atom;
extern HQL_API _ATOM separatorAtom;
extern HQL_API _ATOM sequenceAtom;
extern HQL_API _ATOM _sequence_Atom;
extern HQL_API _ATOM sequentialAtom;
extern HQL_API _ATOM serializationAtom;
extern HQL_API _ATOM setAtom;
extern HQL_API _ATOM sharedAtom;
extern HQL_API _ATOM shutdownAtom;
extern HQL_API _ATOM _sideEffect_Atom;
extern HQL_API _ATOM singleAtom;
extern HQL_API _ATOM sizeAtom;
extern HQL_API _ATOM sizeofAtom;
extern HQL_API _ATOM skewAtom;
extern HQL_API _ATOM skipAtom;
extern HQL_API _ATOM snapshotAtom;
extern HQL_API _ATOM soapActionAtom;
extern HQL_API _ATOM syntaxCheckAtom;
extern HQL_API _ATOM httpHeaderAtom;
extern HQL_API _ATOM prototypeAtom;
extern HQL_API _ATOM proxyAddressAtom;
extern HQL_API _ATOM sort_AllAtom;
extern HQL_API _ATOM sort_KeyedAtom;
extern HQL_API _ATOM sortedAtom;
extern HQL_API _ATOM sourceAtom;
extern HQL_API _ATOM stableAtom;
extern HQL_API _ATOM _state_Atom;
extern HQL_API _ATOM steppedAtom;
extern HQL_API _ATOM storeAtom;
extern HQL_API _ATOM storedAtom;
extern HQL_API _ATOM streamedAtom;
extern HQL_API _ATOM _streaming_Atom;
extern HQL_API _ATOM successAtom;
extern HQL_API _ATOM supportsImportAtom;
extern HQL_API _ATOM supportsScriptAtom;
extern HQL_API _ATOM sysAtom;
extern HQL_API _ATOM tempAtom;
extern HQL_API _ATOM templateAtom;
extern HQL_API _ATOM terminateAtom;
extern HQL_API _ATOM terminatorAtom;
extern HQL_API _ATOM escapeAtom;
extern HQL_API _ATOM thorAtom;
extern HQL_API _ATOM thresholdAtom;
extern HQL_API _ATOM timeoutAtom;
extern HQL_API _ATOM timeLimitAtom;
extern HQL_API _ATOM timestampAtom;
extern HQL_API _ATOM tinyAtom;
extern HQL_API _ATOM tomitaAtom;
extern HQL_API _ATOM topAtom;
extern HQL_API _ATOM trimAtom;
extern HQL_API _ATOM trueAtom;
extern HQL_API _ATOM typeAtom;
extern HQL_API _ATOM _uid_Atom;
extern HQL_API _ATOM unnamedAtom;
extern HQL_API _ATOM unknownAtom;
extern HQL_API _ATOM unknownSizeFieldAtom;
extern HQL_API _ATOM unicodeAtom;
extern HQL_API _ATOM unorderedAtom;
extern HQL_API _ATOM unsortedAtom;
extern HQL_API _ATOM unstableAtom;
extern HQL_API _ATOM updateAtom;
extern HQL_API _ATOM userMatchFunctionAtom;
extern HQL_API _ATOM valueAtom;
extern HQL_API _ATOM versionAtom;
extern HQL_API _ATOM virtualAtom;
extern HQL_API _ATOM _virtualSeq_Atom;
extern HQL_API _ATOM volatileAtom;
extern HQL_API _ATOM warningAtom;
extern HQL_API _ATOM wholeAtom;
extern HQL_API _ATOM widthAtom;
extern HQL_API _ATOM wipeAtom;
extern HQL_API _ATOM _workflow_Atom;
extern HQL_API _ATOM _workflowPersist_Atom;
extern HQL_API _ATOM workunitAtom;
extern HQL_API _ATOM wuidAtom;
extern HQL_API _ATOM xmlAtom;
extern HQL_API _ATOM xmlDefaultAtom;
extern HQL_API _ATOM xpathAtom;

inline bool isInternalAttributeName(_ATOM name) { return (name->str()[0] == '$'); }

//Information which is associated with system attributes.
enum
{
    EAnone,
    EArecordCount,
    EAdiskserializedForm,
    EAinternalserializedForm,
    EAsize,
    EAaligned,
    EAunadorned,
    EAlocationIndependent,
    EAmax
};

//Use a different class implementation for system attributes, so I can associate extra internal information with them
//e.g., an efficient way of mapping from an internal id to an attribute enumeration, so the switch statements are efficient
class HQL_API SysAtom : public Atom
{
public:
    SysAtom(const void * k);

    inline SysAtom & setAttrId(byte _attrId) { attrId = _attrId; return *this; }
public:
    byte attrId;
};

extern HQL_API _ATOM createSystemAtom(const char * s);

inline byte getAttributeId(_ATOM x) { return static_cast<SysAtom *>(x)->attrId; }

/*
 * This is part of an experiment to make identifiers in the language case sensitive - at least optionally, possibly only for syntax checking.
 * It would require
 * a) Case sensitive hash table.
 * b) All attributes to be created with a consistent case.  A couple of issues may occur with attributes on services and others.
 * c) All identifier atoms need to be created through the functions below (to allow optional sensitivity)
 */

inline bool identifiersAreCaseSensitive() { return false; }
inline _ATOM createIdentifierAtom(const char * name) { return createAtom(name); }
inline _ATOM createIdentifierAtom(const char * name, unsigned len) { return createAtom(name, len); }

#endif

