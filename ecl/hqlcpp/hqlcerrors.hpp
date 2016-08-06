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
#ifndef HQLCERRORS_HPP
#define HQLCERRORS_HPP

#include "jexcept.hpp"
#include "hqlerrors.hpp"

/* Code Generation errors - defined in hqlcerrors.hpp */
#define ERR_CODEGEN_FIRST       4000
#define ERR_CODEGEN_LAST        4999

#define HQLERR_NullSetCannotGenerate            4000
#define HQLERR_NoMultiDimIndex                  4001
#define HQLERR_AllPassedExternal                4002
#define HQLERR_IndexAllSet                      4003
#define HQLERR_CannotInstantiateAll             4004
#define HQLERR_TooManyParameters                4005
#define HQLERR_ColumnUnknownLength              4006
#define HQLERR_BuildIndexVarLength              4007
#define HQLERR_BuildIndexVarLengthDs            4008
#define HQLERR_BadTypeInIndex                   4009
#define HQLERR_IndexNotValid                    4010
#define HQLERR_JoinConditionNoMatchIndex        4011
#define HQLERR_FilepositionInPersist            4012
#define HQLERR_PersistValueDiffDefinition       4013
#define HQLERR_LimitNeedsDataset                4014
#define HQLERR_CannotResourceActivity           4015
#define HQLERR_UnsupportedHashWorkunit          4016
#define HQLERR_UnknownVirtualAttr               4017
#define HQLERR_IllegalPattern                   4018
#define HQLERR_VarSizeSortUseThor               4020
#define HQLERR_SubstringOutOfRange              4021
#define HQLERR_RankOnStored                     4022
#define HQLERR_CastInfiniteString               4023
#define HQLERR_TooFewParameters                 4024
#define HQLERR_IndexTypeNotSupported            4025
#define HQLERR_RankOnNull                       4026
#define HQLERR_MatchedUsedOutsideParse          4027
#define HQLERR_BadMatchedPath                   4028
#define HQLERR_RoxieExpectedConstantFilename    4029
#define HQLERR_MatchTextNotUnicode              4030
#define HQLERR_MatchUnicodeNotText              4031
#define HQLERR_MatchTextOrUnicode               4032
#define HQLERR_ChildDatasetInOutput             4033
#define HQLERR_DefineUseStrNotFound             4034
#define HQLERR_DefineUseXYNotFound              4035
#define HQLERR_DuplicateStoredDefinition        4036
#define HQLERR_MismatchRowDiffType              4037
#define HQLERR_FetchNotSupportMode              4038
#define HQLERR_CounterNotValid                  4039
#define HQLERR_XmlTextNotValid                  4040
#define HQLERR_XmlUnicodeNotValid               4041
#define HQLERR_MustMatchExactly                 4042
#define HQLERR_RowTooLarge                      4043
#define HQLERR_ShouldHaveBeenHoisted            4044
#define HQLERR_NoArgumentsInValidator           4045

#define HQLERR_InputMergeNotSorted              4047
#define HQLERR_TooComplicatedToPreload          4048
#define HQLERR_KeyedNotKeyed                    4049
#define HQLERR_KeyedFollowsGap                  4050
#define HQLERR_WildFollowsGap                   4051
#define HQLERR_KeyedWildNoIndex                 4052
#define HQLERR_ZeroLengthIllegal                4053
#define HQLERR_SetCastNotSupported              4054
#define HQLERR_FuncNotInGlobalContext           4055
#define HQLERR_SetUnknownLength                 4056
#define HQLERR_HashStoredTypeMismatch           4057
#define HQLERR_CountAllSet                      4058
#define HQLERR_OnlyKeyFixedField                4059
#define HQLERR_DuplicateStoredDiffType          4060
#define HQLERR_RegexFeatureNotSupport           4061
#define HQLERR_UnsupportedAttribute             4062
#define HQLERR_GroupTableNotYetSupported        4063
#define HQLERR_ChildQueriesNotSupported         4064
#define HQLERR_HashStoredRecordMismatch         4066
#define HQLERR_BadJoinConditionAtMost           4067
#define HQLERR_BadKeyedJoinConditionAtMost      4068
#define HQLERR_FullJoinNeedDataset              4069
#define HQLERR_KeyedLimitNotKeyed               4071
#define HQLERR_ExtendMismatch                   4072
#define HQLERR_DuplicateNameOutput              4073
#define HQLERR_ExtendTypeMismatch               4074
#define HQLERR_OverwriteMismatch                4075
#define HQLERR_ExtendOverwriteMismatch          4076
#define HQLERR_EmbeddedCppNotAllowed            4078
#define HQLERR_ContentsInSoapCall               4079
#define HQLERR_FullKeyedNeedsFile               4081
#define HQLERR_ExpectedConstant                 4082
#define HQLERR_AccessRowBlobInsideChildQuery    4083
#define HQLERR_NoDatasetInCsvOutput             4084
#define HQLERR_NoSetInCsvOutput                 4085
#define HQLERR_CouldNotDetermineMaxSize         4086
#define HQLERR_CannotDetermineSizeVar           4087
#define HQLERR_DuplicateDefinition              4088
#define HQLERR_DuplicateDefinitionDiffType      4089
#define HQLERR_WildNotReferenceIndex            4090
#define HQLERR_InconsistentKeyedOpt             4091
#define HQLERR_OptKeyedFollowsWild              4092
#define HQLERR_KeyedCountCantNormalize          4093
#define HQLERR_KeyedCountNotKeyed               4094
#define HQLERR_KeyedCountNonKeyable             4095
#define HQLERR_LookupNotActiveDataset           4096
#define HQLERR_KeyedJoinTooComplex              4097
#define HQLERR_KeyAccessNeedCast                4098
#define HQLERR_KeyAccessNoKeyField              4099
#define HQLERR_NotSupportedInsideNoThor         4102
#define HQLERR_RegexNoTransformSupport          4103
#define HQLERR_AccessMatchAttrInChildQuery      4104
#define HQLERR_ExpectedTransformManyInputs      4105
#define HQLERR_AmbiguousLeftRight               4106
#define HQLERR_RhsKeyedNotKey                   4107
#define HQLERR_RemoteNoMeaning                  4108
#define HQLERR_RemoteGrouped                    4109
#define HQLERR_LibraryCannotContainWorkflow     4110
#define HQLERR_LoopTooComplexForParallel        4111
#define HQLERR_BadDfaOperator                   4112
#define HQLERR_MatchUtf8NotText                 4113
#define HQLERR_ThisNodeNotInsideAllNodes        4114
#define HQLERR_SteppedNoJoin                    4115
#define HQLERR_UnsupportedRowsetRangeParam      4116
#define HQLERR_CantProjectStepping              4117
#define HQLERR_SteppedVariableSize              4118
#define HQLERR_SteppedVariableOffset            4119
#define HQLERR_TooComplexToStep                 4120
#define HQLERR_NoSteppingOnPayload              4121
#define HQLERR_SteppedRangeOnlyOneDirection     4122
#define HQLERR_SteppedMultiRange                4123
#define HQLERR_SteppingNotMatchSortCondition    4124
#define HQLERR_JoinNotMatchSortCondition        4125
#define HQLERR_StepFieldNotKeyed                4126
#define HQLERR_StepFieldNotContiguous           4127
#define HQLERR_SortOrderMustMatchJoinFields     4128
#define HQLERR_OrMultipleKeyfields              4129
#define HQLERR_RowCompressRequireFixedSize      4130
#define HQLERR_InputsAreTooComplexToUpdate      4131
#define HQLERR_ThorDenormOnlyLeftOuterJoin      4132
#define HQLERR_ThorDenormNoFeatureX             4133
#define HQLERR_SkipNotValidHere                 4134
#define HQLERR_CsvNotSupportTableSet            4135
#define HQLERR_ExpectedConstantWorkunit         4136
#define HQLERR_ExpectedConstantDebug            4137
#define HQLERR_LibraryMemberArgNotDefined       4138
#define HQLERR_ConcreteMemberRequired           4139
#define HQLERR_ThorHavingMustBeGrouped          4140
#define HQLERR_XNotSupportedInsideChild         4141
#define HQLERR_LibrariesCannotContainSideEffects 4142
#define HQLERR_StarRangeOnlyInJoinCondition     4143
#define HQLERR_KeyedDistributeNoSubstringJoin   4144
#define HQLERR_MultipleStepped                  4145
#define HQLERR_RecursiveStored                  4146
#define HQLERR_RecursiveStoredOther             4146
#define HQLERR_PipeNotAllowed                   4147
#define HQLERR_CannotDeduceNameDataset          4148
#define HQLERR_ExpectedUpdateVariable           4149
#define HQLERR_FailedToLoadSystemModule         4150
#define HQLERR_RankOutOfRange                   4152
#define HQLERR_DatasetNotActive                 4153
#define HQLERR_CouldNotOpenTemplateXatY         4154
#define HQLERR_CouldNotCreateOutputX            4155
#define HQLERR_XDoesNotContainExpressionY       4156
#define HQLERR_InvalidSetResultType             4157
#define HQLERR_CouldNotFindDataset              4158
#define HQLERR_CouldNotAnyDatasetX              4159
#define HQLERR_NestedThorNodes                  4160
#define HQLERR_MissingTransformAssignXX         4161
#define HQLERR_JoinXTooComplex                  4162
#define HQLERR_GlobalDedupFuzzy                 4163
#define HQLERR_GlobalDedupNoEquality            4164
#define HQLERR_HashDedupNotSupportX             4165
#define HQLERR_JoinSortedMustBeDataset          4166
#define HQLERR_JoinSortedMustBeThor             4167
#define HQLERR_SortAndCoSortConcurrent          4168
#define HQLERR_WaitNotSupported                 4169
#define HQLERR_IndexHasActiveFields             4170
#define HQLERR_GlobalHasActiveFields            4171
#define HQLERR_ResourceCreateFailed             4172
#define HQLERR_RecordNotCompatible              4173
#define HQLERR_MissingTemplateTerminator        4174
#define HQLERR_FailXUsedOutsideFailContext      4175
#define HQLERR_GlobalSideEffectDependent        4176
#define HQLERR_AggregateNeedMergeTransform      4177
#define HQLERR_AggregateMissingGroupingFields   4178
#define HQLERR_MaximumRowSizeOverflow           4179
#define HQLERR_OpArgDependsDataset              4180
#define HQLERR_CouldNotDetermineMinSize         4181
#define HQLERR_OutsideGroupAggregate            4182
#define HQLERR_ResourceAddAfterFinalManifest    4183
#define HQLERR_SkipInsideCreateRow              4184
#define HQLERR_KeyedJoinNoRightIndex_X          4185
#define HQLERR_ScalarOutputWithinApply          4186
#define HQLERR_EmbeddedTypeNotSupported_X       4187
#define HQLERR_MaximumSizeLessThanMinimum_XY    4188
#define HQLERR_UnexpectedOptionValue_XY         4189
#define HQLERR_VariableRowMustBeLinked          4190
#define HQLERR_UserCodeNotAllowed               4191
#define HQLERR_StreamInputUsedDirectly          4192
#define HQLERR_MaxlengthExceedsLimit            4193
#define HQLERR_CouldNotGenerateDefault          4194
#define HQLERR_DistributionVariableLengthX      4195
#define HQLERR_DistributionUnsupportedTypeXX    4196
#define HQLERR_InconsistentEmbedded             4197
#define HQLERR_UnsupportedRowDiffType           4198
#define HQLERR_EmbedParamNotSupportedInOptions  4199
#define HQLERR_InvalidXmlnsPrefix               4200

//Warnings....
#define HQLWRN_PersistDataNotLikely             4500
#define HQLWRN_CaseCanNeverMatch                4501
#define HQLWRN_NoWorkflowNoWaitOrNotify         4502
#define HQLWRN_NoWorkflowNoWhen                 4503
#define HQLWRN_NoWorkflowNoSuccess              4504
#define HQLWRN_ScheduleOmitted                  4505
#define HQLWRN_UnknownEventName                 4506
#define HQLWRN_UnknownEventShortcut             4507
#define HQLWRN_IgnoringWaitOrNotify             4508
#define HQLWRN_CannotRecreateDistribution       4509
#define HQLWRN_RecursiveDependendencies         4510
#define HQLWRN_MaxSizeExceedsSafeLimit          4512
#define HQLWRN_TomitaMatchPattern               4513
#define HQLWRN_KeyedFollowsGap                  4515
#define HQLWRN_LocalHasNoEffect                 4516
#define HQLWRN_CsvMaxLengthMismatch             4517
#define HQLWRN_KeyedFoldedToGap                 4518
#define HQLWRN_FoldRemoveKeyed                  4519
#define HQLWRN_GlobalDoesntSeemToBe             4520
#define HQLWRN_GroupedJoinIsLookupJoin          4521
#define HQLWRN_ImplicitJoinLimit                4522
#define HQLWRN_ImplicitReadLimit                4523
#define HQLWRN_ImplicitReadAddLimit             4524
#define HQLWRN_LimitAlwaysExceeded              4525
#define HQLWRN_LimitAlwaysExceededX             4526
#define HQLWRN_KeyedLimitIsZero                 4527
#define HQLWRN_LimitIsZero                      4528
#define HQLWRN_MoxieNoBias                      4529
#define HQLWRN_ParseVeryLargeDefinition         4530
#define HQLWRN_JoinConditionFoldedNowAll        4531
#define HQLWRN_MergeBadSortOrder                4532
#define HQLWRN_OutputDependendOnScope           4533
#define HQLWRN_OptionSetToLate                  4534
#define HQLWRN_WorkflowSeemsToBeDependent       4535
#define HQLWRN_GlobalSideEffectDependent        4536
#define HQLWRN_GrammarIsAmbiguous               4537
#define HQLWRN_ComplexHelperClass               4538
#define HQLWRN_TryAddingIndependent             4539
#define HQLWRN_GroupedGlobalFew                 4540
#define HQLWRN_AmbiguousRollupCondition         4541
#define HQLWRN_AmbiguousRollupNoGroup           4542
#define HQLWRN_GlobalActionDependendOnScope     4543
#define HQLWRN_NoThorContextDependent           4544
#define HQLWRN_OnlyLocalMergeJoin               4545
#define HQLWRN_WorkflowDependParameter          4546
#define HQLWRN_OutputScalarInsideChildQuery     4547

//Temporary errors
#define HQLERR_OrderOnVarlengthStrings          4601
#define HQLERR_DistributionNoSequence           4604
#define HQLERR_PhysicalJoinTooComplex           4606
#define HQLERR_ArbitaryRepeatUnimplemented      4609
#define HQLERR_TomitaNoUnicode                  4610
#define HQLERR_TomitaPatternTooComplex          4611
#define HQLERR_CannotCreateSizedChildDataset    4613
#define HQLERR_OnlyNormalizeSimpleChildren      4614
#define HQLERR_RoxieLocalNotSupported           4615
#define HQLERR_MinusOnString                    4616
#define HQLERR_ThorNotSupportStepping           4617
#define HQLERR_OnceCannotAccessStored           4618
#define HQLERR_ThorCombineOnlyLocal             4619
#define HQLERR_SteppedNotImplemented            4620

//Internal errors....
#define HQLERR_NoClearOnLocalDataset            4800
#define HQLERR_NoCreateLocalDataset             4801
#define HQLERR_TableOutsideThor                 4802
#define HQLERR_ExtraAssignInTransform           4803
#define HQLERR_CannotAccessStoredVariable       4804
#define HQLERR_EvaluateTableNotInScope          4805
#define HQLERR_FuncNotFound                     4806
#define HQLERR_InternalOutputScalar             4807
#define HQLERR_DfaTooComplex                    4808
#define HQLERR_UnsupportedInlineQuery           4809
#define HQLERR_CouldNotResolveFileposition      4813
#define HQLERR_CastXNotImplemented              4814
#define HQLERR_MatchedContextNotFound           4815
#define HQLERR_BlobTranslationContextNotFound   4816
#define HQLERR_RowsUsedOutsideContext           4817
#define HQLERR_ExpectedParentContext            4818
#define HQLERR_CounterNotFound                  4820
#define HQLERR_GraphContextNotFound             4821
#define HQLERR_InvalidAcessStoredVariable       4822
#define HQLERR_LibraryNoWorkunitRead            4823
#define HQLERR_LibraryNoWorkunitWrite           4824
#define HQLERR_GraphInputAccessedChild          4825
#define HQLERR_InconsisentLocalisation          4826
#define HQLERR_NoParentExtract                  4827
#define HQLERR_LibraryMustBeFunctional          4828
#define HQLERR_InconsistentNaryInput            4829
#define HQLERR_LinkedDatasetNoContext           4830
#define HQLERR_InternalError                    4833
#define HQLERR_CannotGenerateSerializedCompare  4834
#define HQLERR_ReadSpillBeforeWrite             4835
#define HQLERR_DependencyWithinGraph            4836
#define HQLERR_UnknownCompoundAssign            4837
#define HQLERR_ReadSpillBeforeWriteFix          4838
#define HQLERR_AccessUnavailableGraph           4839
#define HQLERR_NoMappingForField                4840
//#define HQLERR_Max                            4999

//---- Text for all errors (make it easy to internationalise) ---------------------------

#define HQLERR_NullSetCannotGenerate_Text       "INTERNAL: Cannot generate code for an empty set in this context"
#define HQLERR_NoMultiDimIndex_Text             "Multi dimensioned sets are not yet supported"
#define HQLERR_AllPassedExternal_Text           "ALL cannot be passed to an external service"
#define HQLERR_IndexAllSet_Text                 "Indexing ALL is undefined"
#define HQLERR_CannotInstantiateAll_Text        "Cannot use ALL in this context"
#define HQLERR_TooManyParameters_Text           "Too many parameters passed to function '%s'"
#define HQLERR_ColumnUnknownLength_Text         "Cannot define column %s with an unknown length (use alien datatype)"
#define HQLERR_BuildIndexVarLength_Text         "BUILDINDEX can only be used to build fixed width indexes"
#define HQLERR_BuildIndexVarLengthDs_Text       "BUILDINDEX can only be used to build indexes on fixed width files"
#define HQLERR_BadTypeInIndex_Text              "INDEX does not support fields of type %s"
#define HQLERR_IndexNotValid_Text               "INDEX does not match the dataset being joined"
#define HQLERR_JoinConditionNoMatchIndex_Text   "Join condition does not contain sufficient information to use key"
#define HQLERR_FilepositionInPersist_Text       "Not yet implemented:  Cannot use a record structure containing a virtual field (%s) with stored or persist"
#define HQLERR_PersistValueDiffDefinition_Text  "%s has more than one definition"
#define HQLERR_LimitNeedsDataset_Text           "LIMIT clause can only be applied to a dataset"
#define HQLERR_CannotResourceActivity_Text      "Cannot resource activity %s a cluster with %d nodes"
#define HQLERR_UnsupportedHashWorkunit_Text     "Unsupported option #WORKUNIT ('%s')"
#define HQLERR_UnknownVirtualAttr_Text          "INTERNAL: Unsupported virtual attribute '%s'"
#define HQLERR_IllegalPattern_Text              "Illegal pattern '%s..%s'"
#define HQLERR_VarSizeSortUseThor_Text          "THOR must be used for sorting or joining datasets with variable width rows"
#define HQLERR_SubstringOutOfRange_Text         "Substring index %d is outside the field range"
#define HQLERR_RankOnStored_Text                "RANK/RANKED not supported on list %s"
#define HQLERR_CastInfiniteString_Text          "Cannot cast a string of unknown length to another character set"
#define HQLERR_TooFewParameters_Text            "Not enough parameters passed to function '%s'"
#define HQLERR_IndexTypeNotSupported_Text       "Index is not supported for type %s yet"
#define HQLERR_RankOnNull_Text                  "RANK has no meaning on an empty list"
#define HQLERR_MatchedUsedOutsideParse_Text     "%s can only be used in a record supplied to a PARSE() command"
#define HQLERR_BadMatchedPath_Text              "The parameter to MATCHED(%s) is not found in the pattern"
#define HQLERR_RoxieExpectedConstantFilename_Text "Roxie requires constant filenames - expression %s cannot be computed at deployment time"
#define HQLERR_MatchTextNotUnicode_Text         "MATCHTEXT found where MATCHUNICODE was expected"
#define HQLERR_MatchUnicodeNotText_Text         "MATCHUNICODE found where MATCHTEXT was expected"
#define HQLERR_MatchTextOrUnicode_Text          "Only MATCHTEXT and MATCHUNICODE are valid inside a VALIDATE"
#define HQLERR_ChildDatasetInOutput_Text        "Records containing child datasets must be output to a file"
#define HQLERR_DefineUseStrNotFound_Text        "Definition of USE(%s) was not found"
#define HQLERR_DefineUseXYNotFound_Text         "Definition of USE(%s.%s) was not found"
#define HQLERR_DuplicateStoredDefinition_Text   "Duplicate definition of %s (use #stored to override default value)"
#define HQLERR_MismatchRowDiffType_Text         "ROWDIFF: Types are not compatible for field %s"
#define HQLERR_FetchNotSupportMode_Text         "FETCH not supported on dataset of kind %s"
#define HQLERR_CounterNotValid_Text             "COUNTER is not legal in this context"
#define HQLERR_XmlTextNotValid_Text             "XMLTEXT is only legal in a PARSE transform"
#define HQLERR_XmlUnicodeNotValid_Text          "XMLUNICODE is only legal inside a PARSE transform"
#define HQLERR_MustMatchExactly_Text            "Condition on DISTRIBUTE must match the key exactly"
#define HQLERR_RowTooLarge_Text                 "Row size %u exceeds the maximum specified (%u)"
#define HQLERR_ShouldHaveBeenHoisted_Text       "Select expression should have been hoisted"
#define HQLERR_NoArgumentsInValidator_Text      "%s() cannot have a parameter inside a VALIDATE"
#define HQLERR_InputMergeNotSorted_Text         "Input to MERGE does not appear to be sorted"
#define HQLERR_TooComplicatedToPreload_Text     "Expression is too complicated to preload"
#define HQLERR_KeyedNotKeyed_Text               "KEYED(%s) could not be looked up in a key."
#define HQLERR_KeyedFollowsGap_Text             "KEYED(%s) follows unfiltered component %s in the key%s"
#define HQLERR_WildFollowsGap_Text              "WILD(%s) follows unfiltered component %s in the key%s"
#define HQLERR_KeyedWildNoIndex_Text            "%s could not be implemented by the key"
#define HQLERR_ZeroLengthIllegal_Text           "Cannot process zero length rows"
#define HQLERR_SetCastNotSupported_Text         "Set casts are not supported yet"
#define HQLERR_FuncNotInGlobalContext_Text      "Cannot call function %s in a non-global context"
#define HQLERR_SetUnknownLength_Text            "Sets of items of unknown length are not yet supported!"
#define HQLERR_HashStoredTypeMismatch_Text      "#STORED (%s) type mismatch (was '%s' replacement '%s')"
#define HQLERR_CountAllSet_Text                 "Cannot count number of elements in ALL"
#define HQLERR_OnlyKeyFixedField_Text           "Can only key fixed fields at fixed offsets"
#define HQLERR_DuplicateStoredDiffType_Text     "Duplicate definition of %s with different type (use #stored to override default value)"
#define HQLERR_RegexFeatureNotSupport_Text      "Features are not supported by regex - did you mean repeat() instead of {}?"
#define HQLERR_UnsupportedAttribute_Text        "Option %s not yet supported on child datasets"
#define HQLERR_GroupTableNotYetSupported_Text   "Grouped tables not yet supported in this context"
#define HQLERR_ChildQueriesNotSupported_Text    "Nested child queries are not supported yet"
#define HQLERR_HashStoredRecordMismatch_Text    "#STORED (%s) records must match"
#define HQLERR_BadJoinConditionAtMost_Text      "ATMOST JOIN cannot be evaluated with this join condition%s"
#define HQLERR_BadKeyedJoinConditionAtMost_Text "ATMOST JOIN cannot be evaluated with this join condition%s"
#define HQLERR_FullJoinNeedDataset_Text         "RIGHT for a full keyed join must be a disk based DATASET"
#define HQLERR_KeyedLimitNotKeyed_Text          "LIMIT(%s, KEYED) could not be merged into an index read"
#define HQLERR_ExtendMismatch_Text              "EXTEND is required on all outputs to NAMED(%s)"
#define HQLERR_DuplicateNameOutput_Text         "Duplicate output to NAMED(%s).  EXTEND/OVERWRITE required"
#define HQLERR_ExtendTypeMismatch_Text          "OUTPUTs to NAMED(%s) have incompatible types"
#define HQLERR_OverwriteMismatch_Text           "OVERWRITE is required on all outputs to NAMED(%s)"
#define HQLERR_ExtendOverwriteMismatch_Text     "OVERWRITE/EXTEND should be consistent on all outputs to NAMED(%s)"
#define HQLERR_EmbeddedCppNotAllowed_Text       "Insufficient access rights to use embedded C++"
#define HQLERR_ContentsInSoapCall_Text          "Tag contents syntax <> is not supported by SOAPCALL"
#define HQLERR_FullKeyedNeedsFile_Text          "RIGHT side of a full keyed join must be a disk file"
#define HQLERR_ExpectedConstant_Text            "Expression is not constant: %s"
#define HQLERR_AccessRowBlobInsideChildQuery_Text "Unimplemented: Cannot access row blob inside a child query, contact tech support"
#define HQLERR_NoDatasetInCsvOutput_Text        "Field '%s' with dataset type not supported in csv output"
#define HQLERR_NoSetInCsvOutput_Text            "Field '%s' with set type not supported in csv output"
#define HQLERR_CouldNotDetermineMaxSize_Text    "Cannot determine the maximum size of the expression"
#define HQLERR_CannotDetermineSizeVar_Text      "Cannot determine size because variable size dataset is not in scope.  Try using sizeof(x,max)"
#define HQLERR_DuplicateDefinition_Text         "Duplicate definition of %s"
#define HQLERR_DuplicateDefinitionDiffType_Text "Duplicate definition of %s with different type"
#define HQLERR_WildNotReferenceIndex_Text       "WILD() does not reference fields in key %s"
#define HQLERR_InconsistentKeyedOpt_Text        "Field %s cannot have both KEYED and KEYED,OPT conditions"
#define HQLERR_OptKeyedFollowsWild_Text         "KEYED(%s,OPT) follows a WILD() field in key %s"
#define HQLERR_KeyedCountCantNormalize_Text     "COUNT(,KEYED) cannot be used on a child dataset"
#define HQLERR_KeyedCountNotKeyed_Text          "Filter for COUNT(,KEYED) did not contained KEYED() expressions"
#define HQLERR_KeyedCountNonKeyable_Text        "KEYED COUNT used on a non-keyable dataset"
#define HQLERR_LookupNotActiveDataset_Text      "Attempting to lookup field %s in a dataset which has no active element"
#define HQLERR_KeyedJoinTooComplex_Text         "Key condition (%s) is too complex, it cannot be done with the key."
#define HQLERR_KeyAccessNeedCast_Text           "Key condition (%s) requires casts on comparison of field '%s'"
#define HQLERR_KeyAccessNoKeyField_Text         "Key condition (%s) does not have any comparisons against key fields"
#define HQLERR_MinusOnString_Text               "unary - cannot be performed on a string"
#define HQLERR_NotSupportedInsideNoThor_Text    "%s is not supported inside NOTHOR()"
#define HQLERR_RegexNoTransformSupport_Text     "Regular expression parsing does not support productions - need to use tomita"
#define HQLERR_AccessMatchAttrInChildQuery_Text "Unimplemented: Cannot yet access $<n> inside a child query"
#define HQLERR_ExpectedTransformManyInputs_Text "Ambiguous default production for rule with multiple inputs"
#define HQLERR_AmbiguousLeftRight_Text          "Selector %s is used ambiguously at multiple levels"
#define HQLERR_RhsKeyedNotKey_Text              "JOIN%s marked as KEYED does not have a key as the second parameter"
#define HQLERR_RemoteNoMeaning_Text             "ALLNODES(dataset) only legal in roxie and in thor child queries"
#define HQLERR_RemoteGrouped_Text               "ALLNODES() is not currently supported on grouped datasets"
#define HQLERR_LibraryCannotContainWorkflow_Text "Library %s cannot contain any workflow actions%s"
#define HQLERR_LoopTooComplexForParallel_Text   "LOOP body too complicated to evaluate in parallel"
#define HQLERR_BadDfaOperator_Text              "Operator %s not supported inside DFAs"
#define HQLERR_MatchUtf8NotText_Text            "MATCHUTF8 found where MATCHUNICODE/MATCHTEXT was expected"
#define HQLERR_ThisNodeNotInsideAllNodes_Text   "THISNODE() can only be used inside ALLNODES()"
#define HQLERR_SteppedNoJoin_Text               "STEPPED(%s) cannot be evaluated in this context"
#define HQLERR_UnsupportedRowsetRangeParam_Text "Parameter to RANGE() is too complex - not currently supported"
#define HQLERR_CantProjectStepping_Text         "Project of stepping fields is too complex"
#define HQLERR_SteppedVariableSize_Text         "Stepped field %s must have a fixed size"
#define HQLERR_SteppedVariableOffset_Text       "Stepped field %s must be at a fixed offset"
#define HQLERR_TooComplexToStep_Text            "Expression is too complex to STEP (%s)"
#define HQLERR_NoSteppingOnPayload_Text         "Cannot smart step on payload fields"
#define HQLERR_SteppedRangeOnlyOneDirection_Text "STEPPED range condition could only be matched in one direction"
#define HQLERR_SteppedMultiRange_Text           "Multiple STEPPED comparisons %s >= "
#define HQLERR_SteppingNotMatchSortCondition_Text "STEPPED condition is not compatible with the sort/merge condition (%s expected)"
#define HQLERR_JoinNotMatchSortCondition_Text   "JOIN condition is not compatible with the sort/merge condition"
#define HQLERR_StepFieldNotKeyed_Text           "STEPPED field %s is not keyed"
#define HQLERR_StepFieldNotContiguous_Text      "STEPPED field %s does not follow the previous stepped field"
#define HQLERR_SortOrderMustMatchJoinFields_Text "Merge order must match all the stepped join fields"
#define HQLERR_OrMultipleKeyfields_Text         "Cannot OR together conditions on multiple key fields (%s)"
#define HQLERR_RowCompressRequireFixedSize_Text "ROW compression can only be used on fixed size indexes"
#define HQLERR_InputsAreTooComplexToUpdate_Text "UPDATE cannot be used when the inputs names are not globally constant"
#define HQLERR_ThorDenormOnlyLeftOuterJoin_Text "THOR currently only supports LEFT OUTER denormalize"
#define HQLERR_ThorDenormNoFeatureX_Text        "THOR does not support DENORMALIZE(%s)"
#define HQLERR_SkipNotValidHere_Text            "SKIP cannot be used here.  It is only valid directly with a transform"
#define HQLERR_CsvNotSupportTableSet_Text       "Cannot read tables/datasets from a csv file"
#define HQLERR_ExpectedConstantWorkunit_Text    "Argument %s to #workunit must be a constant"
#define HQLERR_ExpectedConstantDebug_Text       "Argument %s to #debug must be a constant"
#define HQLERR_LibraryMustBeFunctional_Text     "Queries libraries must be implemented with a parameterised module"
#define HQLERR_LibraryMemberArgNotDefined_Text  "Member %s not defined in module passed as library parameter"
#define HQLERR_ConcreteMemberRequired_Text      "Member %s was undefined in a module"
#define HQLERR_ThorHavingMustBeGrouped_Text     "Thor does not support HAVING on a non-grouped dataset"
#define HQLERR_XNotSupportedInsideChild_Text    "%s not currently supported as a child action"
#define HQLERR_LibrariesCannotContainSideEffects_Text "Definitions in libraries cannot have side effects (%s)"
#define HQLERR_MultipleStepped_Text             "Index read only supports single STEPPED expression"
#define HQLERR_RecursiveStored_Text             "#%s (%s) creates an illegal recursive reference"
#define HQLERR_RecursiveStoredOther_Text        "#%s (%s) creates an illegal recursive reference (%s also being processed)"
#define HQLERR_PipeNotAllowed_Text              "Insufficient access rights to use PIPE"
#define HQLERR_CannotDeduceNameDataset_Text     "Parameter to __nameof__ must be a dataset or an index"
#define HQLERR_ExpectedUpdateVariable_Text      "Expected a UPDATE attribute name"
#define HQLERR_FailedToLoadSystemModule_Text    "%s @ %d:%d"
#define HQLERR_RankOutOfRange_Text              "Index in RANK/RANKED is out of range"
#define HQLERR_DatasetNotActive_Text            "INTERNAL: Dataset is not active: '%s'"
#define HQLERR_CouldNotOpenTemplateXatY_Text    "Could not open thor template '%s' at '%s'"
#define HQLERR_CouldNotCreateOutputX_Text       "Could not create output '%s'"
#define HQLERR_XDoesNotContainExpressionY_Text  "Dataset '%.80s' does not contain expression '%.50s'"
#define HQLERR_InvalidSetResultType_Text        "Cannot return a result of this type from a workunit"
#define HQLERR_CouldNotFindDataset_Text         "Could not find dataset %s"
#define HQLERR_CouldNotAnyDatasetX_Text         "Could not find dataset %s (no tables in scope)"
#define HQLERR_NestedThorNodes_Text             "INTERNAL: Thor nodes should not be nested"
#define HQLERR_MissingTransformAssignXX_Text    "INTERNAL: Missing assignment from transform to %s[%p]"
#define HQLERR_JoinXTooComplex_Text             "JOIN%s contains no equality conditions - use ,ALL to allow"
#define HQLERR_GlobalDedupFuzzy_Text            "A global DEDUP(ALL) or local hash dedup cannot include comparisons in the dedup criteria"
#define HQLERR_GlobalDedupNoEquality_Text       "Global dedup,ALL must have a field to partition"
#define HQLERR_HashDedupNotSupportX_Text        "Hash dedup does not support %s"
#define HQLERR_JoinSortedMustBeDataset_Text     "SORTED() used by JOINED must be applied to a DATASET"
#define HQLERR_JoinSortedMustBeThor_Text        "SORTED() used by JOINED must be applied to a THOR dataset"
#define HQLERR_SortAndCoSortConcurrent_Text     "SORT supplied to COSORT needs to be executed at the same time"
#define HQLERR_WaitNotSupported_Text            "WAIT not yet supported"
#define HQLERR_IndexHasActiveFields_Text        "Index has fields %s in scope"
#define HQLERR_GlobalHasActiveFields_Text       "Global dataset has fields %s in scope"
#define HQLERR_ResourceCreateFailed_Text        "Create resource library %s failed"
#define HQLERR_RecordNotCompatible_Text         "Records not assignment compatible"
#define HQLERR_MissingTemplateTerminator_Text   "Missing end of placeholder"
#define HQLERR_FailXUsedOutsideFailContext_Text "%s can only be globally or inside ONFAIL or other failure processing"
#define HQLERR_GlobalSideEffectDependent_Text   "Side-effect%s is not currently supported as a context-dependent dependency"
#define HQLERR_AggregateNeedMergeTransform_Text "Cannot deduce MERGE transform for global AGGREGATE"
#define HQLERR_AggregateMissingGroupingFields_Text "AGGREGATE does not include grouping field '%s' in the result record"
#define HQLERR_MaximumRowSizeOverflow_Text      "The calculated maximum row size has overflowed 32bits"
#define HQLERR_OpArgDependsDataset_Text         "%s: %s cannot be dependent on the dataset"
#define HQLERR_CouldNotDetermineMinSize_Text    "Cannot determine the minimum size of the expression"
#define HQLERR_OutsideGroupAggregate_Text       "%s used outside of a TABLE aggregation"
#define HQLERR_ResourceAddAfterFinalManifest_Text "%s resource added after manifest was finalized"
#define HQLERR_SkipInsideCreateRow_Text         "SKIP inside a ROW(<transform>) not supported.  It is only allowed in a DATASET transform."
#define HQLERR_ScalarOutputWithinApply_Text     "A scalar output within an APPLY is undefined and may fail.  Use OUTPUT(dataset,EXTEND) instead."
#define HQLERR_KeyedJoinNoRightIndex_X_Text     "Right dataset (%s) for a keyed join is not a key"
#define HQLERR_EmbeddedTypeNotSupported_X_Text  "Type %s not supported for embedded/external scripts"
#define HQLERR_MaximumSizeLessThanMinimum_XY_Text "Maximum size (%u) for this record is lower than the minimum (%u)"
#define HQLERR_UnexpectedOptionValue_XY_Text    "Unexpected value for option %s: %s"
#define HQLERR_VariableRowMustBeLinked_Text     "External function '%s' cannot return a non-linked variable length row"
#define HQLERR_UserCodeNotAllowed_Text          "Workunit-supplied code is not permitted on this system"
#define HQLERR_StreamInputUsedDirectly_Text     "Library input used directly in a child query"
#define HQLERR_UnsupportedRowDiffType_Text      "ROWDIFF: Does not support type '%s' for field %s"
#define HQLERR_EmbedParamNotSupportedInOptions_Text   "Cannot use bound parameter in embed options - try adding a FUNCTION wrapper"
#define HQLERR_InvalidXmlnsPrefix_Text          "Invalid XMLNS prefix: %s"

//Warnings.
#define HQLWRN_CannotRecreateDistribution_Text  "Cannot recreate the distribution for a persistent dataset"
#define HQLWRN_RecursiveDependendencies_Text    "Recursive filename dependency"
#define HQLWRN_MaxSizeExceedsSafeLimit_Text     "Maximum row size of %u exceeds the recommended maximum (%u)"
#define HQLWRN_TomitaMatchPattern_Text          "MATCHED(%s) will not work on a pattern"
#define HQLWRN_KeyedFollowsGap_Text             "keyed filter on %s follows unkeyed component %s in the key%s"
#define HQLWRN_LocalHasNoEffect_Text            "LOCAL(dataset) only has an effect in roxie and in thor child queries"
#define HQLWRN_CsvMaxLengthMismatch_Text        "CSV read: Max length of record (%d) exceeds the max length (%d) specified on the csv attribute"
#define HQLWRN_KeyedFoldedToGap_Text            "KEYED(%s) follows component %s which is always matched in the key%s"
#define HQLWRN_FoldRemoveKeyed_Text             "The key condition for field (%s) on key%s is always true"
#define HQLWRN_GlobalDoesntSeemToBe_Text        "Global expression%s appears to access a parent dataset - this may cause a dataset not active error"
#define HQLWRN_GroupedJoinIsLookupJoin_Text     "JOIN(,GROUPED) is implemented as a MANY LOOKUP join.  This may be inefficient in thor."
#define HQLWRN_ImplicitJoinLimit_Text           "Implicit LIMIT(%d) added to keyed join%s"
#define HQLWRN_ImplicitReadLimit_Text           "Neither LIMIT() nor CHOOSEN() supplied for index read on %s"
#define HQLWRN_ImplicitReadAddLimit_Text        "Implicit LIMIT(%d) added to index read %s"
#define HQLWRN_LimitAlwaysExceeded_Text         "Limit is always exceeded"
#define HQLWRN_LimitAlwaysExceededX_Text        "Limit '%s' is always exceeded"
#define HQLWRN_KeyedLimitIsZero_Text            "Keyed Limit(0) will fail if there are any records, is this intended?"
#define HQLWRN_LimitIsZero_Text                 "Limit(0) will fail if there are any records, is this intended?"
#define HQLWRN_MoxieNoBias_Text                 "Key will not work on moxie, because cannot deduce bias from variable width base file.  Need to add an explicit BIAS."
#define HQLWRN_ParseVeryLargeDefinition_Text    "PARSE pattern generates a large definition (%d bytes)"
#define HQLWRN_JoinConditionFoldedNowAll_Text   "JOIN condition folded to constant, converting to an ALL join"
#define HQLWRN_MergeBadSortOrder_Text           "MERGE() cannot recreated implicit sort order"
#define HQLWRN_OutputDependendOnScope_Text      "OUTPUT(%s) appears to be context dependent - this may cause a dataset not active error"
#define HQLWRN_OptionSetToLate_Text             "#option ('%s') will have no effect - it needs to be set in the submitted workunit."
#define HQLWRN_WorkflowSeemsToBeDependent_Text  "Workflow item%s seems to be context dependent"
#define HQLWRN_GlobalSideEffectDependent_Text   "Global side-effect%s seems to be context dependent - it may not function as expected"
#define HQLWRN_GrammarIsAmbiguous_Text          "The PARSE pattern for activity %d is ambiguous.  This may reduce the efficiency of the PARSE."
#define HQLWRN_ComplexHelperClass_Text          "Activity %d created a complex helper class (%d)"
#define HQLWRN_GroupedGlobalFew_Text            "Global few expression is grouped"
#define HQLWRN_AmbiguousRollupCondition_Text    "ROLLUP condition on '%s' is also modified in the transform"
#define HQLWRN_AmbiguousRollupNoGroup_Text      "ROLLUP condition - no fields are preserved in the transform - not converted to GROUPed ROLLUP"
#define HQLWRN_GlobalActionDependendOnScope_Text "Global action appears to be context dependent - this may cause a dataset not active error"
#define HQLWRN_NoThorContextDependent_Text      "NOTHOR expression%s appears to access a parent dataset - this may cause a dataset not active error"
#define HQLWRN_OnlyLocalMergeJoin_Text          "Only LOCAL versions of %s are currently supported on THOR"
#define HQLWRN_WorkflowDependParameter_Text     "Workflow action %s appears to be dependent upon a parameter"
#define HQLWRN_OutputScalarInsideChildQuery_Text "Output(%s) of single value inside a child query has undefined behaviour"

#define HQLERR_DistributionVariableLengthX_Text "DISTRIBUTION does not support variable length field '%s'"
#define HQLERR_DistributionUnsupportedTypeXX_Text "DISTRIBUTION does not support field '%s' with type %s"
#define HQLERR_InconsistentEmbedded_Text        "Field '%s' is specified as embedded but child record requires link counting"

#define HQLERR_OrderOnVarlengthStrings_Text     "Rank/Ranked not supported on variable length strings"
#define HQLERR_DistributionNoSequence_Text      "DISTRIBUTION() only supported at the outer level"
#define HQLERR_PhysicalJoinTooComplex_Text      "Physical table join condition too complicated"
#define HQLERR_ArbitaryRepeatUnimplemented_Text "Arbitrary repeats not yet supported!"
#define HQLERR_TomitaNoUnicode_Text             "Tomita does not yet support unicode"
#define HQLERR_TomitaPatternTooComplex_Text     "Patterns are too complicated for Tomita to handle at the moment [%s]"
#define HQLERR_CannotCreateSizedChildDataset_Text "Cannot currently assign to DATASET(SIZEOF(x)) fields"
#define HQLERR_OnlyNormalizeSimpleChildren_Text "Can only normalize simple child datasets - use other form if more complicated"
#define HQLERR_RoxieLocalNotSupported_Text      "ROXIE does not yet support local activities"
#define HQLERR_ThorNotSupportStepping_Text      "STEPPED is not currently supported by thor"
#define HQLERR_StarRangeOnlyInJoinCondition_Text "string[n..*] syntax is only valid in a join condition"
#define HQLERR_KeyedDistributeNoSubstringJoin_Text "Keyed distribute does not support join condition of the form field[n..*]"
#define HQLERR_OnceCannotAccessStored_Text      "ONCE workflow items cannot be dependent on other workflow items (including ONCE)"
#define HQLERR_ThorCombineOnlyLocal_Text        "Thor currently only supports the local version of COMBINE"
#define HQLERR_SteppedNotImplemented_Text       "STEPPED could not be merged into an index read activity"
#define HQLERR_MaxlengthExceedsLimit_Text       "MAXLENGTH(%u) for BUILD(index) exceeds the maximum of (%u)"

#define HQLERR_NoClearOnLocalDataset_Text       "INTERNAL: Clear not supported on LOCAL datasets"
#define HQLERR_NoCreateLocalDataset_Text        "INTERNAL: Local datasets cannot be created"
#define HQLERR_TableOutsideThor_Text            "INTERNAL: Attempt to access dataset outside of Thor"
#define HQLERR_ExtraAssignInTransform_Text      "INTERNAL: Not all Transform targets were assigned to [%s]"
#define HQLERR_CannotAccessStoredVariable_Text  "INTERNAL: Cannot access stored variable %s in this context"
#define HQLERR_EvaluateTableNotInScope_Text     "INTERNAL: Evaluate table is not in scope"
#define HQLERR_FuncNotFound_Text                "INTERNAL: Internal function %s not found"
#define HQLERR_InternalOutputScalar_Text        "INTERNAL: OUTPUT() on a scalar not processed correctly"
#define HQLERR_DfaTooComplex_Text               "INTERNAL: Expression too complex - cannot create a DFA"
#define HQLERR_UnsupportedInlineQuery_Text      "INTERNAL: Unsupported inline query"
#define HQLERR_CouldNotResolveFileposition_Text "INTERNAL: Could not resolve file position"
#define HQLERR_CastXNotImplemented_Text         "INTERNAL: Cannot perform type cast from %s to %s"
#define HQLERR_MatchedContextNotFound_Text      "INTERNAL: Could not find context to evaluate match expression"
#define HQLERR_BlobTranslationContextNotFound_Text "INTERNAL: Blob translation context not found"
#define HQLERR_RowsUsedOutsideContext_Text      "INTERNAL: ROWS can not be evaluated in this context"
#define HQLERR_ExpectedParentContext_Text       "INTERNAL: Expected a parent/container context.  Likely to be caused by executing something invalid inside a NOTHOR."
#define HQLERR_CounterNotFound_Text             "INTERNAL: Could not resolve COUNTER for inline count project"
#define HQLERR_GraphContextNotFound_Text        "INTERNAL: Graph context not found"
#define HQLERR_InvalidAcessStoredVariable_Text  "INTERNAL: Accessing unserialized stored variable %s in slave context"
#define HQLERR_LibraryNoWorkunitRead_Text       "INTERNAL: Library '%s' should not access work unit temporary '%s'"
#define HQLERR_LibraryNoWorkunitWrite_Text      "INTERNAL: Library '%s' should not create work unit temporary '%s'"
#define HQLERR_GraphInputAccessedChild_Text     "INTERNAL: Attempting to access graph output directly from a child query"
#define HQLERR_InconsisentLocalisation_Text     "INTERNAL: Inconsistent activity localisation (child %d:graph %d)"
#define HQLERR_NoParentExtract_Text             "INTERNAL: No active parent extract - activity has incorrect localisation?"
#define HQLERR_InconsistentNaryInput_Text       "INTERNAL: Inputs to nary operation have inconsistent record structures"
#define HQLERR_LinkedDatasetNoContext_Text      "INTERNAL: Linked child rows required without legal context available"
#define HQLERR_CannotGenerateSerializedCompare_Text "INTERNAL: Cannot generated serialized compare function"
#define HQLERR_ReadSpillBeforeWrite_Text        "INTERNAL: Attempt to read spill file %s before it is written"
#define HQLERR_DependencyWithinGraph_Text       "INTERNAL: Dependency within a graph incorrectly generated for hThor (%u)"
#define HQLERR_UnknownCompoundAssign_Text       "INTERNAL: Unrecognised compound assign %s"
#define HQLERR_ReadSpillBeforeWriteFix_Text     "INTERNAL: Attempt to read spill file %s before it is written.  Try adding #option ('allowThroughSpill', false); to the query."
#define HQLERR_CouldNotGenerateDefault_Text     "INTERNAL: Could not generate default value for field %s"
#define HQLERR_AccessUnavailableGraph_Text      "INTERNAL: Attempt to access result from unavailable graph (%s)"
#define HQLERR_NoMappingForField_Text           "INTERNAL: Mapping for field %s is missing from transform"

#define WARNINGAT(cat, e, x)                 reportWarning(cat, SeverityUnknown, e, x, x##_Text)
#define WARNINGAT1(cat, e, x, a)             reportWarning(cat, SeverityUnknown, e, x, x##_Text, a)
#define WARNINGAT2(cat, e, x, a, b)          reportWarning(cat, SeverityUnknown, e, x, x##_Text, a, b)
#define WARNINGAT3(cat, e, x, a, b, c)       reportWarning(cat, SeverityUnknown, e, x, x##_Text, a, b, c)

#endif
