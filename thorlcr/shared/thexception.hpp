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



#ifndef _thexception_hpp
#define _thexception_hpp



#define TE_Base                                 10000

#define TE_MachineOrderNotFound                 TE_Base + 1
#define TE_NoResultFromNode                     TE_Base + 2
#define TE_FailedToCreateProcess                TE_Base + 3
#define TE_FileNotFound                         TE_Base + 4
#define TE_BadFileLength                        TE_Base + 5
#define TE_NoFileName                           TE_Base + 6
#define TE_FileCreationFailed                   TE_Base + 7
#define TE_NonOrdered                           TE_Base + 8
#define TE_HelperUnavailable                    TE_Base + 10
#define TE_SocketReadFailed                     TE_Base + 12
#define TE_JoinTypeNotSupported                 TE_Base + 13
#define TE_DuplicateInRight                     TE_Base + 14
#define TE_NoMachineOrder                       TE_Base + 15
#define TE_VariableSizeRecordsNotSupported      TE_Base + 16
#define TE_ThorTimeout                          TE_Base + 17    
#define TE_SlaveCreationError                   TE_Base + 18
#define TE_ActivityNotFound                     TE_Base + 19
#define TE_HelperLoadError                      TE_Base + 20
#define TE_ActivityConnectionError              TE_Base + 21
#define TE_UnexpectedParameters                 TE_Base + 22
#define TE_FailedToRetrieveWorkunitValue        TE_Base + 23    
#define TE_NoPhysicalForLogicalActivity         TE_Base + 24
#define TE_PollingForNonExistentGraph           TE_Base + 25
#define TE_CannotLocateFilePart                 TE_Base + 26
#define TE_NoSlavesForLogicalFile               TE_Base + 27
#define TE_AddLogPhysNameListError              TE_Base + 28
#define TE_ResolveAllError                      TE_Base + 29
#define TE_UnknownException                     TE_Base + 30
#define TE_LogicalFileNotFound                  TE_Base + 31
#define TE_NoSuchPartForLogicalFile             TE_Base + 32
#define TE_OutOfMemory                          TE_Base + 33
#define TE_CannotConnectToSlave                 TE_Base + 34
#define TE_SpillPositionsOutOfOrder             TE_Base + 35    
#define TE_NoSuchActivityKindRegistered         TE_Base + 36
#define TE_ActivityKindAlreadyPresent           TE_Base + 37
#define TE_CouldNotResolveWildcard              TE_Base + 38
#define TE_CloneInputNotSupported               TE_Base + 39
#define TE_FatalResourceError                   TE_Base + 40
#define TE_CouldNotExtractResourceChains        TE_Base + 41
#define TE_NoMachinesFoundForFile               TE_Base + 42
#define TE_NoMemoryResourceFound                TE_Base + 43
#define TE_InsufficientMachines                 TE_Base + 44
#define TE_FailedToLoadSharedProcedure          TE_Base + 45
#define TE_FileAlreadyUsedAsTempFile            TE_Base + 46
#define TE_FailedToLoadDll                      TE_Base + 47
#define TE_FailedToConnectSlaves                TE_Base + 48
#define TE_BadGraphID                           TE_Base + 49
#define TE_StartingNonProcessActivity           TE_Base + 50
#define TE_FailedToWriteDll                     TE_Base + 51
#define TE_CouldNotFindActivity                 TE_Base + 52
#define TE_CouldNotFindMethod                   TE_Base + 53
#define TE_UnexpectedEOF                        TE_Base + 54
#define TE_NoResponseFromSlave                  TE_Base + 55
#define TE_AbortException                       TE_Base + 56
#define TE_ErrorInTCONTROLShutdown              TE_Base + 57
#define TE_DeleteNameException                  TE_Base + 58
#define TE_JackClientException                  TE_Base + 59
#define TE_NameScanFault                        TE_Base + 60    
#define TE_UnableToStartGraph                   TE_Base + 61
#define TE_ErrorReadyingActivity                TE_Base + 62    
#define TE_ErrorStartingActivity                TE_Base + 63    
#define TE_ErrorCreatingActivity                TE_Base + 64    
#define TE_ErrorEndingActivity                  TE_Base + 65
#define TE_ErrorReleasingActivity               TE_Base + 66
#define TE_ErrorInBatchCall                     TE_Base + 67
#define TE_ErrorCommunicatingWithSlaveManagers  TE_Base + 68
#define TE_FailedQueryActivityManager           TE_Base + 69
#define TE_ExceptionPassingNSFileEntry          TE_Base + 70
#define TE_SplitPostionsOutOfOrder              TE_Base + 71
#define TE_BarrierNoMaster                      TE_Base + 72
#define TE_QueryTimeoutError                    TE_Base + 74
#define TE_ErrorCleaningGraph                   TE_Base + 75
#define TE_ErrorClosingGraph                    TE_Base + 76
#define TE_ErrorInProcessDone                   TE_Base + 77
#define TE_JackResovleError                     TE_Base + 78
#define TE_FailedToAbortSlaves                  TE_Base + 79
#define TE_WorkUnitAborting                     TE_Base + 80
#define TE_SEH                                  TE_Base + 81
#define TE_MasterProcessError                   TE_Base + 82
#define TE_JoinFailedSkewExceeded               TE_Base + 83
#define TE_SortFailedSkewExceeded               TE_Base + 84
#define TE_TooMuchData                          TE_Base + 85
#define TE_DiskReadMachineGroupMismatch         TE_Base + 86
#define TE_WriteToPipeFailed                    TE_Base + 87
#define TE_ReadPartialFromPipe                  TE_Base + 88
#define TE_MissingRecordSizeHelper              TE_Base + 89
#define TE_FailedToStartJoinStreams             TE_Base + 90
#define TE_FailedToStartSplitterInput           TE_Base + 91
#define TE_RecursiveDependency                  TE_Base + 92
#define TE_OverwriteNotSpecified                TE_Base + 93
#define TE_RecordSizeMismatch                   TE_Base + 94
#define TE_CompressMismatch                     TE_Base + 95
#define TE_PipeReturnedFailure                  TE_Base + 96
#define TE_IdleRestart                          TE_Base + 97
#define TE_NotEnoughFreeSpace                   TE_Base + 98
#define TE_WorkUnitWriteLimitExceeded           TE_Base + 99
#define TE_CsvLineLenghtExceeded                TE_Base + 100
#define TE_ActivityBufferingLimitExceeded       TE_Base + 101
#define TE_CouldNotCreateLookAhead              TE_Base + 102
#define TE_FailedToRegisterSlave                TE_Base + 103
#define TE_LargeBufferWarning                   TE_Base + 104
#define TE_KeySizeMismatch                      TE_Base + 105
#define TE_BarrierAborted                       TE_Base + 106
#define TE_UnsupportedActivityKind              TE_Base + 107
#define TE_BufferTooSmall                       TE_Base + 108
#define TE_GroupMismatch                        TE_Base + 109
#define TE_CannotFetchOnCompressedFile          TE_Base + 110
#define TE_UnexpectedMultipleSlaveResults       TE_Base + 111 
#define TE_MoxieIndarOverflow                   TE_Base + 112
#define TE_KeyDiffIndexSizeMismatch             TE_Base + 113
#define TE_KeyPatchIndexSizeMismatch            TE_Base + 114
#define TE_FileCrc                              TE_Base + 115
#define TE_RowCrc                               TE_Base + 116
#define TE_SpillAdded                           TE_Base + 117
#define TE_MemberOfSuperFile                    TE_Base + 118
#define TE_SelfJoinMatchWarning                 TE_Base + 119
#define TE_BuildIndexFewExcess                  TE_Base + 120
#define TE_FetchMisaligned                      TE_Base + 121
#define TE_FetchOutOfRange                      TE_Base + 122
#define TE_IndexMissing                         TE_Base + 123
#define TE_FormatCrcMismatch                    TE_Base + 124
#define TE_UpToDate                             TE_Base + 125
#define TE_UnimplementedFeature                 TE_Base + 126
#define TE_CompressionMismatch                  TE_Base + 127
#define TE_EncryptionMismatch                   TE_Base + 128
#define TE_DistributeFailedSkewExceeded         TE_Base + 129
#define TE_SeriailzationError                   TE_Base + 130
#define TE_NotSorted                            TE_Base + 131
#define TE_LargeAggregateTable                  TE_Base + 132
#define TE_SkewWarning                          TE_Base + 133
#define TE_SkewError                            TE_Base + 134
#define TE_KERN                                 TE_Base + 135
#define TE_WorkUnitAbortingDumpInfo             TE_Base + 136
#define TE_RowLeaksDetected                     TE_Base + 137
#define TE_FileTypeMismatch                     TE_Base + 138
#define TE_RemoteReadFailure                    TE_Base + 139
#define TE_MissingOptionalFile                  TE_Base + 140
#define TE_UnsupportedSortOrder                 TE_Base + 141
#define TE_CostExceeded                         TE_Base + 142
#define TE_InvalidSortConnect                   TE_Base + 143
#define TE_Final                                TE_Base + 144       // keep this last
#define ISTHOREXCEPTION(n) (n > TE_Base && n < TE_Final)

#endif
