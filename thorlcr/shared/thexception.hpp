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

#include "engineerr.hpp"

#define TE_MachineOrderNotFound                 THOR_ERROR_START + 1
#define TE_NoResultFromNode                     THOR_ERROR_START + 2
#define TE_FailedToCreateProcess                THOR_ERROR_START + 3
#define TE_FileNotFound                         THOR_ERROR_START + 4
#define TE_BadFileLength                        THOR_ERROR_START + 5
#define TE_NoFileName                           THOR_ERROR_START + 6
#define TE_FileCreationFailed                   THOR_ERROR_START + 7
#define TE_NonOrdered                           THOR_ERROR_START + 8
#define TE_HelperUnavailable                    THOR_ERROR_START + 10
#define TE_SocketReadFailed                     THOR_ERROR_START + 12
#define TE_JoinTypeNotSupported                 THOR_ERROR_START + 13
#define TE_DuplicateInRight                     THOR_ERROR_START + 14
#define TE_NoMachineOrder                       THOR_ERROR_START + 15
#define TE_VariableSizeRecordsNotSupported      THOR_ERROR_START + 16
#define TE_ThorTimeout                          THOR_ERROR_START + 17
#define TE_SlaveCreationError                   THOR_ERROR_START + 18
#define TE_ActivityNotFound                     THOR_ERROR_START + 19
#define TE_HelperLoadError                      THOR_ERROR_START + 20
#define TE_ActivityConnectionError              THOR_ERROR_START + 21
#define TE_UnexpectedParameters                 THOR_ERROR_START + 22
#define TE_FailedToRetrieveWorkunitValue        THOR_ERROR_START + 23
#define TE_NoPhysicalForLogicalActivity         THOR_ERROR_START + 24
#define TE_PollingForNonExistentGraph           THOR_ERROR_START + 25
#define TE_CannotLocateFilePart                 THOR_ERROR_START + 26
#define TE_NoSlavesForLogicalFile               THOR_ERROR_START + 27
#define TE_AddLogPhysNameListError              THOR_ERROR_START + 28
#define TE_ResolveAllError                      THOR_ERROR_START + 29
#define TE_UnknownException                     THOR_ERROR_START + 30
#define TE_LogicalFileNotFound                  THOR_ERROR_START + 31
#define TE_NoSuchPartForLogicalFile             THOR_ERROR_START + 32
#define TE_OutOfMemory                          THOR_ERROR_START + 33
#define TE_CannotConnectToSlave                 THOR_ERROR_START + 34
#define TE_SpillPositionsOutOfOrder             THOR_ERROR_START + 35
#define TE_NoSuchActivityKindRegistered         THOR_ERROR_START + 36
#define TE_ActivityKindAlreadyPresent           THOR_ERROR_START + 37
#define TE_CouldNotResolveWildcard              THOR_ERROR_START + 38
#define TE_CloneInputNotSupported               THOR_ERROR_START + 39
#define TE_FatalResourceError                   THOR_ERROR_START + 40
#define TE_CouldNotExtractResourceChains        THOR_ERROR_START + 41
#define TE_NoMachinesFoundForFile               THOR_ERROR_START + 42
#define TE_NoMemoryResourceFound                THOR_ERROR_START + 43
#define TE_InsufficientMachines                 THOR_ERROR_START + 44
#define TE_FailedToLoadSharedProcedure          THOR_ERROR_START + 45
#define TE_FileAlreadyUsedAsTempFile            THOR_ERROR_START + 46
#define TE_FailedToLoadDll                      THOR_ERROR_START + 47
#define TE_FailedToConnectSlaves                THOR_ERROR_START + 48
#define TE_BadGraphID                           THOR_ERROR_START + 49
#define TE_StartingNonProcessActivity           THOR_ERROR_START + 50
#define TE_FailedToWriteDll                     THOR_ERROR_START + 51
#define TE_CouldNotFindActivity                 THOR_ERROR_START + 52
#define TE_CouldNotFindMethod                   THOR_ERROR_START + 53
#define TE_UnexpectedEOF                        THOR_ERROR_START + 54
#define TE_NoResponseFromSlave                  THOR_ERROR_START + 55
#define TE_AbortException                       THOR_ERROR_START + 56
#define TE_ErrorInTCONTROLShutdown              THOR_ERROR_START + 57
#define TE_DeleteNameException                  THOR_ERROR_START + 58
#define TE_JackClientException                  THOR_ERROR_START + 59
#define TE_NameScanFault                        THOR_ERROR_START + 60
#define TE_UnableToStartGraph                   THOR_ERROR_START + 61
#define TE_ErrorReadyingActivity                THOR_ERROR_START + 62
#define TE_ErrorStartingActivity                THOR_ERROR_START + 63
#define TE_ErrorCreatingActivity                THOR_ERROR_START + 64
#define TE_ErrorEndingActivity                  THOR_ERROR_START + 65
#define TE_ErrorReleasingActivity               THOR_ERROR_START + 66
#define TE_ErrorInBatchCall                     THOR_ERROR_START + 67
#define TE_ErrorCommunicatingWithSlaveManagers  THOR_ERROR_START + 68
#define TE_FailedQueryActivityManager           THOR_ERROR_START + 69
#define TE_ExceptionPassingNSFileEntry          THOR_ERROR_START + 70
#define TE_SplitPostionsOutOfOrder              THOR_ERROR_START + 71
#define TE_BarrierNoMaster                      THOR_ERROR_START + 72
#define TE_QueryTimeoutError                    THOR_ERROR_START + 74
#define TE_ErrorCleaningGraph                   THOR_ERROR_START + 75
#define TE_ErrorClosingGraph                    THOR_ERROR_START + 76
#define TE_ErrorInProcessDone                   THOR_ERROR_START + 77
#define TE_JackResovleError                     THOR_ERROR_START + 78
#define TE_FailedToAbortSlaves                  THOR_ERROR_START + 79
#define TE_WorkUnitAborting                     THOR_ERROR_START + 80
#define TE_SEH                                  THOR_ERROR_START + 81
#define TE_MasterProcessError                   THOR_ERROR_START + 82
#define TE_JoinFailedSkewExceeded               THOR_ERROR_START + 83
#define TE_SortFailedSkewExceeded               THOR_ERROR_START + 84
#define TE_TooMuchData                          THOR_ERROR_START + 85
#define TE_DiskReadMachineGroupMismatch         THOR_ERROR_START + 86
#define TE_WriteToPipeFailed                    THOR_ERROR_START + 87
#define TE_ReadPartialFromPipe                  THOR_ERROR_START + 88
#define TE_MissingRecordSizeHelper              THOR_ERROR_START + 89
#define TE_FailedToStartJoinStreams             THOR_ERROR_START + 90
#define TE_FailedToStartSplitterInput           THOR_ERROR_START + 91
#define TE_RecursiveDependency                  THOR_ERROR_START + 92
#define TE_OverwriteNotSpecified                THOR_ERROR_START + 93
#define TE_RecordSizeMismatch                   THOR_ERROR_START + 94
#define TE_CompressMismatch                     THOR_ERROR_START + 95
#define TE_PipeReturnedFailure                  THOR_ERROR_START + 96
#define TE_IdleRestart                          THOR_ERROR_START + 97
#define TE_NotEnoughFreeSpace                   THOR_ERROR_START + 98
#define TE_WorkUnitWriteLimitExceeded           THOR_ERROR_START + 99
#define TE_CsvLineLenghtExceeded                THOR_ERROR_START + 100
#define TE_ActivityBufferingLimitExceeded       THOR_ERROR_START + 101
#define TE_CouldNotCreateLookAhead              THOR_ERROR_START + 102
#define TE_FailedToRegisterSlave                THOR_ERROR_START + 103
#define TE_LargeBufferWarning                   THOR_ERROR_START + 104
#define TE_KeySizeMismatch                      THOR_ERROR_START + 105
#define TE_BarrierAborted                       THOR_ERROR_START + 106
#define TE_UnsupportedActivityKind              THOR_ERROR_START + 107
#define TE_BufferTooSmall                       THOR_ERROR_START + 108
#define TE_GroupMismatch                        THOR_ERROR_START + 109
#define TE_CannotFetchOnCompressedFile          THOR_ERROR_START + 110
#define TE_UnexpectedMultipleSlaveResults       THOR_ERROR_START + 111
#define TE_MoxieIndarOverflow                   THOR_ERROR_START + 112
#define TE_KeyDiffIndexSizeMismatch             THOR_ERROR_START + 113
#define TE_KeyPatchIndexSizeMismatch            THOR_ERROR_START + 114
#define TE_FileCrc                              THOR_ERROR_START + 115
#define TE_RowCrc                               THOR_ERROR_START + 116
#define TE_SpillAdded                           THOR_ERROR_START + 117
#define TE_MemberOfSuperFile                    THOR_ERROR_START + 118
#define TE_SelfJoinMatchWarning                 THOR_ERROR_START + 119
#define TE_BuildIndexFewExcess                  THOR_ERROR_START + 120
#define TE_FetchMisaligned                      THOR_ERROR_START + 121
#define TE_FetchOutOfRange                      THOR_ERROR_START + 122
#define TE_IndexMissing                         THOR_ERROR_START + 123
#define TE_FormatCrcMismatch                    THOR_ERROR_START + 124
#define TE_UnimplementedFeature                 THOR_ERROR_START + 125
#define TE_CompressionMismatch                  THOR_ERROR_START + 126
#define TE_EncryptionMismatch                   THOR_ERROR_START + 127
#define TE_DistributeFailedSkewExceeded         THOR_ERROR_START + 128
#define TE_SeriailzationError                   THOR_ERROR_START + 129
#define TE_NotSorted                            THOR_ERROR_START + 130
#define TE_LargeAggregateTable                  THOR_ERROR_START + 131
#define TE_SkewWarning                          THOR_ERROR_START + 132
#define TE_SkewError                            THOR_ERROR_START + 133
#define TE_KERN                                 THOR_ERROR_START + 134
#define TE_WorkUnitAbortingDumpInfo             THOR_ERROR_START + 135
#define TE_RowLeaksDetected                     THOR_ERROR_START + 136
#define TE_RemoteReadFailure                    THOR_ERROR_START + 137
#define TE_UnsupportedSortOrder                 THOR_ERROR_START + 138
#define TE_CostExceeded                         THOR_ERROR_START + 139
#define TE_InvalidSortConnect                   THOR_ERROR_START + 140
#define TE_Final                                THOR_ERROR_START + 141       // keep this last
#define ISTHOREXCEPTION(n) (n > THOR_ERROR_START && n < TE_Final)

#endif
