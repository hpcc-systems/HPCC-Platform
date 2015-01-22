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

#ifndef FVERROR_HPP
#define FVERROR_HPP

#include "errorlist.h"

//Don't remove items from this list - otherwise the error codes will change
enum
{
    FVERR_CouldNotResolveX = FILEVIEW_ERROR_START,
    FVERR_NoRecordDescription,
    FVERR_BadRecordDesc,
    FVERR_NeedClusterToBrowseX,
    FVERR_TimeoutRemoteFileView,
    FVERR_UnknownRemoteCommand,
    FVERR_UnknownUTFFormat,
    FVERR_FailedOpenFile,
    FVERR_CompressedFile,
    FVERR_CannotViewKey,
    FVERR_ViewComplexKey,
    FVERR_FilterTooRestrictive,
    FVERR_ZeroSizeRecord,
    FVERR_FailedOpenCompressedFile,
    FVERR_UnrecognisedJoinFieldSyntax,
    FVERR_UnrecognisedJoinFieldSyntaxXX,
    FVERR_UnrecognisedMappingFunctionX,
    FVERR_UnrecognisedFieldX,
    FVERR_ExpectedFieldSelectedFromDatasetXX,
    FVERR_CannotSelectFromDatasetX,
    FVERR_CannotSelectManyFromDatasetX,
    FVERR_ExpectedFieldSelectedFromRecordXX,
    FVERR_NumJoinFieldsMismatchXY,
    FVERR_ExpectedX,
    FVERR_FailTransformation,
    FVERR_UnrecognisedMappingFunctionXY,
    FVERR_BadStringTermination,
    FVERR_CannotBrowseFile,
    FVERR_PluginMismatch,
    FVERR_RowTooLarge,
    FVERR_CouldNotProcessSchema,
};

#define FVERR_CouldNotResolveX_Text             "Could not resolve file '%s' in DFS"
#define FVERR_NoRecordDescription_Text          "DFS did not contain a record description for '%s'"
#define FVERR_BadRecordDesc_Text                "Could not process record description for '%s'"
#define FVERR_NeedClusterToBrowseX_Text         "Need queue/cluster to browse file '%s'"
#define FVERR_TimeoutRemoteFileView_Text        "Connection to file view server timed out"
#define FVERR_UnknownRemoteCommand_Text         "Unknown remote command"
#define FVERR_UnknownUTFFormat_Text             "Unknown utf format: %s"
#define FVERR_FailedOpenFile_Text               "Failed to open file %s for browsing"
#define FVERR_CompressedFile_Text               "Cannot view compressed file '%s'"
#define FVERR_CannotViewKey_Text                "Cannot view key %s"
#define FVERR_ViewComplexKey_Text               "Cannot view complex key '%s'"
#define FVERR_FilterTooRestrictive_Text         "Filter too restrictive - no records matched within %d seconds"
#define FVERR_ZeroSizeRecord_Text               "File %s appears to have a zero length row"
#define FVERR_FailedOpenCompressedFile_Text     "Failed to open file %s as a compressed file"
#define FVERR_UnrecognisedJoinFieldSyntax_Text  "Unrecognised field mapping syntax"
#define FVERR_UnrecognisedJoinFieldSyntaxXX_Text "Unrecognised field mapping syntax %.*s"
#define FVERR_UnrecognisedMappingFunctionX_Text "Unrecognised field mapping function %s"
#define FVERR_UnrecognisedFieldX_Text           "Unrecognised field %s"
#define FVERR_ExpectedFieldSelectedFromDatasetXX_Text "Expected a field selected from dataset %.*s"
#define FVERR_CannotSelectFromDatasetX_Text     "Selection from dataset %s not supported in this context"
#define FVERR_CannotSelectManyFromDatasetX_Text "Cannot select multiple fields from dataset %s in this context"
#define FVERR_ExpectedFieldSelectedFromRecordXX_Text "Expected a field selected from dataset %.*s"
#define FVERR_NumJoinFieldsMismatchXY_Text       "Number of join fields do not match (%d, %d) relation(%s,%s)"
#define FVERR_ExpectedX_Text                     "Expected %s at [%.*s]"
#define FVERR_FailTransformation_Text            "FAIL transform called unexpectedly"
#define FVERR_UnrecognisedMappingFunctionXY_Text    "Unrecognised field mapping function %s.%s"
#define FVERR_BadStringTermination_Text          "String not terminated correctly %.*s"
#define FVERR_CannotBrowseFile_Text              "Cannot browse file '%s'"
#define FVERR_RowTooLarge_Text                   "Row too large"

#endif
