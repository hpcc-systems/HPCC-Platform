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

#ifndef FVERROR_HPP
#define FVERROR_HPP

#include "errorlist.h"

//Range 6700..6749 is reserved
#if (FILEVIEW_ERROR_START != 6700 || FILEVIEW_ERROR_END != 6749)
#error "FILEVIEW_ERROR_START has changed"
#endif

#define FVERR_CouldNotResolveX                  6700
#define FVERR_NoRecordDescription               6701
#define FVERR_BadRecordDesc                     6702
#define FVERR_NeedClusterToBrowseX              6703
#define FVERR_TimeoutRemoteFileView             6704
#define FVERR_UnknownRemoteCommand              6705
#define FVERR_UnknownUTFFormat                  6706
#define FVERR_FailedOpenFile                    6707
#define FVERR_CompressedFile                    6708
#define FVERR_CannotViewKey                     6709
#define FVERR_ViewComplexKey                    6710
#define FVERR_FilterTooRestrictive              6711
#define FVERR_ZeroSizeRecord                    6712
#define FVERR_FailedOpenCompressedFile          6713
#define FVERR_UnrecognisedJoinFieldSyntax       6714
#define FVERR_UnrecognisedJoinFieldSyntaxXX     6715
#define FVERR_UnrecognisedMappingFunctionX      6716
#define FVERR_UnrecognisedFieldX                6717
#define FVERR_ExpectedFieldSelectedFromDatasetXX 6718
#define FVERR_CannotSelectFromDatasetX          6719
#define FVERR_CannotSelectManyFromDatasetX      6720
#define FVERR_ExpectedFieldSelectedFromRecordXX 6721
#define FVERR_NumJoinFieldsMismatchXY           6722
#define FVERR_ExpectedX                         6723
#define FVERR_FailTransformation                6724
#define FVERR_UnrecognisedMappingFunctionXY     6725
#define FVERR_BadStringTermination              6726
#define FVERR_CannotBrowseFile                  6727
#define FVERR_PluginMismatch                    6728
#define FVERR_RowTooLarge                       6729
#define FVERR_CouldNotProcessSchema             6730
#define FVERR_MaxLengthExceeded                 6731

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
