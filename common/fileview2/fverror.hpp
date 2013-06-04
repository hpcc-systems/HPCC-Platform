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

#include "jerrorrange.hpp"

#define ERR_FILEVIEW_FIRST  2000
#define ERR_FILEVIEW_LAST   2049

#define FVERR_CouldNotResolveX                  2000
#define FVERR_NoRecordDescription               2001
#define FVERR_BadRecordDesc                     2002
#define FVERR_NeedClusterToBrowseX              2003
#define FVERR_TimeoutRemoteFileView             2004
#define FVERR_UnknownRemoteCommand              2005
#define FVERR_UnknownUTFFormat                  2006
#define FVERR_FailedOpenFile                    2007
#define FVERR_CompressedFile                    2008
#define FVERR_CannotViewKey                     2009
#define FVERR_ViewComplexKey                    2010
#define FVERR_FilterTooRestrictive              2011
#define FVERR_ZeroSizeRecord                    2012
#define FVERR_FailedOpenCompressedFile          2013
#define FVERR_UnrecognisedJoinFieldSyntax       2014
#define FVERR_UnrecognisedJoinFieldSyntaxXX     2015
#define FVERR_UnrecognisedMappingFunctionX      2016
#define FVERR_UnrecognisedFieldX                2017
#define FVERR_ExpectedFieldSelectedFromDatasetXX 2018
#define FVERR_CannotSelectFromDatasetX          2019
#define FVERR_CannotSelectManyFromDatasetX      2020
#define FVERR_ExpectedFieldSelectedFromRecordXX 2021
#define FVERR_NumJoinFieldsMismatchXY           2022
#define FVERR_ExpectedX                         2023
#define FVERR_FailTransformation                2024
#define FVERR_UnrecognisedMappingFunctionXY     2025
#define FVERR_BadStringTermination              2026
#define FVERR_CannotBrowseFile                  2027
#define FVERR_PluginMismatch                    2028
#define FVERR_RowTooLarge                       2029

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
