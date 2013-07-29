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

#ifndef FTERROR_HPP
#define FTERROR_HPP

#include "remoteerr.hpp"

#define ERR_DFT_FIRST                           8050
#define ERR_DFT_LAST                            8099

#define DFTERR_CouldNotOpenFile                 8050
#define DFTERR_BadSrcTgtCombination             8051
#define DFTERR_UnknownFileFormatType            8052
#define DFTERR_CouldNotCreateOutput             8053
#define DFTERR_FixedWidthInconsistent           8054
#define DFTERR_FormatNotSpecified               8055
#define DFTERR_UnknownFileFormatX               8056
#define DFTERR_CouldNotOpenFilePart             8057
#define DFTERR_FailedStartSlave                 8058
#define DFTERR_CopyFailed                       8059
#define DFTERR_ReplicateNumPartsDiffer          8060
#define DFTERR_CopyFileOntoSelf                 8061
#define DFTERR_TargetFormatUnknownSource        8065
#define DFTERR_MasterSeemsToHaveDied            8066
#define DFTERR_NoSplitSourceLessTarget          8067
#define DFTERR_NoSplitPushChangeFormat          8068
#define DFTERR_ReplicateOptionNoSupported       8069
#define DFTERR_NoReplicationActionGiven         8070
#define DFTERR_MaxRecordSizeZero                8071
#define DFTERR_NoConvertCsvOther                8072
#define DFTERR_UnknownUTFFormat                 8073
#define DFTERR_UTFXNotYetSupported              8074
#define DFTERR_OnlyConvertUtfUtf                8075
#define DFTERR_PartsDoNotHaveSameUtfFormat      8076
#define DFTERR_BadUtf8InArguments               8077
#define DFTERR_InputIsInvalidMultiple           8078
#define DFTERR_InputIsInvalidMultipleUtf        8079
#define DFTERR_EndOfRecordNotFound              8080
#define DFTERR_InputCrcMismatch                 8081
#define DFTERR_CannotKeepHeaderChangeFormat     8082
#define DFTERR_PrefixCannotTransform            8083
#define DFTERR_PrefixCannotAlsoAddHeader        8084
#define DFTERR_InvalidPrefixFormat              8085
#define DFTERR_PrefixTooSmall                   8086
#define DFTERR_UnexpectedReadFailure            8087
#define DFTERR_NoPartsInDestination             8088
#define DFTERR_PhysicalExistsNoOverwrite        8089
#define DFTERR_InputOutputCrcMismatch           8090
#define DFTERR_InvalidSplitPrefixFormat         8091
#define DFTERR_SplitPrefixSingleTarget          8092
#define DFTERR_SplitPrefixSameFormat            8093
#define DFTERR_SplitNoSplitClash                8094
#define DFTERR_NoFilesMatchWildcard             8095
#define DFTERR_RowCompressedNotSupported        8096
#define DFTERR_CannotPushAndCompress            8097
#define DFTERR_CannotFindFirstXmlRecord         8098
#define DFTERR_CannotFindLastXmlRecord          8099
#define DFTERR_CouldNotOpenCompressedFile       8100
#define DFTERR_EndOfXmlRecordNotFound           8101

//Internal errors
#define DFTERR_UnknownFormatType                8190
#define DFTERR_OutputOffsetMismatch             8191
#define DFTERR_NoSolarisDir                     8192
#define DFTERR_NoSolarisCopy                    8193
#define DFTERR_ReplicateSameFormat              8194
#define DFTERR_PartitioningZeroSizedRowLink     8195
#define DFTERR_CopyAborted                      8196
#define DFTERR_WrongComputer                    8197


//---- Text for all errors (make it easy to internationalise) ---------------------------

#define DFTERR_CouldNotOpenFile_Text            "Could not open source file %s"
#define DFTERR_BadSrcTgtCombination_Text        "This combination of source and target formats is not supported"
#define DFTERR_UnknownFileFormatType_Text       "Unknown file format"
#define DFTERR_CouldNotCreateOutput_Text        "Could not create output file %s"
#define DFTERR_FixedWidthInconsistent_Text      "Length of file %s is inconsistent with its record size"
#define DFTERR_FormatNotSpecified_Text          "Source file format is not specified or is unsuitable for (re-)partitioning"
#define DFTERR_UnknownFileFormatX_Text          "Unknown format '%s' for source file"
#define DFTERR_CouldNotOpenFilePart_Text        "Could not open file part %s"
#define DFTERR_FailedStartSlave_Text            "Failed to start child slave '%s'"
#define DFTERR_CopyFailed_Text                  "Copy failed (unknown reason)"
#define DFTERR_ReplicateNumPartsDiffer_Text     "Number of parts in source and target must match when replicating"
#define DFTERR_CopyFileOntoSelf_Text            "Trying to copy a file (%s) onto itself"
#define DFTERR_TargetFormatUnknownSource_Text   "Cannot omit source format if target format is supplied"
#define DFTERR_MasterSeemsToHaveDied_Text       "Master program seems to have died..."
#define DFTERR_NoSplitSourceLessTarget_Text     "NOSPLIT is not valid if there more targets than sources"
#define DFTERR_NoSplitPushChangeFormat_Text     "NOSPLIT is not supported for a push operation which also changes format"
#define DFTERR_ReplicateOptionNoSupported_Text  "Replication option %s isn't yet supported"
#define DFTERR_NoReplicationActionGiven_Text    "No replicate action supplied"
#define DFTERR_MaxRecordSizeZero_Text           "Maximum record size must be larger than 0"
#define DFTERR_NoConvertCsvOther_Text           "Conversion between csv and other formats is not yet supported"
#define DFTERR_UnknownUTFFormat_Text            "Unknown utf format: %s"
#define DFTERR_UTFXNotYetSupported_Text         "UTF format %s isn't yet supported"
#define DFTERR_OnlyConvertUtfUtf_Text           "Can only comnvert utf to other utf formats"
#define DFTERR_PartsDoNotHaveSameUtfFormat_Text "Input files do not have the same utf format"
#define DFTERR_BadUtf8InArguments_Text          "The utf separators/terminators aren't valid utf-8"
#define DFTERR_InputIsInvalidMultiple_Text      "Source file %s is not a valid multiple of the expected record size (%d)"
#define DFTERR_InputIsInvalidMultipleUtf_Text   "Source file %s is not a valid multiple of the utf character width (%d)"
#define DFTERR_EndOfRecordNotFound_Text         "End of record not found (need to increase maxRecordSize?)"
#define DFTERR_CannotKeepHeaderChangeFormat_Text "keepHeader option cannot be used if pushing and converting formats"
#define DFTERR_PrefixCannotTransform_Text       "Adding a length prefix cannot be done why transforming formats"
#define DFTERR_PrefixCannotAlsoAddHeader_Text   "Cannot add length prefixes and also add headers"
#define DFTERR_InvalidPrefixFormat_Text         "Invalid length prefix: '%s'"
#define DFTERR_PrefixTooSmall_Text              "Length prefix too small to store source length"
#define DFTERR_UnexpectedReadFailure_Text       "Unexpected read failure at file %s[%"I64F"d]"
#define DFTERR_NoPartsInDestination_Text        "Destination does not have any parts"
#define DFTERR_PhysicalExistsNoOverwrite_Text   "OVERWRITE not supplied and physical file %s exists"
#define DFTERR_InvalidSplitPrefixFormat_Text    "Cannot process file %s using the splitprefix supplied"
#define DFTERR_SplitPrefixSingleTarget_Text     "SplitPrefix can only name a single target file"
#define DFTERR_SplitPrefixSameFormat_Text       "SplitPrefix cannot perform a format converstion"
#define DFTERR_SplitNoSplitClash_Text           "/splitprefix is not compatible with /nosplit"
#define DFTERR_NoFilesMatchWildcard_Text        "The wildcarded source did not match any filenames"
#define DFTERR_RowCompressedNotSupported_Text   "Cannot copy from a legacy row compressed file"
#define DFTERR_CannotPushAndCompress_Text       "Need to pull the data when compressing the output"
#define DFTERR_CannotFindFirstXmlRecord_Text    "Could not find the start of the first record"
#define DFTERR_CouldNotOpenCompressedFile_Text  "Could not open file %s as compressed file"
#define DFTERR_EndOfXmlRecordNotFound_Text      "End of XML record not found (need to increase maxRecordSize?)!\nCXmlQuickPartitioner::findSplitPoint: at offset:%"I64F"d, record size (>%d bytes) is larger than expected maxRecordSize (%d bytes) [and blockSize (%d bytes)]."

#define DFTERR_UnknownFormatType_Text           "INTERNAL: Save unknown format type"
#define DFTERR_OutputOffsetMismatch_Text        "INTERNAL: Output offset does not match expected (%"I64F"d expected %"I64F"d) at %s of block %d"
#define DFTERR_NoSolarisDir_Text                "Directory not yet supported for solaris"
#define DFTERR_NoSolarisCopy_Text               "Copy not yet supported for solaris"
#define DFTERR_ReplicateSameFormat_Text         "INTERNAL: Replicate cannot convert formats"
#define DFTERR_PartitioningZeroSizedRowLink_Text        "Zero sized row link in source file at %"I64F"d - cannot partition"
#define DFTERR_CopyAborted_Text                 "Copy failed - User Abort"
#define DFTERR_WrongComputer_Text               "INTERNAL: Command send to wrong computer.  Expected %s got %s"

#endif
