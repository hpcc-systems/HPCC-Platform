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

#ifndef FTERROR_HPP
#define FTERROR_HPP

#include "remoteerr.hpp"
#include "errorlist.h"

#define DFTERR_CouldNotOpenFile                 (DFT_ERROR_START + 0)
#define DFTERR_BadSrcTgtCombination             (DFT_ERROR_START + 1)
#define DFTERR_UnknownFileFormatType            (DFT_ERROR_START + 2)
#define DFTERR_CouldNotCreateOutput             (DFT_ERROR_START + 3)
#define DFTERR_FixedWidthInconsistent           (DFT_ERROR_START + 4)
#define DFTERR_FormatNotSpecified               (DFT_ERROR_START + 5)
#define DFTERR_UnknownFileFormatX               (DFT_ERROR_START + 6)
#define DFTERR_CouldNotOpenFilePart             (DFT_ERROR_START + 7)
#define DFTERR_FailedStartSlave                 (DFT_ERROR_START + 8)
#define DFTERR_CopyFailed                       (DFT_ERROR_START + 9)
#define DFTERR_ReplicateNumPartsDiffer          (DFT_ERROR_START + 10)
#define DFTERR_CopyFileOntoSelf                 (DFT_ERROR_START + 11)
#define DFTERR_TargetFormatUnknownSource        (DFT_ERROR_START + 15)
#define DFTERR_MasterSeemsToHaveDied            (DFT_ERROR_START + 16)
#define DFTERR_NoSplitSourceLessTarget          (DFT_ERROR_START + 17)
#define DFTERR_NoSplitPushChangeFormat          (DFT_ERROR_START + 18)
#define DFTERR_ReplicateOptionNoSupported       (DFT_ERROR_START + 19)
#define DFTERR_NoReplicationActionGiven         (DFT_ERROR_START + 20)
#define DFTERR_MaxRecordSizeZero                (DFT_ERROR_START + 21)
#define DFTERR_NoConvertCsvOther                (DFT_ERROR_START + 22)
#define DFTERR_UnknownUTFFormat                 (DFT_ERROR_START + 23)
#define DFTERR_UTFXNotYetSupported              (DFT_ERROR_START + 24)
#define DFTERR_OnlyConvertUtfUtf                (DFT_ERROR_START + 25)
#define DFTERR_PartsDoNotHaveSameUtfFormat      (DFT_ERROR_START + 26)
#define DFTERR_BadUtf8InArguments               (DFT_ERROR_START + 27)
#define DFTERR_InputIsInvalidMultiple           (DFT_ERROR_START + 28)
#define DFTERR_InputIsInvalidMultipleUtf        (DFT_ERROR_START + 29)
#define DFTERR_EndOfRecordNotFound              (DFT_ERROR_START + 30)
#define DFTERR_InputCrcMismatch                 (DFT_ERROR_START + 31)
#define DFTERR_CannotKeepHeaderChangeFormat     (DFT_ERROR_START + 32)
#define DFTERR_PrefixCannotTransform            (DFT_ERROR_START + 33)
#define DFTERR_PrefixCannotAlsoAddHeader        (DFT_ERROR_START + 34)
#define DFTERR_InvalidPrefixFormat              (DFT_ERROR_START + 35)
#define DFTERR_PrefixTooSmall                   (DFT_ERROR_START + 36)
#define DFTERR_UnexpectedReadFailure            (DFT_ERROR_START + 37)
#define DFTERR_NoPartsInDestination             (DFT_ERROR_START + 38)
#define DFTERR_PhysicalExistsNoOverwrite        (DFT_ERROR_START + 39)
#define DFTERR_InputOutputCrcMismatch           (DFT_ERROR_START + 40)
#define DFTERR_InvalidSplitPrefixFormat         (DFT_ERROR_START + 41)
#define DFTERR_SplitPrefixSingleTarget          (DFT_ERROR_START + 42)
#define DFTERR_SplitPrefixSameFormat            (DFT_ERROR_START + 43)
#define DFTERR_SplitNoSplitClash                (DFT_ERROR_START + 44)
#define DFTERR_NoFilesMatchWildcard             (DFT_ERROR_START + 45)
#define DFTERR_RowCompressedNotSupported        (DFT_ERROR_START + 46)
#define DFTERR_CannotPushAndCompress            (DFT_ERROR_START + 47)
#define DFTERR_CannotFindFirstXmlRecord         (DFT_ERROR_START + 48)
#define DFTERR_CannotFindLastXmlRecord          (DFT_ERROR_START + 49)
#define DFTERR_CouldNotOpenCompressedFile       (DFT_ERROR_START + 50)
#define DFTERR_EndOfXmlRecordNotFound           (DFT_ERROR_START + 51)
#define DFTERR_EndOfCsvRecordNotFound           (DFT_ERROR_START + 52)
#define DFTERR_EndOfUtfRecordNotFound           (DFT_ERROR_START + 53)
#define DFTERR_WrongRECFMvRecordDescriptorWord  (DFT_ERROR_START + 54)
#define DFTERR_WrongRECFMvbBlockDescriptorWord  (DFT_ERROR_START + 55)
#define DFTERR_WrongRECFMvbBlockSize            (DFT_ERROR_START + 56)
#define DFTERR_WrongRECFMvRecordSize            (DFT_ERROR_START + 57)
#define DFTERR_WrongSplitRecordSize             (DFT_ERROR_START + 58)
#define DFTERR_CannotFindFirstJsonRecord        (DFT_ERROR_START + 59)
#define DFTERR_InvalidXmlPartSize               (DFT_ERROR_START + 60)
#define DFTERR_InvalidFilePath                  (DFT_ERROR_START + 61)
#define DFTERR_NoMatchingDropzonePath           (DFT_ERROR_START + 62)
#define DFTERR_LocalhostAddressUsed             (DFT_ERROR_START + 63)
#define DFTERR_InvalidSuperindexCopy            (DFT_ERROR_START + 64)
#define DFTERR_NoMatchingDropzonePlane          (DFT_ERROR_START + 65)
#define DFTERR_CannotConvertKeyFile             (DFT_ERROR_START + 66)
#define DFTERR_CannotCompressKey                (DFT_ERROR_START + 67)
#define DFTERR_InconsistentQuoteSeparator       (DFT_ERROR_START + 68)

//Internal errors
#define DFTERR_UnknownFormatType                (DFT_ERROR_START + 140)
#define DFTERR_OutputOffsetMismatch             (DFT_ERROR_START + 141)
#define DFTERR_NoSolarisDir                     (DFT_ERROR_START + 142)
#define DFTERR_NoSolarisCopy                    (DFT_ERROR_START + 143)
#define DFTERR_ReplicateSameFormat              (DFT_ERROR_START + 144)
#define DFTERR_PartitioningZeroSizedRowLink     (DFT_ERROR_START + 145)
#define DFTERR_CopyAborted                      (DFT_ERROR_START + 146)
#define DFTERR_WrongComputer                    (DFT_ERROR_START + 147)

static_assert(DFTERR_CouldNotOpenFile >= DFT_ERROR_START, "DFT errors must not precede their reserved range");
static_assert(DFTERR_WrongComputer <= DFT_ERROR_END, "DFT errors must not exceed the reserved DFT range");


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
#define DFTERR_CopyFailed_Text                  "Copy failed (%s)"
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
#define DFTERR_UnexpectedReadFailure_Text       "Unexpected read failure at file %s[%" I64F "d]"
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
#define DFTERR_EndOfXmlRecordNotFound_Text      "End of XML record not found (need to increase maxRecordSize?)! At offset:%" I64F "d, record size (>%d bytes) is larger than expected maxRecordSize (%d bytes)."
#define DFTERR_EndOfCsvRecordNotFound_Text      "End of CSV record not found (wrong record terminator defined or need to increase maxRecordSize?) after processing %d bytes!"
#define DFTERR_EndOfUtfRecordNotFound_Text      "End of UTF record not found (wrong record terminator defined or need to increase maxRecordSize?) after processing %d bytes!"
#define DFTERR_WrongRECFMvRecordDescriptorWord_Text  "RECFMv(b) file Record Description Word contains invalid data (0x%04x) or the file is not RECFMv(b) format!"
#define DFTERR_WrongRECFMvbBlockDescriptorWord_Text  "RECFMvb file Block Description Word contains invalid data (0x%08x) or the file is not RECFMvb format!"
#define DFTERR_WrongRECFMvbBlockSize_Text       "Invalid RECFMvb file Block Size (%d) or the file is not RECFMvb format!"
#define DFTERR_WrongRECFMvRecordSize_Text       "Invalid RECFMv file Record Size (%d) or the file is not RECFMv format!"
#define DFTERR_WrongSplitRecordSize_Text        "Invalid Record Size (%d, 0x%08x)!"
#define DFTERR_InvalidXmlPartSize_Text          "Invalid XML part size:%" I64F "d! Size is less than XML Header (%" I64F "d) + Footer (%" I64F "d)) size!"
#define DFTERR_InvalidFilePath_Text             "Invalid file path: '%s'. For security reason it is forbidden to use '%s' or '%s' to build a path!"
#define DFTERR_NoMatchingDropzonePath_Text      "No Drop Zone on '%s' configured at '%s'."
#define DFTERR_NoMatchingDropzonePlane_Text     "No Drop Zone for '%s'"
#define DFTERR_LocalhostAddressUsed_Text        "Localhost address used in remote file name: '%s'"
#define DFTERR_InvalidSuperindexCopy_Text       "Source file %s is a super index file but copying as a simple logical file"
#define DFTERR_CannotConvertKeyFile_Text        "Cannot convert key file to a different format"
#define DFTERR_CannotCompressKey_Text           "Compress output is not compatible with copying a key file"
#define DFTERR_InconsistentQuoteSeparator_Text  "The quote character %s and separator %s are inconsistent with the file contents"

#define DFTERR_UnknownFormatType_Text           "INTERNAL: Save unknown format type"
#define DFTERR_OutputOffsetMismatch_Text        "INTERNAL: Output offset does not match expected (%" I64F "d expected %" I64F "d) at %s of block %d"
#define DFTERR_NoSolarisDir_Text                "Directory not yet supported for solaris"
#define DFTERR_NoSolarisCopy_Text               "Copy not yet supported for solaris"
#define DFTERR_ReplicateSameFormat_Text         "INTERNAL: Replicate cannot convert formats"
#define DFTERR_PartitioningZeroSizedRowLink_Text        "Zero sized row link in source file at %" I64F "d - cannot partition"
#define DFTERR_CopyAborted_Text                 "Copy failed - User Abort"
#define DFTERR_WrongComputer_Text               "INTERNAL: Command send to wrong computer.  Expected %s got %s"

#endif
