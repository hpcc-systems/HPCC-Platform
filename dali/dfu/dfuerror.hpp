/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#ifndef DFUERROR_HPP
#define DFUERROR_HPP

#define ERR_DFU_FIRST             8250
#define ERR_DFU_LAST              8299

//syntax errors 
#define DFUERR_InvalidCommandSyntax     8250
#define DFUERR_TooFewArguments          8251
#define DFUERR_InvalidArgument          8252

#define DFUERR_InvalidLogicalName       8260
//map-spec errors
#define DFUERR_InvalidFileMask          8270
#define DFUERR_NoLogicalName            8271
#define DFUERR_InvalidExternalFileMask  8272
#define DFUERR_NoNameSpec               8273
#define DFUERR_NoDirectory              8274
#define DFUERR_NoNode                   8275
#define DFUERR_NoNodeForPart            8276
#define DFUERR_SameFilePartNumber       8277
#define DFUERR_SameFilePartName         8278
#define DFUERR_DifferentNodes           8279
#define DFUERR_MultiNotAllowed          8280

//recovery errors
#define DFUERR_JobNotFound              8285
#define DFUERR_NoJobFound               8286
#define DFUERR_InconsistentInfo         8287


#define DFUERR_CouldNotOpenFile         8290
#define DFUERR_GroupNotFound            8291
#define DFUERR_SourceFileNotFound       8292
#define DFUERR_DFileExists              8293
#define DFUERR_DFileNotFound            8294
#define DFUERR_DestDFileExists          8295
#define DFUERR_NoDaliServerList         8296
#define DFUERR_NoDefaultUser            8297

#define DFUERR_DSuperFileNotFound       8300
#define DFUERR_DSuperFileNotEmpty       8301
#define DFUERR_DSuperFileContainsSub        8302
#define DFUERR_DSuperFileDoesntContainSub   8303


//---- Text for all errors (make it easy to internationalise) ---------------------------

#define DFUERR_InvalidCommandSyntax_Text    "Syntax error on command DFU %s"
#define DFUERR_TooFewArguments_Text         "Syntax error - too few arguments for command DFU %s"
#define DFUERR_InvalidArgument_Text         "Syntax error on %s for command DFU %s"

#define DFUERR_InvalidLogicalName_Text      "Invalid logical name %s"

#define DFUERR_InvalidFileMask_Text         "File mask contains directory but no file name (%s)."
#define DFUERR_NoLogicalName_Text           "No logical name specified"
#define DFUERR_InvalidExternalFileMask_Text "$L$ illegal in file mask for external file"
#define DFUERR_NoNameSpec_Text              "No <name-spec> found. When omitted, specify DEFAULTNAMESPEC in ini file."
#define DFUERR_NoDirectory_Text             "No directory information found. When omitted, specify DEFAULTBASEDIR in ini file."
#define DFUERR_NoNode_Text                  "No node information found."
#define DFUERR_NoNodeForPart_Text           "No node for part %d (%s) found"
#define DFUERR_SameFilePartNumber_Text      "Number of file parts must be the same (original has %d, copy has %d)."
#define DFUERR_SameFilePartName_Text        "DFile has no part %s. Replicated parts must have same name as original."
#define DFUERR_DifferentNodes_Text          "Part %d(%s): Nodes must be different (currently %s)"
#define DFUERR_MultiNotAllowed_Text         "Wild or Multi-filenames not allowed for this operation"

#define DFUERR_JobNotFound_Text             "No recovery information found for job %"I64F"d"
#define DFUERR_NoJobFound_Text              "No matching recovery information found."
#define DFUERR_InconsistentInfo_Text        "Recovery information is inconsistent with current state."

#define DFUERR_CouldNotOpenFile_Text        "Could not open file %s"
#define DFUERR_GroupNotFound_Text           "Group %s not found"
#define DFUERR_SourceFileNotFound_Text      "Source DFile %s not found"
#define DFUERR_DFileExists_Text             "DFile %s already exists."
#define DFUERR_DFileNotFound_Text           "DFile %s not found"
#define DFUERR_DestDFileExists_Text         "Destination DFile %s already exists."
#define DFUERR_NoDaliServerList_Text        "No Dali server list specified in DFU.INI (DALISERVERS=ip:port,ip:port...)"
#define DFUERR_NoDefaultUser_Text           "No default user specified in DFU.INI (e.g. DEFAULTUSER=domain\\userid)"

#define DFUERR_DSuperFileNotFound_Text      "SuperFile %s not found"
#define DFUERR_DSuperFileNotEmpty_Text      "SuperFile %s not empty"
#define DFUERR_DSuperFileContainsSub_Text       "Superfile already contains subfile %s"
#define DFUERR_DSuperFileDoesntContainSub_Text  "Superfile doesn't contains subfile %s"

#endif
