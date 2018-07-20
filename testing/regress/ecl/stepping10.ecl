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

//class=file
//class=index
//version multiPart=false,useExplicitSuper=false
//version multiPart=true,useExplicitSuper=false
//version multiPart=false,useExplicitSuper=true
//version multiPart=true,useExplicitSuper=true

//nothor
//Stepped global joins unsupported, see issue HPCC-8148
//skip type==thorlcr TBD

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, false);
useLocal := #IFDEFINED(root.useLocal, false);
useTranslation := #IFDEFINED(root.useTranslation, false);
useExplicitSuper := #IFDEFINED(root.useExplicitSuper, false);

//--- end of version configuration ---

#option ('layoutTranslation', useTranslation);
#onwarning (4126, ignore);

import $.setup;
Files := setup.Files(multiPart, useLocal, useTranslation);

#if (useExplicitSuper)
idx := INDEX(Files.DG_DupKeyedIndexFile, Files.DG_DupKeyedIndexSuperFileName);
#else
idx := Files.DG_DupKeyedIndexFile;
#end

// should be equivalent to OUTPUT(SORT(Files.DG_IndexFile(DG_firstname = 'DAVID'), DG_Prange, DG_firstname, DG_lastname));
OUTPUT(STEPPED(idx(KEYED(DG_firstname = 'DAVID')), DG_Prange));
