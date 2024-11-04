/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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
//version multiPart=false
//version multiPart=true
//version multiPart=true,useLocal=true
//version multiPart=true,useTranslation=true
//version multiPart=false,conditional=true,nothor,nohthor
//version multiPart=true,conditional=true,nothor,nohthor

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, true);
useLocal := #IFDEFINED(root.useLocal, true);
useTranslation := #IFDEFINED(root.useTranslation, false);
conditional := #IFDEFINED(root.conditional, false);

//--- end of version configuration ---

#option ('layoutTranslation', useTranslation);
#onwarning (4523, ignore);
#onwarning (5402, ignore);
#onwarning (4522, ignore);

import $.setup;
Files := setup.Files(multiPart, useLocal, useTranslation);

IMPORT Std;


strRecord := { boolean n, string v };
intRecord := { boolean n, INTEGER v };
inRecord := { strRecord s, intRecord i; };

inDs := DATASET([
    { { false, '1' }, { false, 1 } },
    { { false, '3' }, { false, 3 } },
    { { false, '6' }, { false, 3 } },
    { { false, '7' }, { false, 7 } }
    ], inRecord);


#if (conditional)
trueValue := true : stored('trueValue');
indexAlias := INDEX(Files.DG_IntIndex, 'indexAlias', OPT);
joinIndex := IF(trueValue, Files.DG_IntIndex, indexAlias);
#else
joinIndex := Files.DG_IntIndex;
#end

output(JOIN(inDs, joinIndex, KEYED(RIGHT.DG_parentId = (integer)LEFT.s.v AND RIGHT.DG_parentId = LEFT.i.v)));
