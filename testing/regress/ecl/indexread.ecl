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

//class=index
//version multiPart=false
//version multiPart=true
//version multiPart=true,useLocal=true
//noversion multiPart=true,useTranslation=true,nothor

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, true);
useLocal := #IFDEFINED(root.useLocal, false);
useTranslation := #IFDEFINED(root.useTranslation, false);

//--- end of version configuration ---

#option ('layoutTranslationEnabled', useTranslation);
#onwarning (5402, ignore);

import $.setup;
Files := setup.Files(multiPart, useLocal, useTranslation);

IMPORT Std;

// try it with just one limit

o1 := output(LIMIT(Files.DG_FetchIndex(Lname='Anderson'),1,SKIP), {Lname,Fname,TRIM(tfn),state,TRIM(blobfield)});
o2 := output(LIMIT(Files.DG_FetchIndex(Lname='Anderson'),10,SKIP), {Lname,Fname,TRIM(tfn),state,TRIM(blobfield)});

o1:independent;
o2:independent;
