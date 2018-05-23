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

//fail

//class=error

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, true);
useLocal := #IFDEFINED(root.useLocal, false);
useTranslation := #IFDEFINED(root.useTranslation, false);

//--- end of version configuration ---

#onwarning (4515, ignore);
#onwarning (4523, ignore);

import $.setup;
Files := setup.Files(multiPart, useLocal, useTranslation);

// this would use RLT, but we have not enabled it, so it should fail
#option ('layoutTranslation', false);

DG_FetchIndex1Alt1 := INDEX(Files.DG_FetchFile,{Fname,Lname,__filepos},Files.DG_FetchIndex1Name);

ds := DATASET([{'Anderson'}, {'Doe'}], {STRING25 Lname});

OUTPUT(SORT(DG_FetchIndex1Alt1(Lname = 'Smith'), record), {Fname, Lname});
