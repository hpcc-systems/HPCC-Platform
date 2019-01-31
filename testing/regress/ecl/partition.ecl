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
//version multiPart=false
//version multiPart=true
//version multiPart=true,useLocal=true
//version multiPart=true,useTranslation=true

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, true);
useLocal := #IFDEFINED(root.useLocal, true);
useTranslation := #IFDEFINED(root.useTranslation, false);

//--- end of version configuration ---

#option ('layoutTranslation', useTranslation);
#onwarning (4523, ignore);
#onwarning (5402, ignore);
#onwarning (4522, ignore);

import $.setup;
Files := setup.Files(multiPart, useLocal, useTranslation);

IMPORT Std;

// Checking that partitions and bloom filters are working properly on files with leading integer field

OUTPUT(Files.DG_IntIndex(KEYED(DG_parentId = 1)));
OUTPUT(Files.DG_IntIndex(KEYED(DG_parentId = 3)));
OUTPUT(Files.DG_IntIndex(KEYED(DG_parentId = 7)));
OUTPUT(Files.DG_IntIndex(KEYED(DG_parentId = 22)));
OUTPUT(SORT(Files.DG_IntIndex(KEYED(DG_parentId = 1) OR KEYED(DG_parentId = 22)), DG_parentId)); // testing key filter that cannot be partitioned

// test partitoned key with keyed join
inds := DATASET(COUNT(Files.DG_IntIndex), TRANSFORM({unsigned id}, SELF.id := COUNTER-1));
COUNT(JOIN(inds, Files.DG_IntIndex, LEFT.id=RIGHT.DG_parentId));
COUNT(JOIN(inds, Files.DG_IntIndex, RIGHT.DG_parentId IN [1,22,LEFT.id])); // tests filter that cannot be partitioned
