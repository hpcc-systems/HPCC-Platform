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

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, false);
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

set of unsigned4 myAll := ALL : STORED('myAll');

sequential(
    output(count(nofold(limit(Files.DG_IntIndex, 64, KEYED, COUNT))));
    output(count(nofold(limit(Files.DG_IntIndex, 63, KEYED, COUNT, SKIP))));


    output(count(nofold(limit(Files.DG_IntIndex(DG_ParentID in myAll), 64, KEYED, COUNT))));
    output(count(nofold(limit(Files.DG_IntIndex(DG_ParentID in myAll), 63, KEYED, COUNT, SKIP))));

    output(count(Files.DG_IntIndex));
    output(count(Files.DG_IntIndex(WILD(DG_ParentID)), KEYED));
    output(count(Files.DG_IntIndex(KEYED(DG_ParentID in myAll)), KEYED));
);
