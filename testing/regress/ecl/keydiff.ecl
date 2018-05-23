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

//noRoxie
//noHthor
//varskip setuptype==hthor TBD
//varskip varload TBD

//class=file
//class=index
//noversion multiPart=false   // This should be supported but hthor adds meta information into the index that key diff doesn't support
//version multiPart=true
//version multiPart=true,useLocal=true

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, true);
useLocal := #IFDEFINED(root.useLocal, false);
useTranslation := #IFDEFINED(root.useTranslation, false);

//--- end of version configuration ---

#option ('layoutTranslation', useTranslation);

import $.setup;
Files := setup.Files(multiPart, useLocal, useTranslation);


KEYDIFF(Files.DG_KeyDiffIndex1, Files.DG_KeyDiffIndex2, Files.DG_FetchIndexDiffName, OVERWRITE);


