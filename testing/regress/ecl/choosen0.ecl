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

#onwarning (1036, ignore);
//version useGeneric=false,multiPart=false
//version useGeneric=true,multiPart=false
//version useGeneric=false,multiPart=true
//version useGeneric=true,multiPart=true

import ^ as root;
useGeneric := #IFDEFINED(root.useGeneric, true);
multiPart := #IFDEFINED(root.multiPart, true);
useLocal := #IFDEFINED(root.useLocal, false);
useTranslation := #IFDEFINED(root.useTranslation, false);

#option('genericDiskReadWrites', useGeneric);

//--- end of version configuration ---

import $.setup;
Files := setup.Files(multiPart, useLocal, useTranslation);

d := nofold(dataset([{1},{2},{3}], { unsigned a} ));

choosen(d, nofold(0));
choosen(d, 1);
choosen(d, 10);
choosen(d, ALL);

d1 := Files.DG_FlatFile;
choosen(d1, nofold(0));
choosen(d1, 1);
choosen(d1, 10);
choosen(d1, ALL);

