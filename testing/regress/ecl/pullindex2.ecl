/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.

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

//version multiPart=true
//version multiPart=false

#onwarning (4523, ignore);
import ^ as root;
multiPart := #IFDEFINED(root.multiPart, true);
variant := #IFDEFINED(root.variant, '');

//--- end of version configuration ---

import $.setup;
files := setup.files(multiPart, false);

// Test reading the same file index file sequentially from two different activities
i1 := files.getSearchIndexVariant(variant);

clonedName := __nameof__(i1) : independent;
i2 := index(i1, clonedName);

combined := PULL(i1) + PULL(i2);
output(COUNT(combined) = 2 * COUNT(i1));
