/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
//version multiPart=false,useSequential=true

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, false);
useSequential := #IFDEFINED(root.useSequential, false);

//--- end of version configuration ---

import $.setup;
sq := setup.sq(multiPart);

pr:= table(sq.SimplePersonBookDs, { fullname := trim(surname) + ', ' + trim(forename), aage });

//Aggregate on a projected table that can't be merged.  seq is actually set to aage
pr2:= table(sq.SimplePersonBookDs, { surname, forename, aage, unsigned8 seq := (random() % 100) / 2000 + aage; });

#EXPAND(IF(useSequential, 'SEQUENTIAL', 'ORDERED'))
(
    //Filtered Aggregate on a projected table.
    output(table(pr(aage > 20), { max(group, fullname) }));

    //Filtered Aggregate on a projected table.
    output(table(pr2(seq > 30), { ave(group, aage) }));
);
