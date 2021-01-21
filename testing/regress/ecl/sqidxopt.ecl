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

#onwarning (1036, ignore);

//version useValueSetsInIndex=false,useValueSetsInJoin=false
//version useValueSetsInIndex=true,useValueSetsInJoin=false
//version useValueSetsInIndex=false,useValueSetsInJoin=true
//version useValueSetsInIndex=true,useValueSetsInJoin=true

import ^ as root;

multiPart := #IFDEFINED(root.multiPart, false);
boolean useValueSetsInIndex := #IFDEFINED(root.useValueSetsInIndex, true);
boolean useValueSetsInJoin := #IFDEFINED(root.useValueSetsInJoin, true);

//--- end of version configuration ---

import $.setup;
sq := setup.sq(multiPart);

idx := INDEX(sq.SimplePersonBookIndex, __nameof__(sq.SimplePersonBookIndex), HINT(createValueSets(useValueSetsInIndex)));

o1 := limit(idx(KEYED(surname = 'Flintstone')), 10000, keyed);

searches := DATASET(['Windsor', 'Halliday'], { string search });


o2 := JOIN(searches, idx, KEYED(LEFT.search = RIGHT.surname), transform(right), limit(99999), HINT(createValueSets(useValueSetsInJoin)));

sequential(
    output(o1, { forename });
    output(o2, { forename });
);
