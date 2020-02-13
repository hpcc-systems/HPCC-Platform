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

//nothor
//version multiPart=true,useLocal=true

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, false);
useLocal := #IFDEFINED(root.useLocal, false);

//--- end of version configuration ---

#option ('checkAsserts',false);
#onwarning (3164, ignore);

import $.Common.TextSearch;
import $.Common.TextSearchQueries;
import $.Setup;
import $.Setup.TS;

//SingleQuery := 'AND("the":1, "software":2, "source":3)';
//SingleQuery := 'AND("the", "software", "source")';

#if (#ISDEFINED(SingleQuery))
q1 := TextSearchQueries.SingleBatchQuery(SingleQuery);
#else
q1 := TextSearchQueries.WordTests;
#end

Files := Setup.Files(multiPart, useLocal);
wordIndex := index(TS.textSearchIndex, Files.NameWordIndex());
p := project(nofold(q1), TextSearch.doBatchExecute(wordIndex, LEFT, useLocal, 0x00000200));           // 0x200 forces paranoid order checking on
output(p);
