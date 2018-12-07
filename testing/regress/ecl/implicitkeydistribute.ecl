/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.

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

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, false);
useSequential := #IFDEFINED(root.useSequential, false);

//--- end of version configuration ---

#onwarning (2309, ignore);      // remove complaint aboute filtered right preventing keyed join
#onwarning (4515, ignore);
#onwarning (4523, ignore);      // index read is not limited

import $.setup;

files := setup.files(multiPart, false);

inlineDs := dataset([
        {'AAAA','AAA'},
        {'CLAIRE','BAYLISS'},
        {'KIMBERLY','SMITH'},
        {'ZZZ','ZZZ'}]
        , { string first, STRING last} );
        
myIndex := files.DG_VarIndex(dg_lastname != '');

j := JOIN(inlineDs, myIndex, LEFT.last = RIGHT.dg_lastname AND LEFT.first = RIGHT.dg_firstName);

gr := SORT(TABLE(j, { last, cnt := COUNT(GROUP) }, last), last); 

gr;
