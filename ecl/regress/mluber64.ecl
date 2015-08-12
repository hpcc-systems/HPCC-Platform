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

#option ('targetClusterType', 'roxie');

l :=
RECORD
    STRING100 s;
    unsigned docid;
    unsigned wordpos;
END;


ds := DATASET([{'a',1,1},{'a',2,1},{'a',3,1},{'b',2,2},{'b',3,2},{'c',3,3}],l);
ds_sorted := SORT(DISTRIBUTE(ds,HASH(docid)),s,docid,wordpos,LOCAL);


i := INDEX(ds_sorted, {s,docid,wordpos},'~thor::key::localindex');


i_read := i(keyed(s='a'));
local_i_read := allnodes(local(i_read));

output(i);
output(i_read);
output(local_i_read);
