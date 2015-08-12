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

import dt;

pstring := type
    export integer physicallength(string x) := transfer(x[1],
unsigned1)+1;
    export string load(string x) := x[2..transfer(x[1], unsigned1)+1];
    export string store(string x) := transfer(length(x), string1)+x;
end;

input := record
           integer2 len;
           string5 cid;
         end;

inputfile := dataset('xxx', input, flat);

person := dataset('person', { unsigned8 person_id, string1 per_sex, string2 per_st, string40 per_first_name, data9 per_cid}, thor);
otherinput := table(person, {per_cid, per_first_name});

outputrec := record
               integer2 len;
               pstring cid;
             end;

outputrec t(input L, otherinput R) := transform
//     self.cid := L.cid;
       self := L;
     END;

outputfile := join(inputfile, otherinput, LEFT.cid = (string)right.per_cid,t(LEFT, RIGHT));

output(outputfile,, 'ccc')
