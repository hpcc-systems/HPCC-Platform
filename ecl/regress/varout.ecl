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

export vstring(integer i) := TYPE
    export integer physicallength := i;
    export string load(string s) := s[1..i];
    export string store(string s) := s[1..i];
    END;

input := record
           integer2 len;
           data9 cid;
           vstring(SELF.len - 11) filler;
         end;

inputfile := dataset('xxx', input, flat);

person := dataset('person', { unsigned8 person_id, string1 per_sex, string2 per_st, string40 per_first_name, data9 per_cid}, thor);
otherinput := table(person, {per_cid, per_first_name});


outputrec := record
               integer2 len;
               data9 cid;
               string25 per_first_name;
               vstring(self.len) filler;
             end;

outputrec t(input L, otherinput R) := transform
       self.len := L.len + 25;
       self.cid := L.cid;
       self.per_first_name := R.per_first_name;
       self.filler := L.filler;
     END;

outputfile := join(inputfile, otherinput, LEFT.cid = right.per_cid,t(LEFT, RIGHT));

output(outputfile,, 'ccc')
