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

//skip type==thorlcr TBD
//nothor

inrec := record
unsigned6 did;
    end;

outrec := record(inrec)
string20        name;
string10        ssn;
unsigned8       dob;
          end;

ds := dataset([1,2,3,4,5,6], inrec);

i1 := dataset([{1, 'Gavin'}, {2, 'Richard'}, {5,'Nigel'}], { unsigned6 did, string10 name });
i2 := dataset([{3, '123462'}, {5, '1287234'}, {6,'007001002'}], { unsigned6 did, string10 ssn });
i3 := dataset([{1, 19700117}, {4, 19831212}, {6,20000101}], { unsigned6 did, unsigned8 dob});

j1 := join(ds, i1, left.did = right.did, left outer, lookup);
j2 := join(ds, i2, left.did = right.did, left outer, lookup);
j3 := join(ds, i3, left.did = right.did, left outer, lookup);

combined1 := combine(j1, j2, transform(outRec, self := left; self := right; self := []));
combined2 := combine(combined1, j3, transform(outRec, self.dob := right.dob; self := left));
output(combined2);

