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


inrec := record
unsigned6 did;
    end;

outrec := record(inrec)
string20        name;
string10        ssn;
unsigned8       dob;
          end;

ds := dataset('ds', inrec, thor);



i1 := index(ds, { unsigned6 did, string10 name }, { } ,'\\key1');
i2 := index(ds, { unsigned6 did, string10 ssn }, { } ,'\\key2');
i3 := index(ds, { unsigned6 did, unsigned8 dob }, { } ,'\\key3');


j1 := join(ds, i1, left.did = right.did, left outer);
j2 := join(ds, i2, left.did = right.did, left outer);
j3 := join(ds, i3, left.did = right.did, left outer);

combined1 := combine(j1, j2, transform(outRec, self := left; self := right; self := []), local);
combined2 := combine(combined1, j3, transform(outRec, self := left; self := right), local);
output(combined2);

