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

//BUG: #13964 IF() needs to inherit the interesction of dataset information, not just 1st.

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

someCond := false : stored('someCond');

ds1 := dataset('x',namesRecord,FLAT);

ds2a := sort(ds1, surname, forename);
ds2 := ds2a(forename != '');

x := if(someCond, ds2, ds1);
y := sort(x, surname, forename);
output(y);


zds1 := dataset('x',namesRecord,FLAT);
zds2a := distribute(zds1, hash(surname));
zds2 := dedup(zds2a, forename, local);

zx := if(someCond, zds2, zds1);
zy := distribute(zx, hash(surname));
output(zy);
