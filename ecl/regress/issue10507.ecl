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


namesRecord :=
            RECORD
string20        surname;
string          forename{maxlength(12345)};
            END;

ds := dataset('ds', namesRecord, thor);
o1 := BUILD(ds, {surname}, {forename },'i1',overwrite);
o2 := BUILD(ds, {surname}, {forename },'i2',overwrite,maxlength);
o3 := BUILD(ds, {surname}, {forename },'i3',overwrite,maxlength(18000));

i1 := index(ds, {surname}, {forename },'i1');
i2 := index(ds, {surname}, {forename },'i1',maxlength);
i3 := index(ds, {surname}, {forename },'i1',maxlength(4321));
o4 := BUILD(i1, overwrite);
o5 := BUILD(i2, overwrite);
o6 := BUILD(i3, overwrite);

o7 := BUILD(i2, overwrite, maxlength(23456.0));
o8 := BUILD(i3, overwrite, maxlength(32000));

SEQUENTIAL(o1, o2, o3, o4, o5, o6, o7, o8);
