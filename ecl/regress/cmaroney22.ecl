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

//BUG: #14105 - duplicate
export namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

ds := dataset('x1',namesRecord,FLAT);

outr := record
string20        surname;
string10        forename;
        end;

export ds1 := project(ds, transform(outr, SELF := LEFT));

ds := dataset('x2',namesRecord,FLAT);

outr := record
string20        surname;
integer2        age := 25;
        end;

export ds2 := project(ds, transform(outr, SELF := LEFT));


combined := record
dataset(recordof(ds1))  ds1;
dataset(recordof(ds2))  ds2;
dataset(recordof(ds1))  ds1b;
dataset(recordof(ds2))  ds2b;
            end;



result := dataset(row(transform(combined, self.ds1 := ds1; self.ds2 := ds2; self.ds1b := []; self.ds2b := [])));

output(result,,'out.d00');
