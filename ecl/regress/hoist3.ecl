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

namesRecord :=
            RECORD
varstring20     surname;
string10        forename;
integer2        age := 25;
integer2        score;
integer8        holepos;
            END;

ds1 := dataset('names', namesRecord, THOR);
ds2 := dataset('names', namesRecord, THOR);
ds3 := dataset('names3', namesRecord, THOR);
file1 := ds1 + ds2 + ds3;

cnt1 := count(file1);

myReport := RECORD
   score := file1.score;
   cnt := count(group);
   prcnt := count(group) * 100 / cnt1;
end;

x := table(file1, myReport);

output(x);

