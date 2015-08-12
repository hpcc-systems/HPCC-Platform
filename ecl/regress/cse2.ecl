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

didRec := record
unsigned6   did;
        end;


namesRecord :=
            RECORD
unsigned        jobid;
dataset(didRec) didin;
dataset(didRec) didout;
            END;


func(dataset(didRec) didin) := function

ds0 := dedup(didin, did, all);
ds1 := __COMMON__(ds0);
ds := ds0;
f1 := ds(did != 0);
f2 := ds(did = 0);

c := nofold(f1 + f2);

return sort(c, did);
end;


namesTable := dataset('x',namesRecord,FLAT);


namesRecord t(namesRecord l) := transform

    self.didout := func(l.didin);
    self := l;
    end;


output(table(namesTable, t(namesTable)));
