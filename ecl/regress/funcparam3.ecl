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

#option ('pickBestEngine', false);

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;


datasetActionPrototype(dataset(namesRecord) in) := in;
noAction(dataset(namesRecord) in) := in;
limitAction(dataset(namesRecord) in) := limit(in, 500);
limitSkipAction(dataset(namesRecord) in) := limit(in, 500, skip);
sortAction(dataset(namesRecord) in) := sort(in, surname);

processDs(dataset(namesRecord) in, datasetActionPrototype actionFunc = noAction) := function

x := in(age != 0);

y := actionFunc(x);

return table(y, { surname, forename });

end;



namesTable := dataset('x',namesRecord,FLAT);
output(processDs(namesTable));
output(processDs(namesTable, limitAction));
output(processDs(namesTable, limitSkipAction));
output(processDs(namesTable, sortAction));
