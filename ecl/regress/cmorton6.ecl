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

r := record
unsigned id;
    end;
namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
dataset(r)      recs;
            END;

namesTable1 := dataset('x1',namesRecord,FLAT);

output(namesTable1);

nullTable := dataset('x1',namesRecord,FLAT)(false) : stored('x', few);

doZ := function

output(nullTable);
return (namesTable1);
end;

boolean cond := false : stored('cond');

if (cond,
parallel(
output(nullTable),
output(namesTable1)
));



