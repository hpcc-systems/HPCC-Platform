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

namesRecord :=
            RECORD
string20        surname := '?????????????';
string10        forename := '?????????????';
integer2        age := 25;
            END;

addressRecord :=
            RECORD
string30        addr := 'Unknown';
string20        surname;
            END;


namesTable := dataset('names1', namesRecord, thor);
addressTable := dataset('address', addressRecord, thor);

namesRecordEx := RECORD(namesRecord)
    unsigned1 side;
END;

filteredNames(unsigned searchage, unsigned1 side) := function
    f := namesTable(age = searchAge);
    return project(f, transform(namesRecordEx, self := LEFT; SELF.side := side));
end;

j := cogroup(dataset([], namesRecordEx), filteredNames(60,1), groupby(surname));

innerJ := having(j, exists(rows(left)(side=0)) and exists(rows(left)(side=1)));

result := rollup(innerJ, group, transform({ string20 surname, unsigned4 cnt}, self.surname := LEFT.surname, self.cnt := count(rows(left))));
output(result);

