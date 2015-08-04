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

j := cogroup(filteredNames(18, 0), filteredNames(60,1), groupby(surname));

innerJ := having(j, exists(rows(left)(side=0)) and exists(rows(left)(side=1)));

result := rollup(innerJ, group, transform({ string20 surname, unsigned4 cnt}, self.surname := LEFT.surname, self.cnt := count(rows(left))));
output(result);


xJoinRecord := record
    string20 surname;
    dataset(namesRecord) leftDs;
    dataset(addressRecord) rightDs;
end;

xJoinRecord makeJoinRecord(string20 surname, dataset(namesRecord) leftDs, dataset(addressRecord) rightDs) := TRANSFORM
    SELF.surname := surname;
    SELF.leftDs := leftDs;
    SELF.rightDs := rightDs;
END;

lhsRaw := group(namesTable(age < 18), surname, all, sorted);
rhsRaw := group(addressTable(addr = 'No fixed abode'), surname, all, sorted);
lhs := rollup(lhsRaw, group, makeJoinRecord(left.surname, rows(left), dataset([], addressRecord)));
rhs := rollup(rhsRaw, group, makeJoinRecord(left.surname, dataset([], namesRecord), rows(left)));

j2 := cogroup(lhs, rhs, groupby(surname));

innerJ2 := having(j2, exists(rows(left)(exists(leftDs))) and exists(rows(left)(exists(rightDs))));

result2 := rollup(innerJ2, group, transform({ string20 surname, unsigned4 cnt}, self.surname := LEFT.surname, self.cnt := sum(rows(left), count(leftDs) + count(rightDs))));
output(result2);

