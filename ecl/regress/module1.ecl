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
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);





filterDataset(dataset(namesRecord) ds, string search, boolean onlyOldies) := module
    f := ds;
    shared g := if (onlyOldies, f(age >= 65), f);
    export included := g(surname != search);
    export excluded := g(surname = search);
end;


filtered := filterDataset(namesTable, 'Hawthorn', true);
output(filtered.included,,named('Included'));
output(filtered.excluded,,named('Excluded'));

grAggregateDataset(grouped dataset(namesRecord) ds, string search, boolean onlyOldies) := module
    f := table(ds, { cnt := count(group)});
    export included := f(cnt != 0);
end;

grfiltered := grAggregateDataset(group(namesTable, surname), 'Hawthorn', true);
output(grfiltered.included,,named('GrIncluded'));
