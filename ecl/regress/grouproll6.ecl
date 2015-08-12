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
//This should be legal, but we need to revisit the way items are serialied to the keyed join slave to handle it.

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);



i := index({ string20 surname, unsigned8 id }, 'i');

groupedRecord := record
    string20 surname;
    dataset(recordof(i)) people{maxcount(90)};
end;


gr := group(namesTable, surname, all);

results(dataset(namesRecord) l) := function
    slimmed := table(l, {surname});
    return join(slimmed,i,RIGHT.surname IN SET(l(surname != LEFT.surname), surname), transform(right));
end;

r := rollup(gr, group, transform(groupedRecord, self.people := results(rows(left)); self := left));
output(r);
