/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
