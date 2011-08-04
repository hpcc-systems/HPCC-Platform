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

