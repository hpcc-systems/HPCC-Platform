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

NameFilterLibrary(dataset(namesRecord) ds, string search, boolean onlyOldies) := interface
    export included := ds;
    export excluded := ds;
    export integer version;
    export integer numBoth;
    export string10 firstIncludedName;
end;

filterNames(dataset(namesRecord) ds, string search, boolean onlyOldies) := LIBRARY('NameLibrary_1_0', NameFilterLibrary(ds, search, onlyOldies));

addressRecord := record
string100 addr;
dataset(namesRecord) names;
dataset(namesRecord) names2;
unsigned    version;
unsigned    numBoth;
string10    firstForename;
            end;

namesTable := dataset('x',namesRecord,FLAT);

filtered := filterNames(namesTable, 'Smith', true);
output(filtered.included,,named('Included'));


addressTable := dataset('y', addressRecord, flat);


p := project(addressTable,
            transform(addressRecord,
                    processed := filterNames(left.names, 'Hawthorn', false);
                    self.names := sort(processed.included, surname);
                    self.names2 := sort(processed.excluded, surname);
                    self.version := processed.version;
                    self.numBoth := processed.numBoth;
                    self.firstForename := processed.firstIncludedName;
                    self := left));

output(p);
