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
export dataset(namesRecord) included;
export dataset(namesRecord) excluded;
    end;

filterDataset(dataset(namesRecord) ds, string search, boolean onlyOldies) := LIBRARY('NameLibrary_1_0', NameFilterLibrary(ds, search, onlyOldies));

namesTable := dataset('x',namesRecord,FLAT);
namesTable2 := dataset([
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',30},
        {'Smithe','Pru',10},
        {'X','Z'}], namesRecord);

filtered := filterDataset(namesTable, 'Smith', true);
output(filtered.included,,named('Included'));

filtered2 := filterDataset(namesTable, 'Hawthorn', true);
output(filtered2.excluded,,named('Excluded'));


//Error - not all methods impemented
NameFilterLibrary_1_0(dataset(namesRecord) ds, string search, boolean onlyOldies) := module,library(NameFilterLibrary)
    f := ds;
    export dataset(namesRecord) excluded := ds(surname = search);
end;


//Error - library is still abstract
NameFilterLibrary_1_1(dataset(namesRecord) ds, string search, boolean onlyOldies) := module,library(NameFilterLibrary)
    f := ds;
    export dataset(namesRecord) notdefined;
    export dataset(namesRecord) included := notDefined;
    export dataset(namesRecord) excluded := ds(surname = search);
end;



