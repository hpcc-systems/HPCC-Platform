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

//nothor
//nothorlcr
//The library is defined and built in aaalibrary2.ecl

namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

FilterDatasetInterface(dataset(namesRecord) ds, string search, boolean onlyOldies) := interface
    export dataset(namesRecord) matches;
    export dataset(namesRecord) others;
end;


boolean falseval := false: stored('yeahright');

filterDataset(dataset(namesRecord) ds, string search, boolean onlyOldies) := library('aaaLibrary2',FilterDatasetInterface(ds,search,onlyOldies));

namesTable := dataset([
        {'Halliday','Gavin',31},
        {'Halliday','Liz',30},
        {'Jones','John', 44},
        {'Smith','George',75},
        {'Smith','Baby', 2}], namesRecord);

filtered := filterDataset(namesTable, 'Smith', false);
output(if (falseval, filtered.matches),,named('MatchSmith'));

filtered2 := filterDataset(namesTable, 'Halliday', false);
//output(filtered2.others,,named('NotHalliday'));

filtered3 := filterDataset(namesTable, 'Halliday', true);
//output(filtered3.others,,named('OldNotHalliday'));
