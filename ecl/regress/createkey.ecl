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
data10          forename;
qstring10       city;
integer2        age;
big_endian integer3 salary;
unsigned4       id;
string          notes;
            end;

namesRecordEx := record(namesRecord)
big_endian unsigned6 filepos{virtual(fileposition)};
            END;


empty := dataset(row(transform(namesRecord, self := [])));

forenames := ['Gavin', 'Emma', 'John'];
surnames := ['Halliday', 'Hicks', 'Jones'];
salaries := [15000,20000,19000,40000];
cities := ['Rome','London','Mousehole'];
allnotes := ['', 'Watch list','','Xy13', 'Visa violation'];
addForenames := normalize(empty, 3, transform(namesRecord, self.forename := (data)forenames[counter%3+1]; self := left));
addSurnames := normalize(addForenames, 3, transform(namesRecord, self.surname := surnames[counter%3+1]; self := left));
addCities := normalize(addSurnames, 3, transform(namesRecord, self.city := cities[counter%3+1]; self := left));
addNotes := project(addCities, transform(namesRecord, self.notes := allnotes[counter%9+1]; self := left));
addExtra := project(addNotes, transform(namesRecord, self.salary:= salaries[counter%4+1]; self.age := (counter % 10)+30; self.id := counter; self := left));
output(addExtra,,'~testing::rawdata',overwrite);

namesTable := dataset('~testing::rawdata',namesRecordEx,FLAT);
i1 := index(namesTable, { surname, forename, city, age, salary, id }, { notes, filepos }, '~testing::rawindex');
buildindex(i1,overwrite);
i2 := index(namesTable, { surname, forename, city, age, salary, id }, { string nodes{blob} := namesTable.notes, filepos }, '~testing::blobindex');
buildindex(i2,overwrite);
