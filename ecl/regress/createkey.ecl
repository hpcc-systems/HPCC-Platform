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
surnames := ['Hawthorn', 'Hewit', 'Jones'];
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
