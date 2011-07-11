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

namesTable := dataset([
        {'Salter','Abi',10},
        {'Halliday','Gavin',31},
        {'Halliday','Liz',30},
        {'X','Z'}], namesRecord);

person := dataset('person', { unsigned8 person_id, string1 per_sex, string2 per_st, string40 per_first_name, string40 per_last_name}, thor);
thorNamesTable := dataset('names.d00', namesrecord, THOR);
sortedNames := sort(namesTable, -surname, forename);
//output(sortedNames,,'out.d00');

previousSort := sorted(thorNamesTable,(string25)thorNamesTable.forename,(string15)thorNamesTable.surname);

myset := sort(person,person.per_last_name,person.per_first_name,stable('Insertion'));
output(myset);
string algo := '' : stored('algorithm');
//myset2 := sort(namesTable,(string25)namesTable.surname,(string15)namesTable.forename,joined(myset));
myset2 := sort(namesTable,(string25)namesTable.surname,(string15)namesTable.forename,joined(previousSort),unstable(algo));

output(myset2);
