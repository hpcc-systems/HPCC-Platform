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


surnameRecord := { string       surname };
surnameField := surnameRecord.surname;

//Really want to come up with a way to allow defaults
filterByName(virtual dataset({ string surname, integer age }) ds, opt string searchName = '', opt integer searchAge = 0) := function
    return ds((searchName = '' or (surname = searchName)), (searchAge = 0 or (age = searchAge)));
END;

namesRecord1 :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable1 := nofold(dataset([
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',30},
        {'Smithe','Pru',10},
        {'X','Z'}], namesRecord1));

output(filterByName(namesTable1, 'Hawthorn',31));
output(filterByName(namesTable1, 'Smithe'));
output(filterByName(namesTable1, 30));

namesRecord2 :=
            RECORD
string20        lastname;
string10        firstname;
integer2        age := 25;
            END;

namesTable2 := nofold(dataset([
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',30},
        {'Smithe','Pru',10},
        {'X','Z'}], namesRecord2));

output(filterByName(namesTable2 { surname := lastname }, 'Hawthorn',31));
output(filterByName(namesTable2 { surname := lastname }, 'Smithe'));
output(filterByName(namesTable2 { surname := lastname }, 30));
