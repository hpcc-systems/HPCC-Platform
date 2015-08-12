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
