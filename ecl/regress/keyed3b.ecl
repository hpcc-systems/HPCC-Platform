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
unsigned8       fileposition{virtual(fileposition)}
            END;

namesTable := dataset('x',namesRecord,FLAT);

i := index(namesTable, { namesTable }, 'i');

set of string10 searchForenames := all : stored('searchForenames');

filterByAge(dataset(recordof(i)) in) := in(keyed(age = 10, opt));


output(filterByAge(i(keyed(surname='Hawthorn'),WILD(forename))));
//output(filterByAge(i(keyed(surname='Hawthorn' and forename='Gavin'))));
//output(filterByAge(i(keyed(surname='Hawthorn' and forename in searchForenames))));

