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
string10        forename1;
string10        forename2;
string10        forename3;
string10        forename4;
integer2        age := 25;
            END;

namesTable := dataset('names.d00', namesrecord, THOR);
sortedNames := sort(namesTable, -age, record);
output(sortedNames,,'out.d00');


output(sort(namesTable, -age, record, except age, surname));

output(sort(namesTable, forename, forename4, record));

output(sort(namesTable, except age));
