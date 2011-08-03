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
string      surname;
string      forename;
ebcdic string eforename;
varstring   vsurname;
            END;

namesTable := dataset('x',namesRecord,FLAT);


namesRecord2 :=
            RECORD
string          fullname := namesTable.forename + ' ' + namesTable.surname;
string          shortname := namesTable.forename[1] + '.' + namesTable.surname;
string          forename := namesTable.eforename;
varstring       vfullname := namesTable.vsurname + ', ' + namesTable.forename;
varstring       vsurname := namesTable.surname;
            END;

output(table(namesTable,namesRecord2),,'out.d00');
