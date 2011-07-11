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
big_endian integer2     age := 25;
integer2        age2 := 25;
integer8        age8 := 25;
real8           salary := 0;
ebcdic string20 surname;
string10        forename;
unsigned8       filepos{virtual(fileposition)}
            END;

namesTable := dataset('x',namesRecord,FLAT);

i := index(namesTable, { age, surname }, {namesTable }, 'i', distributed);
build(i);
