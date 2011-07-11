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
string1         s1;
string1         s2;
string1         s3;
string1         s4;
string1         s5;
string1         s6;
integer2        age := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);

f := namesTable(s1 = '1', 
                     s2 = '6', 
                     s3 = '5',
                     s4 = '6',
                     s5 = '2',
                     s6 = '0');
                     
output(f, NAMED('test'), EXTEND);
output(f, NAMED('test'), EXTEND);