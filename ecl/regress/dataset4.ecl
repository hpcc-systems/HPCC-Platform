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

#option ('optimizeGraph', false);
//Check that spilling continues to read all the records, even if input is terminated.
namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset([
        {'Halliday','Gavin',31},
        {'Halliday','Liz',30},
        {'Halliday','Abigail',0},
        {'Page','John',62},
        {'Page','Chris',26},
        {'Salter','Abi',10},
        {'X','Z'}], namesRecord);

w := sort(namesTable, surname);
x := namesTable(age != 100);

y1 := choosen(x, 1);
y2 := sort(x, forename)(surname <> 'Page');

output(y1);
output(y2);
output(y1);


