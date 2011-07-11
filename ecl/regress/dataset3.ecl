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

//Check that reads from grouped input, where all of a group is removed
//don't add an extra end of group.

namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset([
        {'Halliday','Gavin',33},
        {'Halliday','Liz',32},
        {'Halliday','Abigail',0},
        {'Page','John',62},
        {'Page','Chris',26},
        {'Salter','Abigail',13},
        {'X','Za'}], namesRecord);

x := group(namesTable, surname);// : persist('~GroupedNames2');

y := table(x, { countAll := count(group)});

output(y);

z := x(surname <> 'Halliday');


output(z, { countNonHalliday := count(group)});
