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

zpr := 
            RECORD
string20        forename;
string20        surname;
unsigned4       age;
            END;

zperson := dataset([
        {'Gavin','Halliday',32},
        {'Jason','Halliday',28},
        {'Liz','Malloy',28},
        {'Liz','Stevenson',32},
        {'James','Mildew',46},
        {'Arther','Dent',60},
        {'Liz','Halliday',31},
        {'Ronald','Regan',84},
        {'Liz','Zappa',12}
        ], zpr);

sortedzperson := sort(zperson, surname, age);

x := choosesets(sortedzperson, surname='Halliday'=>2,forename='Liz'=>3,1,EXCLUSIVE);
output(x);

y := choosesets(sortedzperson, surname='Halliday'=>2,forename='Liz'=>3,1);
output(y);

x1 := choosesets(sortedzperson, surname='Halliday'=>2,forename='Liz'=>3,1,ENTH);
output(x1);

y1 := choosesets(sortedzperson, surname='Halliday'=>2,forename='Liz'=>3,LAST);
output(y1);

