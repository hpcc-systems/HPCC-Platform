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
        {'Gavin','Hawthorn',32},
        {'Jason','Hawthorn',28},
        {'Mia','Malloy',28},
        {'Mia','Stevenson',32},
        {'James','Mildew',46},
        {'Arther','Dent',60},
        {'Mia','Hawthorn',31},
        {'Ronald','Regan',84},
        {'Mia','Zappa',12}
        ], zpr);


x := choosesets(zperson, surname='Hawthorn'=>2,forename='Mia'=>3,1,EXCLUSIVE);
output(x,,'out1.d00');

y := choosesets(zperson, surname='Hawthorn'=>2,forename='Mia'=>3,1);
output(y,,'out2.d00');

x1 := choosesets(zperson, surname='Hawthorn'=>2,forename='Mia'=>3,1,ENTH);
output(x1,,'out3.d00');

y1 := choosesets(zperson, surname='Hawthorn'=>2,forename='Mia'=>3,LAST);
output(y1,,'out4.d00');

