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

#STORED('unsignedeight',18446744073709551615)
unsigned8 eight := 0 : stored('unsignedeight');
output(eight);

#STORED('unsignedeightstr','18446744073709551615')
string eight_as_str := '' : stored('unsignedeightstr');
unsigned8 eight_cast := (unsigned8) eight_as_str;

output(eight_cast);

#STORED('x1',0x7fffffffffffffff)
unsigned8 x1 := 0 : stored('x1');
output(x1);

#STORED('x2',0x8000000000000000)
unsigned8 x2 := 0 : stored('x2');
output(x2);

#STORED('x3',0xFFFFFFFFFFFFFFFF)
unsigned8 x3 := 0 : stored('x3');
output(x3);

#STORED('x4',0b0111111111111111111111111111111111111111111111111111111111111111)
unsigned8 x4 := 0 : stored('x4');
output(x4);

#STORED('x5',0b1000000000000000000000000000000000000000000000000000000000000000)
unsigned8 x5 := 0 : stored('x5');
output(x5);

#STORED('x6',0b1111111111111111111111111111111111111111111111111111111111111111)
unsigned8 x6 := 0 : stored('x6');
output(x6);

