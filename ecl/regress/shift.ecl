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

#option ('globalFold', true);

integer one := 1                            : stored('one');
integer n1 := 0x12345                       : stored('n1');
unsigned8 n2 := 0xFEDCBA9876543210          : stored('n2');
integer8 n3 := 0xFEDCBA9876543210           : stored('n3');


output('**'+(string)0x400+'**');
output(1 << 10);
output(one << 10);
output('**'+(string)0x1234+'**');
output((unsigned8)0x12345 >> 4);
output(n1 >> 4);
output('**'+(string)(unsigned8)0x0FEDCBA987654321+'**');
output((unsigned8)0xFEDCBA9876543210 >> 4);
output(n2 >> 4);
output('**'+(string)0xFFEDCBA987654321+'**');
output((integer8)0xFEDCBA9876543210 >> 4);
output(n3 >> 4);
