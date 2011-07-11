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

unsigned8 max_u8 := 18446744073709551615; // 2^64 - 1
unsigned8 max_i8 := 9223372036854775807; // 2^63 - 1
unsigned8 minbad := max_i8+1; // 2^63
unsigned8 max_u8s := 18446744073709551615 : independent; // 2^64 - 1

output(max_u8, named('max_u8'));
output((real8)max_u8, named('r_max_u8')); // bad
output((real8)max_u8s, named('r_max_u8s'));

output(max_i8, named('max_i8'));
output((real8)max_i8, named('r_max_i8')); // good

output(minbad, named('minbad'));
output((real8)minbad, named('r_minbad')); // bad

output((decimal30_0)max_u8, named('d_max_u8')); // bad
output((utf8)max_u8, named('u8_max_u8')); // bad
output((unicode)max_u8, named('u_max_u8')); // bad
output((string)(qstring)max_u8, named('q_max_u8')); // bad

output((decimal30_0)max_u8s, named('d_max_u8s')); // bad


output((packed unsigned)max_u8s, named('p_max_u8s')); // bad
