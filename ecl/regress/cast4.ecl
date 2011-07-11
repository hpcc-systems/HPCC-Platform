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

STRING30 rid := (STRING30)123456789;
output(rid[1..10]); // =''
output(rid[20..30]);

real r := 1.23456;
output((string1)r);
output((string5)r);
output((string10)r);

integer i := -12345;

output((string1)i);
output((string5)i);
output((string6)i);
output((string10)i);

unsigned u := 12345;

output((string1)u);
output((string5)u);
output((string6)u);
output((string10)u);
