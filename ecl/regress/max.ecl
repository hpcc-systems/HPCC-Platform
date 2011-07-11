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



calculateMax(any x, any y) := IF (x > y, x, y);
calculateMax4(any x1, any x2, any x3, any x4) := calculateMax(calculateMax(x1, x2), calculateMax(x3, x4));

output(calculateMax(10,5));
output(calculateMax(0.5,0.9));
output(calculateMax('a','z'));


output(calculateMax4(88,1,10,5));
output(calculateMax4(-0.4,0.0,0.5,0.9));
output(calculateMax4('a','zz','','z'));

