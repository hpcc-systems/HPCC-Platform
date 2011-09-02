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
//Make sure both of these don't lose the extra digit.
output((string)(round(9.9D)) + '\n');
output((string)(round(5D, -1)) + '\n');


output((string)(round(nofold(1.1D), 0)) + '\n');
output((string)(round(nofold(1.1D), 1)) + '\n');
output((string)(round(nofold(1.1D), -1)) + '\n');


output((string)(round(1234567, -2)) + '\n');
output((string)(round(nofold(1234567), -2)) + '\n');
