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

#option ('targetClusterType','roxie');

ds := nofold(dataset([
            {1, 1}, {1,3}, {1, 5}, {1, 4},
            {2, 10}, {2,8}, {2,3}, {2,5}, {2,8},
            {3, 4}], { unsigned val1, unsigned val2 }));


gr := group(ds, val1);
f1 := having(gr, count(rows(left)) > 1);
output(f1) : independent;

