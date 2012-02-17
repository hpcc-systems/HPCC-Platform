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

ds1 := DATASET('ds1', { unsigned i }, thor);
f1 := assert(ds1, i != 10);
output(f1);

ds2 := DATASET('ds2', { unsigned i }, thor);
f2 := assert(ds2, i != 10 or true);
output(f2);

ds3 := DATASET('ds3', { unsigned i }, thor);
f3 := assert(ds3, i != 10 and false);
output(f3);
