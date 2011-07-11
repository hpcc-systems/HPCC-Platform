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

aaa := DATASET('aaa', {STRING1 f1;}, THOR);
bbb := GROUP(aaa, ALL);


s := ' abc ';
TRIM(s, ALL);


f(SET is, INTEGER i) := i IN is;

MyJobSet := [1,2,3];

BOOLEAN ok := f(ALL, 1);


SORT(aaa, ALL);
