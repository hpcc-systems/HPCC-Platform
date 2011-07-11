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

//UseStandardFiles
//skip type==thor TBD

unsigned lim := 1 : stored('lim');

i1 := DG_FetchIndex1(KEYED(Lname IN ['Anderson']));
i2 := DG_FetchIndex1(KEYED(Lname IN ['Smith']));

ds1 := stepped(limit(i1, lim, keyed, count, SKIP), fname);
ds2 := stepped(limit(i2, lim, keyed, count, SKIP), fname);

output(mergejoin([ds1, ds2], LEFT.fname = right.fname, fname), {fname, lname});

