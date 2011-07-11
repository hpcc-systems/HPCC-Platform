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


i := dataset([{1}, {3}, {5}, {7}, {9}, {2}, {4}, {6}, {8}, {10}], {unsigned1 d});

p := project(nofold(i), transform({ string100000 s, unsigned d}, self.s := (string)LEFT.d + 'xxx' + (string)LEFT.d; self.d := LEFT.d; ));

dist := distribute(nofold(p), hash32(d));

agg1 := TABLE(nofold(dist), { d, count(group, s != '88xxx88') }, d, few);

agg2 := TABLE(nofold(dist), { d, count(group, s != '88xxx88') }, d, few, local);

sequential(
    output(agg1);
    output(agg2)
);

