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


i3 := dataset([{'a', 1}, {'c',3}, {'b',5}, {'c',7}, {'a',9}, {'b',2}, {'d',4}, {'a',6}, {'a',8}, {'b',10}, {'d',11}], {string1 l, unsigned1 d});
g3 := group(i3, d % 2);
t1 := topn(g3, 2, l);
output(t1);     // expect {'a',1},{'a',6'}
output(table(t1, { count(group) }));        // expect {1,1}


output(sort(topn(i3, 2, l, best('a'), local), l, d, local));       // expect {'a',1},{'a',9} - terminate early...
output(topn(i3, 1, -l, best('d'), local));      // expect {'d',4}
b1 := topn(g3, 2, l, best('a'));
output(sort(b1, l, d));     // expect {'a',1},{'a',9'},{'a',6},{'a',8},{'d',11}
output(table(b1, { count(group) }));        // expect {1,1}
