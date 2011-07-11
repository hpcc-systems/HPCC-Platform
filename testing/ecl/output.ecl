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

d := dataset([{1, 'a', 3.4, 5.6, true, false}], {unsigned a, string1 b, real4 c, decimal5_3 de, boolean t, boolean f});

output(d);
output(d, named('named1'));

1;
'a';
3.4;
(decimal5_3) 5.6;
true;
false;

output(1);
output('a');
output(3.4);
output((decimal1_1) 5.6);
output(true);
output(false);

output(1, named('named2'));
output('a', named('named3'));
output(3.4, named('named4'));
output((decimal5_3) 5.6, named('named5'));
output(true, named('named6'));
output(false, named('named7'));
