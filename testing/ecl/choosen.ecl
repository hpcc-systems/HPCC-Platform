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

fooSet := DATASET([{1},{2},{3},{4}], {INTEGER1 x});

OUTPUT(fooSet);

s2 := CHOOSEN(fooSet, 7, 6);
COUNT(s2);

s3 := CHOOSEN(fooSet, 7, 2);
OUTPUT(s3);

s4 := CHOOSEN(fooSet, ALL, 2);
OUTPUT(s4);
COUNT(s4);


fooSet2 := DATASET([{1},{2},{3},{4},{5}], {INTEGER1 x});

output(fooSet2[6..]);
output(fooSet2[2..9]);
output(fooSet2[2..]);


fooSet3 := DATASET([1,1,1,2,3,3,3,4,4,4,4,4], { integer x });
gr3 := group(fooSet3, x);

output(choosen(gr3, 2));
output(choosen(gr3, 8));
output(table(choosen(gr3, 8), { count(group) }));
output(choosen(gr3, 2, grouped));
output(table(choosen(gr3, 2, grouped), { count(group) }));
output(choosen(gr3, 2, 2, grouped));
output(table(choosen(gr3, 2, 2, grouped), { count(group) }));
output(choosen(gr3, 2, 4, grouped));
output(table(choosen(gr3, 2, 4, grouped), { count(group) }));
