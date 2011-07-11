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

rec :=record
string6 ucc_key;
string s;
ebcdic string4 es;
integer i;
unicode u;
end;

d1 := dataset('~in::D&B',rec,csv(terminator('\n')));
d2 := dataset('~in::D&B',rec,csv(ebcdic, terminator('\n')));

d3 := dataset('~thor::in::D&B',rec,csv(ascii, terminator('\n')));
d4 := dataset('~thor::in::D&B',rec,csv(unicode, terminator('\n')));


output(d1);
output(d2);
output(d3);
output(d4);


fpos := dataset([100,200,300,99], { unsigned fpos});

output(fetch(d1, fpos, right.fpos));
output(fetch(d2, fpos, right.fpos));
output(fetch(d3, fpos, right.fpos));
output(fetch(d4, fpos, right.fpos));

ds := dataset('ds', rec, thor);

output(ds,,'csvDefault',csv);
output(ds,,'csvAsEbcdic',csv(ebcdic));
output(ds,,'csvAsAscii', csv(ascii));
output(ds,,'csvAsUnicode', csv(unicode));
