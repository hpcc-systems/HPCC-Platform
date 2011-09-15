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

output((string)trim((ebcdic string)'abcdef    '));
output(trim(nofold((ebcdic string)'abcdef    ')));

s1 := 'G';
s2 := 'G ';
d1 := x'34';
d2 := x'3400';
q1 := Q'G';
q2 := Q'G ';
u1 := U'G';
u2 := U'G ';
vu1 := (varunicode)U'G';
vu2 := (varunicode)U'G ';
v1 := V'G';
v2 := V'G ';
u81 := U8'G';
u82 := U8'G ';

dq1 := (>data<)Q'G';
dq2 := (>data<)Q'G ';

xs1 := 'G' : stored('xs1');
xs2 := 'G ' : stored('xs2');
xd1 := x'34' : stored('xd1');
xd2 := x'3400' : stored('xd2');
xq1 := Q'G' : stored('xq1');
xq2 := Q'G ' : stored('xq2');
xu1 := U'G' : stored('xu1');
xu2 := U'G ' : stored('xu2');
xu81 := U8'G' : stored('xu81');
xu82 := U8'G ' : stored('xu82');
varunicode xvu1 := U'G' : stored('xvu1');
varunicode xvu2 := U'G ' : stored('xvu2');
xv1 := 'G' : stored('xv1');
xv2 := 'G ' : stored('xv2');

compareValues(x, y, title) := MACRO
    output('----- ' + title + ' -----');
    output(hash(x));
    output(hash(y));
    output(hash(x) = hash(y));
    output(x <=> y);
    output(trim(x) <=> trim(y));
ENDMACRO;

compareValues(s1, s2, 's1 s2');
compareValues(q1, q2, 'q1 q2');
compareValues(v1, v2, 'v1 v2');
compareValues(xs1, xs2, 'xs1 xs2');
compareValues(xq1, xq2, 'xq1 xq2');
compareValues(xv1, xv2, 'xv1 xv2');
compareValues(s1, q1, 's1 q1');
compareValues(s1, v2, 's1 v2');

compareValues(d1, d2, 'd1 d2');
compareValues(d2, d1, 'd2 d1');
compareValues(xd1, xd2, 'xd1 xd2');
compareValues(dq1, dq2, 'dq1 dq2');

compareValues(u1, u2, 'u1 u2');
compareValues(u81, u82, 'u81 u82');
compareValues(xu1, xu2, 'xu1 xu2');
compareValues(xu81, xu82, 'xu81 xu82');
compareValues(vu1, vu2, 'vu1 vu2');
compareValues(vu1, u2, 'vu1 u2');
compareValues(xvu1, xvu2, 'xvu1 xvu2');
