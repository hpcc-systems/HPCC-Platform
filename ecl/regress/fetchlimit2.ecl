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

#option ('foldAssign', false);
#option ('globalFold', false);
#option ('targetClusterType', 'hthor');
#option ('pickBestEngine', false);
#option ('optimizeGraph', true);

d := dataset('~local::rkc::person', { string15 name, unsigned8 dpos {virtual(fileposition)}}, flat);
d2 := dataset('~local::rkc::person', { string15 name, unsigned8 dpos {virtual(fileposition)}}, csv);

i := index(d, { string10 zname, string15 f_name := name, unsigned8 fpos{virtual(fileposition)} } , '~local::key::person');

//ii := choosen(i(f_name NOT IN ['RICHARD','SARAH'] or f_name between 'GAVIN' AND 'GILBERT'), 10);

dd := record
  string15 lname;
  string15 rname;
  unsigned8 lpos;
  unsigned8 rpos;
end;

dd xt(D l, i r) := TRANSFORM
sELF.lname := l.name;
sELF.rname := r.f_name;
sELF.lpos := l.dpos;
sELF.rpos := r.fpos;
END;

x0 := fetch(d, i(f_name='Gavin1'), RIGHT.fpos, xt(LEFT, RIGHT));
x1 := x0(lname <> 'x');
output(x1,,'a');

w0 := fetch(d, i(f_name='Gavin2'), RIGHT.fpos, xt(LEFT, RIGHT));
w1 := w0(lname <> 'x');
w2 := limit(w1, 100);
output(w2);

y0 := fetch(d, i(f_name='Gavin3'), RIGHT.fpos, xt(LEFT, RIGHT));
y1 := limit(y0, 200);
y2 := y1(lname <> 'x');
output(y2);

z0 := fetch(d, i(f_name='Gavin4'), RIGHT.fpos, xt(LEFT, RIGHT));
z1 := z0(lname <> 'x');
z2 := limit(z1, 200);
z3 := z2(rname <> 'y');
output(z3);

boolean useAll := true : stored('useAll');

m3 := fetch(d, i(f_name='Gavin5'), RIGHT.fpos, xt(LEFT, RIGHT));
m4 := fetch(d, i(f_name='Gavin6'), RIGHT.fpos, xt(LEFT, RIGHT));

m0 := if(useAll, m3, m4);
m1 := limit(m0, 100);
output(m1);

n0 := if(useAll, y0, z0);
n1 := limit(n0, 100);
output(n1);
