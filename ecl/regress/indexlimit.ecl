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

#option ('globalFold', false);
#option ('targetClusterType', 'hthor');
#option ('pickBestEngine', false);

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

x1 := i(f_name <> 'x');
output(x1,,'a');

w1 := i(f_name <> 'x');
w2 := limit(w1, 100);
output(w2);

y1 := limit(i, 200);
y2 := y1(f_name <> 'x');
output(y2);

z1 := i(f_name <> 'x');
z2 := limit(z1, 200);
z3 := z2(f_name <> 'y');
output(z3);


e1 := i(keyed(f_name <> 'e'));
e2 := limit(e1, 200, keyed, count);
e3 := e2(keyed((integer)zname *3 > 9));
e4 := limit(e3, 2000);
e5 := e4(zname <> '');
output(e5);
