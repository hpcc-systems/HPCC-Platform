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

#option ('optimizeGraph', false);
#option ('globalFold', false);
string15 name1 := 'LOR' : stored('LORRAINE');

d := dataset('~local::rkc::person', { string15 name, unsigned8 dpos {virtual(fileposition)}}, flat);

i := sorted(index(d, { string15 f_name := name, unsigned8 fpos } , 'key::person'));

//ii := choosen(i(f_name>='RICHARD', f_name <= 'SARAH', f_name <>'ROBERT', f_name[1]='O'), 10);
ii := i(f_name IN ['RICHARD A']);

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

d xt1(D l) := TRANSFORM
sELF.name := 'f';
SELF := l;
END;

x := fetch(d, ii, RIGHT.fpos, xt(LEFT, RIGHT));
output(choosen(x, 1000000));
output(choosen(sort(i(f_name >= name1), f_name), 10));
//output(choosen(sort(i(f_name >= 'LOR'), f_name), 10));
//output(project(d(name='RIC'), xt1(LEFT)));
//count(d(name='RIC'));
