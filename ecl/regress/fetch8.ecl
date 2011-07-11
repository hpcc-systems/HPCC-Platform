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

string2 x1 := '' : stored('x1');
boolean x2 := false : stored('x1');

//xName := if(x1='on', 'x', 'y');
xName := if(x2, 'x', 'y');

d := dataset('~local::rkc::person'+xName, { string15 name, unsigned8 dpos {virtual(fileposition)}}, flat);

i := index(d, { string10 zname, string15 f_name := name, unsigned8 fpos{virtual(fileposition)} } , '~local::key::person');

//ii := choosen(i(f_name NOT IN ['RICHARD','SARAH'] or f_name between 'GAVIN' AND 'GILBERT'), 10);

myFilter := i.f_name between 'GAVIN' AND 'GILBERT';
ii := choosen(i((f_name[1]!='!') AND (myFilter or f_name[1..2]='XX' or 'ZZZ' = f_name[..3])), 10);      //should be or but not processed yet.

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

z := fetch(d, ii, RIGHT.fpos);//, xt(LEFT, RIGHT));
output(z);