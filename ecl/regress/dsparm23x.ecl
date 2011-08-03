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

// A join example

r1 := record
 String10 first_name1;
 string20 last_name1;
end;

r2 := record
 String10 first_name2;
 string20 last_name2;
end;

ds := dataset('ds', r1, FLAT);
dsx := dataset('dsx', r2, FLAT);

rec := record
  string10 id;
end;

rec tranx(r1 l, r2 r) := transform
  self.id := l.first_name1;
end;


myjoin := Join(ds, dsx, left.first_name1 = right.first_name2, tranx(left,right));

count(myjoin);

