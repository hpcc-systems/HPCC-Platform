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

r1 := record
  data8 d;
  end;

dr1 := dataset('fred',r1,flat);
dr2 := dataset('ethel',r1,flat);

r1 proj(r1 l,r1 r) := transform
self := l;
  end;

j := join(dr1,dr2,(data2)left.d=(data2)right.d AND left.d[3] =
right.d[3], proj(left,right));

output(j)