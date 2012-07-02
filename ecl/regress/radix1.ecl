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

rec := record
      string10  key;
      string10  seq;
      string80  fill;
       end;

cvt(string1 x) := (>unsigned1<)x-32;
scale(integer x, string1 y) := (x * 95 + cvt(y));
radix(string10 key) := scale(scale(scale(cvt(key[1]), key[2]), key[3]), key[4]);
divisor := global((95*95*95*95+CLUSTERSIZE-1) DIV CLUSTERSIZE);

in := DATASET('nhtest::terasort1',rec,FLAT);

// radix sort (using distribute then local sort)
d := DISTRIBUTE(in,radix(key) DIV divisor);
s := SORT(d,key,LOCAL,UNSTABLE);

OUTPUT(s,,'terasortrad_out',OVERWRITE);
