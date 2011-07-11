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

export DataLib := SERVICE
  string    PreferredFirst(const string scr)  : c, pure, entrypoint='dataCalcPreferredFirst'; 
END;

layout :=
RECORD
    STRING str;
END;


ds1 := DATASET([{'MARK'}, {'ED'}], layout);
ds2 := DATASET([{'MARK'}, {'ED'}], layout);
s1 := SORT(DISTRIBUTE(ds1, HASH(str)), str, local);
s2 := SORT(DISTRIBUTE(ds2, HASH(str)), str, local);
ss1 := SORTED(s1, str, local);
ss2 := SORTED(s2, str, local);



p1 := SORT(DISTRIBUTE(ds1, HASH(datalib.preferredfirst(str))), datalib.preferredfirst(str), local);
p2 := SORT(DISTRIBUTE(ds2, HASH(datalib.preferredfirst(str))), datalib.preferredfirst(str), local);
sp1 := SORTED(p1, datalib.preferredfirst(str), local);
sp2 := SORTED(p2, datalib.preferredfirst(str), local);

j := JOIN(ss1, ss2, LEFT.str = RIGHT.str, LOCAL);
jp := JOIN(sp1, sp2, datalib.preferredfirst(LEFT.str) = datalib.preferredfirst(RIGHT.str), LOCAL);

//output(j);
output(jp);
