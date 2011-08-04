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

aaa := DATASET('aaa',{STRING1 fa; }, FLAT);
/*
distibuted_aaa := DISTRIBUTE(aaa, RANDOM());
distibuted_aaax := DISTRIBUTED(aaa, RANDOM());

/*
//OUTPUT(SORT(aaa, fa, LOCAL));
OUTPUT(SORT(distibuted_aaa, fa, LOCAL));
OUTPUT(SORT(distibuted_aaax, fa, LOCAL));
*/
/*
bbb := DATASET('aaa',{STRING1 fb; }, FLAT);

RECORD ResultRec := RECORD
    aaa.fa;
    END;

ResultRec Trans(aaa x, bbb y) := TRANSFORM
    SELF := x;
    END;

r1 := JOIN(aaa, bbb, aaa.fa = bbb.fb, LOCAL);
r2 := SORT(distibuted_aaa, f1, LOCAL);
r3 := SORT(distibuted_aaax, f1, LOCAL);
*/
