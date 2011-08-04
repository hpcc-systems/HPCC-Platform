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
namesRecord :=
            RECORD
decimal9_2      v1;
udecimal8_2     v2;
            END;

names := dataset([{1.0,1.0},{+0.0,-1.23},{-10,10.126},{0,12300000000000},{0,0.000001}],namesRecord);

 output(nofold(names),
        {
         v1,v2,
         '!!!!!!!',
         v1<=>v1,v2<=>v2,
         v1<0,
         v1<5,
         v1<-(decimal10_2)0,
         v2<0x0,
         v2<128,
         v1<(decimal10_2)-991234567.89,
         v1*2<v1,v1<v1*2,
         '$',
         -(decimal10_2)0.0 < (decimal10_2)0.0, false,
         (decimal10_2)-8 < (decimal10_2)8, true,
         (decimal10_2)3.21 < (decimal10_2)1.23, false,
         (udecimal10_2)12340.0 < (decimal10_2)0.98, false,
         (udecimal10_2)8.1212 < (decimal10_2)8.1234, false,
         (udecimal10_2)3.21 < (decimal10_2)1.23, false,
         (decimal10_2)12345678.12 < (decimal10_2)9999999.12, false,
         '$'
         },'out.d00');

01234.56D <=> -01234.56D;
-01234.56D <=> 01234.56D;
123456789.123456789D <=> 123456789.123456788D;


