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
#option ('foldAssign', false);
#option ('globalFold', false);
import dt;

namesRecord := 
            RECORD
string20        surname;
string10        phone;
ebcdic string10 ephone;
varstring10     vphone;
dt.pstring      pphone;
data10          dphone;

integer2        did := 25;
string2         short_match;
string10        ssn;
            END;

namesTable := dataset('x',namesRecord,FLAT);

h10 := namesTable;
h7 := namesTable;

FindPhone(set of string10 fp, set of string7 sp ) := 
  dedup( 
    sort ( 
       h10(phone IN fp)+h7(phone IN sp)
    ,did,-short_match,-ssn )
    , did );


x := FindPhone(['1234567890','123456789 ', '12345     '],['1234567','1234   ','GavinHa']);

//output(x,,'out.d00');


y1 := namesTable(ephone IN ['1234567','12345  ','123    ']);
y2 := y1(vphone IN ['1234567','12345  ','123    ']);
y3 := y2(pphone IN ['1234567','12345  ','123    ']);
y4 := y3(dphone IN [D'1234567',D'12345  ',D'123    ']);
y5 := y4((string)dphone IN ['1234567','12345  ','123    ']);
y6 := y5(dphone IN [(data7)'1234567',(data7)'12345  ',(data7)'123    ']);

output(y6,,'out2.d00');

boolean ePhoneInList(ebcdic string10 ephone, set of string7 l) := (ephone IN l);
boolean ePhoneInEList(ebcdic string10 ephone, set of ebcdic string7 l) := (ephone IN l);
boolean vPhoneInList(varstring10 vphone, set of string7 l) := vphone IN l;
boolean pPhoneInList(string pphone, set of string7 l) := pphone IN l;
boolean dPhoneInList(data dphone, set of data7 l) := dphone IN l;

z1 := namesTable(ephoneInList(ephone, ['1234567','12345  ','123    ']));
z2 := z1(vphoneInList(vphone, ['1234567','12345  ','123    ']));
z3 := z2(pphoneInList(pphone, ['1234567','12345  ','123    ']));
z4 := z3(ephoneInEList(ephone, ['1234567','12345  ','123    ']));
z5 := z4(dphoneInList(dphone, [D'1234567',D'12345  ',D'123    ']));

output(z5,,'out.d00');
