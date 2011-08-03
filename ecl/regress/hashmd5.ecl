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
ppersonRecord := RECORD
string10    surname ;
varstring10 forename;
unicode     uf1;
varunicode10 uf2;
decimal10_2  df;
integer4    nl;
  END;


pperson := DATASET([{'','',U'',U'',0,0}], ppersonRecord);

output(pperson, {hashmd5(surname),hashmd5(forename)});
output(pperson, {hashmd5(uf1),hashmd5(uf2)});
output(pperson, {hashmd5(df),hashmd5(nl)});
output(pperson, {data20 hashsurname := hashmd5(surname), data10 hashforename := hashmd5(forename)});


string compareMD5(data16 l, data16 r) :=
    if(l=r,'MD5 match', 'MD5 do not match "' + l + '","' + r);


compareMD5(x'00000000000000000000000000000001',     x'00000000000000000000000000000001');
compareMD5(HASHMD5(''),                             x'd41d8cd98f00b204e9800998ecf8427e');
compareMD5(HASHMD5('a'),                            x'0cc175b9c0f1b6a831c399e269772661');
compareMD5(HASHMD5('abc'),                          x'900150983cd24fb0d6963f7d28e17f72');
compareMD5(HASHMD5('message digest'),               x'f96b697d7cb7938d525a2f31aaf161d0');
compareMD5(HASHMD5('abcdefghijklmnopqrstuvwxyz'),   x'c3fcd3d76192e4007dfb496cca67e13b');
compareMD5(HASHMD5('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'), x'd174ab98d277d9f5a5611c2c9f419d9f');
compareMD5(HASHMD5('12345678901234567890123456789012345678901234567890123456789012345678901234567890'), x'57edf4a22be3c955ac49da2e2107b67a');

