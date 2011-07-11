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

rawfile1a := DATASET([{'CHAPMAN'},{'HALLIDAY'},{'RUSHTON'},{'BAYLISS'}], { 
string25 xlname});
outputraw1 := OUTPUT(rawfile1a,,'RKC::TestKeyedJoin', OVERWRITE);

rawfile1 := DATASET('RKC::TestKeyedJoin', { string25 xlname, UNSIGNED8 
__filepos {virtual(fileposition)}}, FLAT);

index1 := INDEX(rawfile1, {xlname,__filepos}, 'RKC::lname.key');
bld1 := BUILDINDEX(index1, OVERWRITE);

rawfile2a := DATASET([{'CHAPMAN'},{'SMITH'},{'BAYLISS'}], { string25 lname});
outputraw2 := OUTPUT(rawfile2a,,'RKC::TestKeyedJoin2', OVERWRITE);
rawfile2 := DATASET('RKC::TestKeyedJoin2', { string25 lname, UNSIGNED8 
__filepos {virtual(fileposition)}}, FLAT);

rawfile2 doJoin(rawfile2 l) := TRANSFORM
            SELF := l;
            END;

rawfile2 doJoin1(rawfile2 l, rawfile1 r) := TRANSFORM
            SELF := l;
            END;

hkjoin := JOIN(rawfile2, index1, LEFT.lname=RIGHT.xlname AND RIGHT.xlname[7] != 'N', doJoin(LEFT), LEFT OUTER);
fkjoin := JOIN(rawfile2, rawfile1, LEFT.lname=RIGHT.xlname AND RIGHT.xlname[7] != 'N', doJoin1(LEFT, RIGHT), LEFT OUTER, KEYED(index1));
out1 := output(hkjoin);
out2 := output(fkjoin);

SEQUENTIAL(
  outputraw1, 
  bld1, //<--- remove the comment and you get an internal error from the code generator
  outputraw2, 
  out1,
  out2
)
