/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

rawfile1a := DATASET([{'DRIMBAD'},{'HALLIDAY'},{'RUSHTON'},{'BAYLISS'}], {
string25 xlname});
outputraw1 := OUTPUT(rawfile1a,,'RKC::TestKeyedJoin', OVERWRITE);

rawfile1 := DATASET('RKC::TestKeyedJoin', { string25 xlname, UNSIGNED8
__filepos {virtual(fileposition)}}, FLAT);

index1 := INDEX(rawfile1, {xlname,__filepos}, 'RKC::lname.key');
bld1 := BUILDINDEX(index1, OVERWRITE);

rawfile2a := DATASET([{'DRIMBAD'},{'SMITH'},{'BAYLISS'}], { string25 lname});
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
