/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

outrec := record
    integer id;
    string20 desc;
end;

aData := DATASET([{1234}],{ integer id});
bData := DATASET([{1234,'BigRed'}],outrec);

outrec xform(aData l, bData r) := transform
    self.id := l.id;
    self.desc := if (r.desc <> '', r.desc, '');
end;

zero := 0 : stored('zero');
one := 1 : stored('one');

abData1 := join(aData, bData,left.id = right.id, xform(LEFT,RIGHT), left outer, KEEP(1), LIMIT(0));
abData2 := join(aData, bData,left.id = right.id, xform(LEFT,RIGHT), left outer, KEEP(1), LIMIT(2));
abData3 := join(aData, bData,left.id = right.id, xform(LEFT,RIGHT), left outer, KEEP(1), LIMIT(one));
abData4 := join(aData, bData,left.id = right.id, xform(LEFT,RIGHT), left outer, KEEP(1), LIMIT(zero));

sequential(
output(abData1);                                                         
output(abData2);
output(abData3);
output(abData4);
);                                                         
