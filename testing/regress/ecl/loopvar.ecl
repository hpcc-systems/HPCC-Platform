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


#option('warnOnImplicitJoinLimit', false);
import Std;

filename1 := '~someflatfile1';
filename2 := '~someflatfile2';
keyname1 := '~somekey' : STORED('keyname1');
keyname2 := '~someotherkey' : STORED('keyname2');

dummy := 0;
keynamecalc := IF(Std.System.thorlib.Daliserver() = 'dummy', '~somekey' , '~someotherkey');

lVal := RECORD
 INTEGER id := 0;
 STRING val := '';
END;

lValKey := RECORD
 lVal;
 Unsigned8 fpos {virtual(fileposition)} ;
END;

dsVal1 := DATASET(10000, TRANSFORM(lVal, SELF.id := COUNTER; SELF.val := 'val from dsVal1=' + COUNTER;), DISTRIBUTED);
dsVal2 := DATASET(10000, TRANSFORM(lVal, SELF.id := COUNTER; SELF.val := 'val from dsVal2=' + COUNTER;), DISTRIBUTED);

saveit1 := OUTPUT(SORT(DISTRIBUTE(dsVal1, id), id, LOCAL), , filename1, OVERWRITE);
saveit2 := OUTPUT(SORT(DISTRIBUTE(dsVal2, id), id, LOCAL), , filename2, OVERWRITE);

dsKey1 := DATASET(filename1, lValKey, THOR);
dsKey2 := DATASET(filename2, lValKey, THOR);

i1 := INDEX(dsKey1, {id}, {val, fpos}, keyname1, DISTRIBUTED);
i2 := INDEX(dsKey2, {id}, {val, fpos}, keyname2, DISTRIBUTED);
buildit1 := BUILDINDEX(i1, LOCAL, SORTED, OVERWRITE);
buildit2 := BUILDINDEX(i2, LOCAL, SORTED, OVERWRITE);

dsQry := DATASET([{100, ''}], lVal);

lVal loopBody(dataset(lVal) inVal, unsigned4 c) := FUNCTION
 keyname := IF(c%2=1, keyname1, keyname2);
 valKey := INDEX(DATASET([], lValKey), {id}, {val, fpos}, keyname);
 RETURN JOIN(inVal, valKey, LEFT.id = RIGHT.id, TRANSFORM(lVal, SELF := RIGHT));
END;

dsLoop := LOOP(dsQry, 2, loopBody(rows(left), counter));

lVal loopBody2(dataset(lVal) inVal, unsigned4 c) := FUNCTION
 valKey := INDEX(DATASET([], lValKey), {id}, {val, fpos}, keynamecalc);
 RETURN JOIN(inVal, valKey, LEFT.id = RIGHT.id, TRANSFORM(lVal, SELF := RIGHT));
END;

dsLoop2 := LOOP(dsQry, 2, loopBody2(rows(left), counter));

SEQUENTIAL(
 saveit1, saveit2, buildit1, buildit2,
 OUTPUT(dsLoop, named('dsLoop'));
 OUTPUT(dsLoop2, named('dsLoop2'));
);
