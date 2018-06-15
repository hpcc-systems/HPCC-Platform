/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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

//Test only makes any sense on thor, test roxie to ensure attributes are ignored
//nohthor

DataRec := RECORD
    UNSIGNED2   f1;
END;

dsTemp := DATASET
    (
        [1, 2, 3, 4, 5],
        DataRec
    );

ds := DISTRIBUTE(dsTemp);

TableRec := RECORD
    REAL8       average := AVE(GROUP, ds.f1);
END;

r1 := TABLE(ds, TableRec, FEW);
OUTPUT(r1, NAMED('Few'));

r2 := TABLE(ds, TableRec, FEW, MERGE);
OUTPUT(r2, NAMED('FewMerge'));

r1x := TABLE(ds, TableRec, f1 DIV 2, FEW);
OUTPUT(SORT(r1x, RECORD), NAMED('FewX'));

r2x := TABLE(ds, TableRec, f1 DIV 2, FEW, MERGE);
OUTPUT(SORT(r2x, RECORD), NAMED('FewMergeX'));

dsTemp2 := NOFOLD(DATASET
    (
        [1, 1, 1],
        DataRec
    ));

TableRec2 := RECORD
    REAL8       average := AVE(GROUP, dsTemp2.f1);
END;

r3 := TABLE(dsTemp2, TableRec2, MERGE);
OUTPUT(r3, NAMED('Merge2'));

r3x := TABLE(dsTemp2, TableRec2, f1, MERGE);
OUTPUT(r3x, NAMED('Merge2x'));


dsTemp3 := NOFOLD(DATASET(1000, transform({string3 x}, SELF.x := INTFORMAT(COUNTER-1, 3, 1)), DISTRIBUTED));

r4 := TABLE(dsTemp3, { cnt := count(group), x[3]+x[1] }, x[3]+x[1], MERGE);
output(r4(cnt != 10));
