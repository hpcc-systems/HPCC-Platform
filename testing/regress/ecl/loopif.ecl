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
#onwarning(2168, ignore); // Disabled warning until it is moved out of parser

rec := RECORD
 unsigned4 i;
 unsigned4 j;
 unsigned4 c := 0;
END;

ds := DATASET([{1, 1}, {2, 2}], rec);

DATASET(rec) loopBody(DATASET(rec) ds, unsigned c) := FUNCTION
 step1 := TABLE(NOFOLD(ds), { i, j, unsigned ni := SUM(GROUP, i); }, j, FEW);
 p1 := PROJECT(ds, TRANSFORM(rec, SELF.i := LEFT.i + 20; SELF.c := c; SELF := LEFT));
 p2 := PROJECT(NOFOLD(step1), TRANSFORM(rec, SELF.i := LEFT.i + 10; SELF.c := c; SELF := LEFT));
 dsret := IF(c % 2 = 1, p1, p2);
 RETURN dsret;
END;

l1 := LOOP(NOFOLD(ds), 4, loopBody(ROWS(LEFT), COUNTER));

OUTPUT(l1);
