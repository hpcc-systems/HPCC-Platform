/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

// Tests a workunit result inside a child query,
// generated as side-effect that extends a result

numRecs := 100;

rec := RECORD
 unsigned num;
 unsigned firstNum := 0;
 unsigned groupCount := 0;
 unsigned groupAggregate := 0;
END;

ds := DATASET(numRecs, TRANSFORM(rec, SELF.num := COUNTER%4));

sds := SORT(ds, num, LOCAL);
gds := GROUP(sds, num, LOCAL);

someFunc(DATASET(rec) ss, unsigned groupCount) := FUNCTION
  pr := PROJECT(ss, TRANSFORM({LEFT.num, unsigned total}, SELF.total := LEFT.num + groupCount; SELF := LEFT));
  gpr := GROUP(pr, total);
  o := OUTPUT(gpr, NAMED('cqresult'), EXTEND);
  RETURN WHEN(true, o, SUCCESS);
END;

rec rollupTrans(rec l, DATASET(rec) s) := TRANSFORM
 pr := PROJECT(s, TRANSFORM(rec, SELF.num := LEFT.num*10));
 SELF.firstNum := l.num;
 SELF.groupCount := COUNT(pr);
 SELF.groupAggregate := SUM(pr, num);
 SELF.num := IF(someFunc(CHOOSEN(s, 1), SELF.groupCount), l.num, 0);
END;

r := ROLLUP(gds, GROUP, rollupTrans(LEFT, ROWS(LEFT)));

OUTPUT(r);
