/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

rec1 := RECORD
 INTEGER id := 0;
END;

rec2 := RECORD
 unsigned id;
 DATASET(rec1) children;
END;

ds := DATASET([{1, [{50}, {40}, {30}, {20}, {10}]}, {0, [{100}, {90}, {80}, {70}, {60}]}, {1, [{100}, {90}, {80}, {70}, {60}]}, {0, [{100}, {90}, {80}, {70}, {60}]}], rec2);

rec1 loopBody(dataset(rec1) inVal, unsigned4 c) := FUNCTION
 s := SORT(inVal, id);
 RETURN PROJECT(s, TRANSFORM(rec1, SELF.id := LEFT.id +1));
END;

rec2 myf(rec2 l, unsigned c) := TRANSFORM
 newchildren := LOOP(l.children, 4, loopBody(ROWS(LEFT), COUNTER));
 SELF.children := IF(l.id>0, newchildren, l.children);
 SELF := l;
END;

OUTPUT(NORMALIZE(NOFOLD(ds), 10, myf(LEFT, COUNTER)));
