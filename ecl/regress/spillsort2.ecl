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


inputRecord := { unsigned4 id1, unsigned4 id2 };

inputRecord createRecord(unsigned c) := TRANSFORM
    SELF.id1 := (c-1) DIV 10000;
    SELF.id2 := HASH64(c);
END;

unsigned numRows := 20000000;

ds := dataset(numRows, createRecord(COUNTER));

sortedDs := SORT(NOFOLD(ds), id1, id2, UNSTABLE('spillingquicksort'));
counted1 := COUNT(NOFOLD(sortedDs));
output(counted1);

shuffledDs := UNGROUP(SORT(GROUP(NOFOLD(ds), id1),id2, UNSTABLE('spillingquicksort')));
counted2 := COUNT(NOFOLD(shuffledDs));
output(counted2);
