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

numInput := 100000;

r := { unsigned id; unsigned num; };
ds := DATASET(numInput, TRANSFORM(r, SELF.id := COUNTER; SELF.num := (COUNTER % 10) + 10), DISTRIBUTED);

s := SORTED(ds, id);

r2 := RECORD(r)
    unsigned cnum;
END;

idRecord := { unsigned id; };

r2 createMatch(r l) := TRANSFORM
    d := DATASET(l.num, TRANSFORM(idRecord, SELF.id := HASH64(COUNTER)));
    sd := SORT(d, id);
    SELF.cnum := COUNT(NOFOLD(sd));
    SELF := l;
END;

j := JOIN(s, s(id != 0), LEFT.id = RIGHT.id, createMatch(LEFT), LOCAL, UNORDERED);

f := NOFOLD(j)(num != cnum);

CHOOSEN(f, 1); // There should be 0 output rows.
