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

idRecord := { unsigned id; };
xRecord := { unsigned id; dataset(idRecord) ids; };

// Inlining the transform
unsigned c0 := 10;
makeTenDs(unsigned start) := dataset(c0, transform(idRecord, self.id := start + counter));
ds0 := dataset([0,1,2,3,5], idRecord);
xRecord t0(idRecord l) := TRANSFORM
    SELF.id := l.id;
    SELF.ids := makeTenDs(l.id);
END;
output(PROJECT(nofold(ds0), t0(LEFT)));

// Inlining removed (0 records)
unsigned c1 := 0;
makeZeroDs(unsigned start) := dataset(c1, transform(idRecord, self.id := start + counter));
ds1 := dataset([0,1,2,3,5], idRecord);
xRecord t1(idRecord l) := TRANSFORM
    SELF.id := l.id;
    SELF.ids := makeZeroDs(l.id);
END;
output(PROJECT(nofold(ds1), t1(LEFT)));

// Identical to above, (negative records)
integer c2 := -10;
makeNegativeDs(unsigned start) := dataset(c2, transform(idRecord, self.id := start + counter));
ds2 := dataset([0,1,2,3,5], idRecord);
xRecord t2(idRecord l) := TRANSFORM
    SELF.id := l.id;
    SELF.ids := makeNegativeDs(l.id);
END;
output(PROJECT(nofold(ds2), t2(LEFT)));

// Identical to above, (negative records in a variable)
integer c3 := -20  : stored('c3');
makeNegativeVarDs(unsigned start) := dataset(c3, transform(idRecord, self.id := start + counter));
ds3 := dataset([0,1,2,3,5], idRecord);
xRecord t3(idRecord l) := TRANSFORM
    SELF.id := l.id;
    SELF.ids := makeNegativeVarDs(l.id);
END;
output(PROJECT(nofold(ds3), t3(LEFT)));

// Valid range, distributed (should not inline)
makeDistributedDs(unsigned start) := dataset(10, transform(idRecord, self.id := start + counter), DISTRIBUTED);
ds4 := dataset([0,1,2,3,5], idRecord);
xRecord t4(idRecord l) := TRANSFORM
    SELF.id := l.id;
    SELF.ids := makeDistributedDs(l.id);
END;
output(PROJECT(nofold(ds4), t4(LEFT)));
