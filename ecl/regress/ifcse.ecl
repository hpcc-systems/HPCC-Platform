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

idRec := { unsigned id; };

inRecord := { unsigned id; dataset(idRec) ids; };

outRecord := RECORD(inRecord)
    UNSIGNED score;
END;

mkIds(unsigned c) := DATASET(4, transform(idRec, SELF.id := c + COUNTER));

inDs := DATASET(10, transform(inRecord, SELF.id := COUNTER; SELF.ids := mkIds(COUNTER)));

outRecord t1(inRecord l) := TRANSFORM
    complexExpr := (l.id * (l.id + 1));
    complexRow := NOFOLD(ROW(TRANSFORM(idRec, SELF.id := complexExpr)));
    complex := ComplexRow.id;

    SELF.score := complex + COUNT(l.ids(id = complex));
    SELF := l;
END;

output(PROJECT(nofold(inDs), t1(LEFT)));

outRecord t2(inRecord l) := TRANSFORM
    complexExpr := (l.id * (l.id + 1));
    complexRow := NOFOLD(ROW(TRANSFORM(idRec, SELF.id := complexExpr)));
    complex := ComplexRow.id;

    SELF.score := COUNT(l.ids(id = complex)) + complex;
    SELF := l;
END;

output(PROJECT(nofold(inDs), t2(LEFT)));
