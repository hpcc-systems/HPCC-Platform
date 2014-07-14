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

inRecord := { unsigned id; dataset(idRecord) ids; };

inDs := DATASET([{1,[3,5]},{2,[7,11]}], inRecord);

idRecord tId(unsigned id, delta) := TRANSFORM
    SELF.id := id + delta;
END;

inRecord t(inRecord l) := TRANSFORM
    p1 := PROJECT(l.ids, tId(LEFT.id, 3));
    p2 := PROJECT(l.ids, tId(LEFT.id, 7));

    SELF.ids := COMBINE(p1, p2, TRANSFORM(idRecord, SELF.id := LEFT.id * RIGHT.id));  // (x+3)*(x+7)
    SELF := l;
END;

output(PROJECT(inDs, t(LEFT)));
