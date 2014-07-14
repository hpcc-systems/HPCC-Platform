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

makeDataset(unsigned start) := dataset(3, transform(idRecord, self.id := start + counter));


ds1 := dataset([0,1,2,3,5], idRecord);

xRecord t(idRecord l) := TRANSFORM
    SELF.id := l.id;
    SELF.ids := sort(CHOOSE(l.id, makeDataset(l.id), makeDataset(l.id+1), makeDataset(l.id+2)), id);
END;

output(PROJECT(ds1, t(LEFT)));
