/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems.

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

//Temporarily disable tests on hthor/thor, until activity is implemented
//nohthor
//nothor

rawRecord := { real id; };

quantRec := RECORD(rawRecord)
    UNSIGNED quant;
END;

rawRecord createRaw(REAL id) := TRANSFORM
    SELF.id := id;
END;

inRecord := RECORD
    UNSIGNED rid;
    UNSIGNED whichPercentile;
    DATASET(rawRecord) ids;
END;

quantRec createQuantile(rawRecord l, UNSIGNED quant) := TRANSFORM
    SELF := l;
    SELF.quant := quant;
END;

createDataset(unsigned cnt, real scale, unsigned delta = 0) := FUNCTION
    RETURN NOFOLD(SORT(DATASET(cnt, createRaw((COUNTER-1) * scale + delta), DISTRIBUTED), HASH(id)));
END;

inRecord createIn(unsigned rid, unsigned whichPercentile, unsigned cnt, real scale, unsigned delta) := TRANSFORM
    SELF.rid := rid;
    SELF.whichPercentile := whichPercentile; 
    SELF.ids := createDataset(cnt, scale, delta);
END;


inDs := DATASET([
            createIn(1, 2, 10, 1, 1),
            createIn(2, 13, 50, 0.3, 1),
            createIn(3, 15, 10, 15, 1),
            createIn(4, 3, 32767, 1, 1),
            createIn(5, 27, 99, 0.03, 1)
            ]);

//Check quantile inside a child query
inRecord t(inRecord l) := TRANSFORM
    SELF.ids := QUANTILE(l.ids, 100, { id }, RANGE([l.whichPercentile]));
    SELF := l;
END;


output(PROJECT(inDs, t(LEFT)));
