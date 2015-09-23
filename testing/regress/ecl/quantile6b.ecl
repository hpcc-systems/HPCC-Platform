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

rawRecord := { unsigned id; };

quantRec := RECORD(rawRecord)
    UNSIGNED4 quant;
END;

rawRecord createRaw(UNSIGNED id) := TRANSFORM
    SELF.id := id;
END;

inRecord := RECORD
    UNSIGNED rid;
    DATASET(rawRecord) ids;
END;

quantRec createQuantile(rawRecord l, UNSIGNED quant) := TRANSFORM
    SELF := l;
    SELF.quant := quant;
END;

createDataset(unsigned cnt, real scale, unsigned delta = 0) := FUNCTION
    RETURN NOFOLD(SORT(DATASET(cnt, createRaw((COUNTER-1) * scale + delta), DISTRIBUTED), HASH(id)));
END;

inRecord createIn(unsigned rid, unsigned cnt, real scale, unsigned delta) := TRANSFORM
    SELF.rid := rid;
    SELF.ids := createDataset(cnt, scale, delta);
END;


inDs := DATASET([
            createIn(1, 10, 1, 1),
            createIn(2, 10, 0.3, 1),
            createIn(3, 10, 15, 1),
            createIn(4, 32767, 1, 1),
            createIn(5, 99, 0.03, 1)
            ]);

normRecord := { UNSIGNED rid; UNSIGNED id, };
norm := NORMALIZE(inDs, LEFT.ids, TRANSFORM(normRecord, SELF := LEFT; SELF := RIGHT));

normRecord createSkipQuantile(normRecord l) := TRANSFORM,SKIP(l.rid IN [2,3])
    SELF := l;
END;

//Grouped quantile..
gr := GROUP(norm, rid);
q := QUANTILE(gr, 2, { id }, createSkipQuantile(LEFT));
output(q);
