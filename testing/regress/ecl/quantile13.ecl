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

rawRec := { unsigned id; };

quantRec := RECORD(rawRec)
    UNSIGNED4 quant;
END;

rawRec createRaw(UNSIGNED id) := TRANSFORM
    SELF.id := id;
END;

quantRec createQuantile(rawRec l, UNSIGNED quant) := TRANSFORM
    SELF := l;
    SELF.quant := quant;
END;

createDataset(unsigned cnt, integer scale, unsigned delta = 0) := FUNCTION
    RETURN DATASET(cnt, createRaw(((COUNTER-1) * scale + delta) % cnt), DISTRIBUTED);
END;

rawRec createSkipMatch(rawRec l, UNSIGNED c) := TRANSFORM, SKIP(c IN [2,3,4])
    SELF := l;
END;

//Check that skip on a quantile prevents the fixed numbers of outputs being generated.
ds100 := createDataset(100, 1, 0);
output(COUNT(QUANTILE(ds100, 5, { id }))); // 4
output(COUNT(QUANTILE(ds100, 5, { id }, FIRST, LAST))); // 6
output(COUNT(QUANTILE(ds100, 5, { id }, createSkipMatch(LEFT, COUNTER)))); // 1
output(COUNT(QUANTILE(ds100, 5, { id }, createSkipMatch(LEFT, COUNTER), FIRST, LAST))); // 3
