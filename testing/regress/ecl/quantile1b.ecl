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

createHashDataset(unsigned cnt, integer scale, unsigned delta = 0) := FUNCTION
    RETURN DATASET(cnt, createRaw(HASH32((COUNTER-1) * scale + delta)), DISTRIBUTED);
END;

calcQuantile(unsigned c, unsigned num, unsigned total) := IF(c = total, num, ((c-1) * num + (num DIV 2)) DIV total);

//Does not work if num < count(in)
//Version 1: Calculate which quantile each element should belong to
simpleQuantile1(dataset(rawRec) in, unsigned num, boolean first = false, boolean last = false) := FUNCTION
    s := SORT(in, id);
    q := PROJECT(s, createQuantile(LEFT, calcQuantile(COUNTER, num, COUNT(in))));
    RETURN DEDUP(q, quant)(first OR quant!=0, last OR quant!=num);
END;

//Version 1: Calculate which rank corresponds to which quantile, and then extract those
simpleQuantile2(dataset(rawRec) in, unsigned num, boolean first = false, boolean last = false) := FUNCTION
    numRows := COUNT(in);
    s := SORT(in, id);
    quantiles := DATASET(num+1, TRANSFORM({unsigned quant, unsigned idx}, SELF.quant := COUNTER-1; SELF.idx := IF(COUNTER = num+1, numRows, ((numRows * (COUNTER-1) + (num-1) DIV 2) DIV num)+1)));
    q := PROJECT(s, createQuantile(LEFT, COUNTER));
    j := JOIN(q, quantiles, LEFT.quant = RIGHT.idx, createQuantile(LEFT, RIGHT.quant), LOOKUP);
    RETURN j(first OR quant!=0, last OR quant!=num);
END;

//Compare results of a trivial implementation with results from the activity
ds9 := createDataset(9, 1, 0);
output(simpleQuantile1(ds9, 3, first := true, last := true));      // (0,3,6,8)
output(simpleQuantile2(ds9, 3, first := true, last := true));
output(QUANTILE(ds9, 3, { id }, createQuantile(LEFT, COUNTER), FIRST, LAST));

ds8 := createDataset(8, 1, 0);
output(simpleQuantile1(ds8, 3, first := true, last := true));      // (0,3,5,7)
output(simpleQuantile2(ds8, 3, first := true, last := true));
output(QUANTILE(ds8, 3, { id }, createQuantile(LEFT, COUNTER), FIRST, LAST));

ds10 := createDataset(10, 1, 0);
output(simpleQuantile1(ds10, 3, first := true, last := true));      // (0,3,7,9)
output(simpleQuantile2(ds10, 3, first := true, last := true));
output(QUANTILE(ds10, 3, { id }, createQuantile(LEFT, COUNTER), FIRST, LAST));

