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
simpleQuantile(dataset(rawRec) in, unsigned num, boolean first = false, boolean last = false) := FUNCTION
    s := SORT(in, id);
    q := PROJECT(s, createQuantile(LEFT, calcQuantile(COUNTER, num, COUNT(in))));
    RETURN DEDUP(q, quant)(first OR quant!=0, last OR quant!=num);
END;


//Check that quantile on a sorted dataset produces the same results.
ds100 := createDataset(100, 1, 0);
ds100sort := SORTED(ds100, { id });
output(QUANTILE(ds100sort, 5, { id }, createQuantile(LEFT, COUNTER), FIRST, LAST));
output(QUANTILE(ds100sort, 27, { id }, createQuantile(LEFT, COUNTER)));
