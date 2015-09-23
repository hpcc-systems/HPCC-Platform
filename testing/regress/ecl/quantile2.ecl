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
    RETURN DATASET(cnt, createRaw(((COUNTER-1) * scale + delta) % cnt));
END;

show(virtual dataset({ unsigned id }) ds) := OUTPUT(SORT(ds, {id}));

nullDataset := DATASET([], rawRec);
emptyDataset := NOFOLD(DATASET([], rawRec));
singleDataset := NOFOLD(createDataset(1, 1, 0)); 
smallDataset := createDataset(7, 3, 0); 

show(QUANTILE(smallDataset, 2, {id}));    // 3

//Two few entries in the input  
show(QUANTILE(smallDataset, 10, {id}));    // 0,1,2,2,3,4,4,5,6 
show(QUANTILE(smallDataset, 10, {id}, createQuantile(LEFT, COUNTER), FIRST, LAST));   // 0,0,1,2,2,3,4,4,5,6,6 
show(QUANTILE(smallDataset, 10, {id}, createQuantile(LEFT, COUNTER), FIRST, LAST, DEDUP)); // 0,1,2,3,4,5,6    

//Unusual values for the number of blocks
show(QUANTILE(smallDataset, 0, {id}));  // nothing - illegal really  
show(QUANTILE(smallDataset, 1, {id}));  // nothing  
show(QUANTILE(smallDataset, NOFOLD(1), {id}));  // nothing  
show(QUANTILE(smallDataset, NOFOLD(1), {id}, FIRST, LAST));  // 0,6  
show(QUANTILE(smallDataset, NOFOLD(999999999), {id}, DEDUP));  // 0,1,2,3,4,5,6  
show(QUANTILE(singleDataset, 5, {id}));  // 0,0,0,0  

//Check null/empty datasets are not optimized away incorrectly
show(QUANTILE(nullDataset, NOFOLD(3), {id}));  // 0, 0  
show(QUANTILE(emptyDataset, NOFOLD(3), {id}));  // 0, 0  
show(QUANTILE(nullDataset, NOFOLD(3), {id}, DEDUP));  // 0  
show(QUANTILE(emptyDataset, NOFOLD(3), {id}, DEDUP));  // 0 

show(QUANTILE(emptyDataset, NOFOLD(0), {id}));  // nothing  
show(QUANTILE(emptyDataset, NOFOLD(1), {id}));  // nothing  
show(QUANTILE(emptyDataset, NOFOLD(1), {id}, FIRST, LAST));  // 0,0  
show(QUANTILE(emptyDataset, NOFOLD(1), {id}, FIRST, LAST, DEDUP));  // 0   
