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

createDataset(unsigned cnt, integer scale, unsigned delta = 0) := FUNCTION
    RETURN DATASET(cnt, createRaw(((COUNTER-1) * scale + delta) % cnt));
END;

ascending := createDataset(100, 1, 0);

integer zero := 0 : stored('zero');
integer minusOne := -1 : stored('minusOne');
integer oneHundred := 100 : stored('oneHundred');

//Out of range number of items
OUTPUT(QUANTILE(ascending, zero, {id}));  
//Out of range number of items
OUTPUT(QUANTILE(ascending, minusOne, {id}));  

//Number of items is sensible, but range items are invalid
OUTPUT(QUANTILE(ascending, 5, {id}, range([minusOne,oneHundred])));  
